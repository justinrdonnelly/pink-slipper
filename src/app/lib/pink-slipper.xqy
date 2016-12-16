(: 
 
Copyright 2016 MarkLogic Corporation
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

:)

xquery version "1.0-ml";
module namespace ps = "http://marklogic.com/pink-slipper";

declare variable $default-chunk-size := 500;
declare variable $trace-event-name := "pink-slipper";
declare variable $collection := "http://marklogic.com/pink-slipper";
declare variable $status-incomplete := "Incomplete";
declare variable $status-successful := "Successful";
declare variable $status-unsuccessful := "Unsuccessful";

declare function ps:run(
  $selector-path as xs:string, (: the path to the selector module :)
  $selector-vars as map:map?, (: selector vars :)
  $process-path as xs:string, (: the path to the process module :)
  $process-vars as map:map?, (: process vars in addition to $URI :)
  $chunk-size as xs:int? (: number of documents to process with each thread :)
  ) as xs:string (: a UUID for the job :)
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:run")
  let $_ := if ($chunk-size = 0) then fn:error(xs:QName("INVALIDCHUNKSIZE"), "Chunk size cannot be 0") else ()
  let $chunk-size := ($chunk-size, $default-chunk-size)[1]
  let $job-id := sem:uuid-string()
  let $start-time := fn:current-dateTime()
  let $thread-statuses := map:map()
  
  let $job-document-ids := ps:select-documents($selector-path, $selector-vars)
  let $_ := xdmp:log("$job-document-ids:")
  let $_ := $job-document-ids ! xdmp:log(.)
  
  let $document-count-index := ps:get-count-index($job-document-ids)
  let $params := fn:subsequence($job-document-ids, 1, $document-count-index - 1)
  let $job-document-ids := fn:subsequence($job-document-ids, $document-count-index + 1)

  (: handle additional corb parameters :)
  let $_ := for $param in $params return
    if (fn:starts-with($param, "PROCESS-MODULE."))
    then 
      let $key-value := fn:tokenize(fn:substring-after($param, "PROCESS-MODULE."), "=")
      let $_ := xdmp:log("adding var: " || $key-value[1] || "=" || $key-value[2])
      return map:put($process-vars, $key-value[1], $key-value[2])
    else ()
    (: TODO: handle other possible additional corb parameters :)
  
  let $thread-count := fn:ceiling(fn:count($job-document-ids) div $chunk-size)
  (:let $threads := map:map() (: thread id to URIs :):)
  let $_ := xdmp:log("thread count: " || xdmp:quote($thread-count))
  let $_ := for $thread in 1 to $thread-count
    let $thread-id := sem:uuid-string()
    let $_ := map:put($thread-statuses, $thread-id, $status-incomplete)
    let $start := ($thread - 1) * $chunk-size + 1
    let $thread-document-ids := fn:subsequence($job-document-ids, $start, $chunk-size)
    (:let $_ := xdmp:log("thread document ids")
    let $_ := xdmp:log($thread-document-ids):)
    (:return map:put($threads, $thread-id, fn:subsequence($uris, $start, $chunk-size)):)
    
    let $_ := ps:create-thread-status-document($job-id, $thread-id, $status-incomplete, (), (), $thread-document-ids, (), ())

    return ps:process-documents($process-path, $process-vars, $job-id, $thread-id)

  let $_ := ps:create-job-status-document($job-id, $start-time, $chunk-size, $thread-statuses)
  return $job-id
};

declare function ps:select-documents(
  $selector-path as xs:string, (: the path to the selector module :)
  $selector-vars as map:map? (: selector vars :)
  ) as item()* (: a sequence beginning with the count of documents, then the document IDs (often a URI) :)
{
  xdmp:invoke($selector-path, $selector-vars)
};

declare function ps:process-documents(
  $process-path as xs:string, (: the path to the process module :)
  $process-vars as map:map?, (: process vars in addition to $URI :)
  $job-id as xs:string, (: the UUID for the job :)
  $thread-id as xs:string (: the UUID for this thread :)
  ) as empty-sequence()
{
  (: create an anonymous function because spawn takes a module, and spawn-fucntion takes 0-arity function :)
  let $_ := xdmp:trace($trace-event-name, "Entering ps:process-documents")
  let $f := function() {
    let $_ := xdmp:trace($trace-event-name, "inside anonymous function")
    let $thread-start-time := fn:current-dateTime()
    let $failed := map:map()
    let $successful := map:map()
    
    let $thread-status-doc-uri := ps:get-thread-status-doc-uri($job-id, $thread-id)
    let $thread-status-doc := fn:doc($thread-status-doc-uri)
    (:let $_ := xdmp:log("status doc uri")
    let $_ := xdmp:log($thread-status-doc-uri):)
    let $document-ids := $thread-status-doc/ps:threadStatus/ps:documentStatus/ps:unprocessedDocuments/ps:documentId/fn:string()
    (:let $_ := xdmp:log("document IDs (for this thread):")
    let $_ := xdmp:log($document-ids):)
    let $_ := for $document-id in $document-ids
      (: invoke the users process module :)
      return try
      {
        let $local-process-vars := ps:add-uri-to-vars($process-vars, $document-id)
        let $_ := xdmp:log("document ID: " || $document-id || " - URI: " || map:get($local-process-vars, "URI"))
        let $_ := xdmp:invoke($process-path, $local-process-vars)
        let $_ := xdmp:log("finished invoke for doc: " || $document-id)
        return map:put($successful, $document-id, $document-id)
      }
      catch ($e)
      {
        xdmp:trace($trace-event-name, $e),
        map:put($failed, $document-id, $e)
      }
    (: update status doc :)
    let $thread-end-time := fn:current-dateTime()
    let $_ := xdmp:trace($trace-event-name, "about to re-create status doc from anonymous function")
    let $thread-status := if (map:count($failed) > 0) then $status-unsuccessful else $status-successful
    (: TODO: update the job status doc :)
    let $_ := ps:create-thread-status-document($job-id, $thread-id, $thread-status, $thread-start-time, $thread-end-time, (), map:keys($successful), map:keys($failed))
    return ps:update-job-status-document-for-thread($job-id, $thread-id, $thread-status)
  }
  let $options :=
    <options xmlns="xdmp:eval">
      <transaction-mode>update-auto-commit</transaction-mode>
    </options>
  return xdmp:spawn-function($f, $options)
};

(: This function inserts the job status document :)
declare function ps:create-job-status-document(
  $job-id as xs:string, (: the UUID for the job :)
  $start-time as xs:dateTime, (: the start date/time of the job :)
  $chunk-size as xs:int, (: number of documents to process with each thread :)
  $thread-statuses as map:map (: a map of thread IDs to status ("Incomplete", "Successful", or "Unsuccessful") :)
  ) as empty-sequence()
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:create-job-status-document")
  let $uri := ps:get-job-status-doc-uri($job-id)
  let $doc :=
    <ps:jobStatus>
      <ps:jobId>{$job-id}</ps:jobId>
      <ps:startTime>{$start-time}</ps:startTime>
      <ps:chunkSize>{$chunk-size}</ps:chunkSize>
      <ps:threads>
        {
        for $thread-id in map:keys($thread-statuses) return
          <ps:thread>
            <ps:threadId>{$thread-id}</ps:threadId>
            <ps:threadStatus>{map:get($thread-statuses, $thread-id)}</ps:threadStatus>
          </ps:thread>
        }
      </ps:threads>
    </ps:jobStatus>

  let $_ := xdmp:trace($trace-event-name, "Exiting ps:create-job-status-document")
  return xdmp:document-insert($uri, $doc, xdmp:default-permissions(), $collection)
};

(: This function inserts the thread status documents :)
declare function ps:create-thread-status-document(
  $job-id as xs:string, (: the UUID for the job :)
  $thread-id as xs:string, (: the UUID for the thread :)
  $thread-status as xs:string, (: the status (Incomplete, Successful, or Unsuccessful) :)
  $start-time as xs:dateTime?, (: the start time for the thread :)
  $end-time as xs:dateTime?, (: the end time for the thread :)
  $unprocessed-document-ids as xs:string*, (: document IDs (often a URI) of documents to be processed :)
  $successful-document-ids as xs:string*, (: document IDs (often a URI) of documents that were processed successfully :)
  $unsuccessful-document-ids as xs:string* (: document IDs (often a URI) of documents that were processed unsuccessfully :)
  ) as empty-sequence()
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:create-thread-status-document")
  let $uri := ps:get-thread-status-doc-uri($job-id, $thread-id)
  let $doc :=
    <ps:threadStatus>
      <ps:jobId>{$job-id}</ps:jobId>
      <ps:threadId>{$thread-id}</ps:threadId>
      <ps:threadStatus>{$thread-status}</ps:threadStatus>
      <ps:startTime>{$start-time}</ps:startTime>
      <ps:endTime>{$end-time}</ps:endTime>
      <ps:documentStatus>
        <ps:unprocessedDocuments>
          {for $document-id in $unprocessed-document-ids return <ps:documentId>{$document-id}</ps:documentId>}
        </ps:unprocessedDocuments>
        <ps:successfulDocuments>
          {for $document-id in $successful-document-ids return <ps:documentId>{$document-id}</ps:documentId>}
        </ps:successfulDocuments>
        <ps:unsuccessfulDocuments>
          {for $document-id in $unsuccessful-document-ids return <ps:documentId>{$document-id}</ps:documentId>}
        </ps:unsuccessfulDocuments>
      </ps:documentStatus>
    </ps:threadStatus>

  let $_ := xdmp:trace($trace-event-name, "Exiting ps:create-thread-status-document")
  return xdmp:document-insert($uri, $doc, xdmp:default-permissions(), $collection)
};

declare function ps:add-uri-to-vars(
  $vars as map:map?, (: variables :)
  $uri as xs:string (: the URI to add to $vars :)
  ) as map:map (: return the modifed $vars :)
{
  (: create a new map because otherwise we modify the map used by multiple threads :)
  let $return-vars := map:new($vars)
  let $_ := map:put($return-vars, "URI", $uri)
  return $return-vars
};

declare function ps:update-job-status-document-for-thread(
  $job-id as xs:string, (: the UUID for the job :)
  $thread-id as xs:string, (: the UUID for the thread :)
  $thread-status as xs:string (: the status for this thread :)
  ) as empty-sequence()
{
  let $job-status-doc-uri := ps:get-job-status-doc-uri($job-id)
  let $job-status-doc := fn:doc($job-status-doc-uri)
  let $old := $job-status-doc/ps:jobStatus/ps:threads/ps:thread[ps:threadId = $thread-id]/ps:threadStatus
  let $new := <ps:threadStatus>{$thread-status}</ps:threadStatus>
  return xdmp:node-replace($old, $new)
};

(: return the index in the sequence of the first item of type xs:integer :)
declare function ps:get-count-index($seq)
{
  let $item-location := for $i at $pos in $seq
    return if ($i instance of xs:integer) then $pos else ()
  return $item-location[1] (: there should only be 1 integer, but just in case... :)
};

declare function ps:get-job-status-doc-uri(
  $job-id as xs:string (: the UUID for the job :)
  ) as xs:string
{
  "/taskTracker/" || $job-id || ".xml"
};

declare function ps:get-thread-status-doc-uri(
  $job-id as xs:string, (: the UUID for the job :)
  $thread-id as xs:string (: the UUID for the thread :)
  ) as xs:string
{
  "/taskTracker/" || $job-id || "/" || $thread-id || ".xml"
};

(: ====== Status functions start here ====== :)

(: return the overall status of a job :)
declare function ps:get-job-status(
  $job-id as xs:string (: the UUID for the job :)
  ) as xs:string
{
  let $job-status-doc-uri := ps:get-job-status-doc-uri($job-id)
  let $job-status-doc := fn:doc($job-status-doc-uri)
  let $_ := if (fn:empty($job-status-doc)) then fn:error(xs:QName("INVALIDJOBID"), "Job ID does not exist") else ()
  let $thread-statuses := $job-status-doc/ps:jobStatus/ps:threads/ps:thread/ps:threadStatus/fn:string()
  let $job-status :=
    if ($thread-statuses = $status-incomplete) then $status-incomplete
    else if ($thread-statuses = $status-unsuccessful) then $status-unsuccessful
    else $status-successful
  return $job-status
};

(: return the status of each thread within a job :)
declare function ps:get-thread-statuses(
  $job-id as xs:string (: the UUID for the job :)
  ) as element()*
{
  let $job-status-doc-uri := ps:get-job-status-doc-uri($job-id)
  let $job-status-doc := fn:doc($job-status-doc-uri)
  let $threads := $job-status-doc/ps:jobStatus/ps:threads/ps:thread
  return $threads
};

(:
declare function ps:get-status-for-all-documents(
  $job-id as xs:string (: the UUID for the job :)
  ) as element()*
{
  let $job-status-doc-uri := ps:get-job-status-doc-uri($job-id)
  let $job-status-doc := fn:doc($job-status-doc-uri)
};
:)