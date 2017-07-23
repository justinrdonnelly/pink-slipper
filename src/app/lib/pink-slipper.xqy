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

declare option xdmp:mapping "false";

declare variable $default-chunk-size := 500;
declare variable $trace-event-name := "pink-slipper";
declare variable $collection := "http://marklogic.com/pink-slipper";
declare variable $base-uri := "http://marklogic.com/pink-slipper/";
declare variable $status-incomplete := "Incomplete";
declare variable $status-successful := "Successful";
declare variable $status-unsuccessful := "Unsuccessful";

declare function ps:run(
  $corb-properties as map:map (: corb properties (eg URIS-MODULE) :)
  ) as xs:string (: a UUID for the job :)
{
  ps:run($corb-properties, ())
};

declare function ps:run(
  $corb-properties as map:map, (: corb properties (eg URIS-MODULE) :)
  $chunk-size as xs:int? (: number of documents to process with each thread :)
  ) as xs:string (: a UUID for the job :)
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:run")
  (: confirm required corb properties are present :)
  let $selector-module-path := map:get($corb-properties, "URIS-MODULE")
  let $process-module-path := map:get($corb-properties, "PROCESS-MODULE")
  let $_ := if (fn:empty($selector-module-path) or fn:empty($process-module-path))
  then fn:error(xs:QName("MISSINGREQUIREDCORBPROPERTIES"), "CORB properties URIS-MODULE and PROCESS-MODULE are required")
  else ()
  let $_ := if ($chunk-size le 0)
  then fn:error(xs:QName("INVALIDCHUNKSIZE"), "Chunk size must be greater than 0")
  else ()
  
  let $chunk-size := ($chunk-size, $default-chunk-size)[1]
  let $job-id := sem:uuid-string()
  let $start-time := fn:current-dateTime()
  let $thread-statuses := map:map()
  
  
  (: corb properties :)
  let $init-vars := map:map()
  let $selector-vars := map:map()
  let $pre-batch-vars := map:map()
  let $process-vars := map:map()
  let $post-batch-vars := map:map()
  let $_ := for $param in map:keys($corb-properties)
    return if (fn:starts-with($param, "INIT-MODULE."))
    then map:put($init-vars, fn:substring-after($param, "INIT-MODULE."), map:get($corb-properties, $param))
    else if (fn:starts-with($param, "URIS-MODULE."))
    then map:put($selector-vars, fn:substring-after($param, "URIS-MODULE."), map:get($corb-properties, $param))
    else if (fn:starts-with($param, "PRE-BATCH-MODULE."))
    then map:put($pre-batch-vars, fn:substring-after($param, "PRE-BATCH-MODULE."), map:get($corb-properties, $param))
    else if (fn:starts-with($param, "PROCESS-MODULE."))
    then map:put($process-vars, fn:substring-after($param, "PROCESS-MODULE."), map:get($corb-properties, $param))
    else if (fn:starts-with($param, "POST-BATCH-MODULE."))
    then map:put($post-batch-vars, fn:substring-after($param, "POST-BATCH-MODULE."), map:get($corb-properties, $param))
    else ()
  
  
  (: run INIT-MODULE :)
  let $init-module-path := map:get($corb-properties, "INIT-MODULE")
  let $_ := if (fn:exists($init-module-path))
  then
    (: execute in a different transaction so results of INIT-MODULE are visisble :)
    ps:invoke-in-different-transaction($init-module-path, $init-vars)
  else ()
  
  
  (: run URIS-MODULE :)
  (: execute in a different transaction so results of INIT-MODULE are visisble :)
  let $job-document-ids := ps:invoke-in-different-transaction($selector-module-path, $selector-vars)
  
  (: handle additional corb parameters from selector module (before count of URIs) :)
  let $document-count-index := ps:get-count-index($job-document-ids)
  let $params := fn:subsequence($job-document-ids, 1, $document-count-index - 1)
  let $job-document-ids := fn:subsequence($job-document-ids, $document-count-index + 1)

  let $_ := for $param in $params return
    if (fn:starts-with($param, "PRE-BATCH-MODULE."))
    then
      let $key-value := fn:tokenize(fn:substring-after($param, "PRE-BATCH-MODULE."), "=")
      return map:put($pre-batch-vars, $key-value[1], $key-value[2])
    else if (fn:starts-with($param, "PROCESS-MODULE."))
    then
      let $key-value := fn:tokenize(fn:substring-after($param, "PROCESS-MODULE."), "=")
      return map:put($process-vars, $key-value[1], $key-value[2])
    else if (fn:starts-with($param, "POST-BATCH-MODULE."))
    then
      let $key-value := fn:tokenize(fn:substring-after($param, "POST-BATCH-MODULE."), "=")
      return map:put($post-batch-vars, $key-value[1], $key-value[2])
    else fn:error(xs:QName("INVALIDCUSTOMINPUT"), $param || " is not a valid custom input for CORB")
  
  
  (: run PRE-BATCH-MODULE :)
  let $pre-batch-module := map:get($corb-properties, "PRE-BATCH-MODULE")
  let $_ := if (fn:exists($pre-batch-module))
  then
    (: execute in a different transaction so results of INIT-MODULE are visisble :)
    ps:invoke-in-different-transaction($pre-batch-module, $pre-batch-vars)
  else ()
  
  
  (: run PROCESS-MODULE :)
  let $thread-count := fn:ceiling(fn:count($job-document-ids) div $chunk-size)
  let $_ := for $thread in 1 to $thread-count
    let $thread-id := sem:uuid-string()
    let $_ := map:put($thread-statuses, $thread-id, $status-incomplete)
    let $start := ($thread - 1) * $chunk-size + 1
    let $thread-document-ids := fn:subsequence($job-document-ids, $start, $chunk-size)
    
    let $_ := ps:create-thread-status-document($job-id, $thread-id, $status-incomplete, (), (), $thread-document-ids, (), map:map())
    return ps:process-documents($process-module-path, $process-vars, $job-id, $thread-id)

  
  (: set up POST-BATCH-MODULE :)
  let $post-batch-module := map:get($corb-properties, "POST-BATCH-MODULE")
  let $post-batch-element := if (fn:exists($post-batch-module))
  then
    <ps:postBatchModule>
      <ps:path>{$post-batch-module}</ps:path>
      <ps:status>{$status-incomplete}</ps:status>
      <ps:variables>
        {
          for $var in map:keys($post-batch-vars)
          return (
          <ps:variable>
            <ps:name>{$var}</ps:name>
            <ps:value>{map:get($post-batch-vars, $var)}</ps:value>
          </ps:variable>
         )
        }
      </ps:variables>
    </ps:postBatchModule>
  else <ps:postBatchModule/>
  
  let $_ := ps:create-job-status-document($job-id, $start-time, $chunk-size, $thread-statuses, $post-batch-element)
  
  return $job-id
};

declare function ps:invoke-in-different-transaction(
  $module-path as xs:string, (: the path to the module :)
  $module-vars as map:map? (: module vars :)
  ) as item()* (: a sequence beginning with the count of documents, then the document IDs (often a URI) :)
{
  xdmp:invoke(
    $module-path,
    $module-vars,
    <options xmlns="xdmp:eval">
      <isolation>different-transaction</isolation>
    </options>
    )
};

declare function ps:process-documents(
  $process-module-path as xs:string, (: the path to the process module :)
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
    
    let $thread-status-doc := ps:get-thread-status-doc($thread-id)
    let $document-ids := $thread-status-doc/ps:threadStatus/ps:documentStatus/ps:unprocessedDocuments/ps:documentId/fn:string()
    let $_ := for $document-id in $document-ids
      (: invoke the users process module :)
      return try
      {
        let $local-process-vars := ps:add-uri-to-vars($process-vars, $document-id)
        let $_ := xdmp:invoke($process-module-path, $local-process-vars)
        return map:put($successful, $document-id, $document-id)
      }
      catch ($e)
      {
        xdmp:trace($trace-event-name, $e),
        map:put($failed, $document-id, $e)
      }
    (: update thread and job status docs :)
    let $thread-end-time := fn:current-dateTime()
    let $_ := xdmp:trace($trace-event-name, "about to re-create status doc from anonymous function")
    let $thread-status := if (map:count($failed) > 0) then $status-unsuccessful else $status-successful
    let $_ := ps:create-thread-status-document($job-id, $thread-id, $thread-status, $thread-start-time, $thread-end-time, (), map:keys($successful), $failed)
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
  $thread-statuses as map:map, (: a map of thread IDs to status ("Incomplete", "Successful", or "Unsuccessful") :)
  $post-batch-element as element(ps:postBatchModule) (: element containing post batch modules URI and variables :)
  ) as empty-sequence()
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:create-job-status-document")
  let $uri := ps:get-job-status-doc-uri($job-id)
  let $doc :=
    <ps:jobStatus>
      <ps:jobId>{$job-id}</ps:jobId>
      <ps:startTime>{$start-time}</ps:startTime>
      <ps:chunkSize>{$chunk-size}</ps:chunkSize>
      {$post-batch-element}
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

(: This function inserts the thread status document :)
declare function ps:create-thread-status-document(
  $job-id as xs:string, (: the UUID for the job :)
  $thread-id as xs:string, (: the UUID for the thread :)
  $thread-status as xs:string, (: the status (Incomplete, Successful, or Unsuccessful) :)
  $start-time as xs:dateTime?, (: the start time for the thread :)
  $end-time as xs:dateTime?, (: the end time for the thread :)
  $unprocessed-document-ids as xs:string*, (: document IDs (often a URI) of documents to be processed :)
  $successful-document-ids as xs:string*, (: document IDs (often a URI) of documents that were processed successfully :)
  $unsuccessful-document-ids as map:map (: map of document IDs (often a URI) of documents that were processed unsuccessfully to errors :)
  ) as empty-sequence()
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:create-thread-status-document")
  let $uri := ps:get-thread-status-doc-uri($thread-id)
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
          {for $document-id in map:keys($unsuccessful-document-ids) return
            <ps:error documentId="{$document-id}">{map:get($unsuccessful-document-ids, $document-id)}</ps:error>
          }
        </ps:unsuccessfulDocuments>
      </ps:documentStatus>
    </ps:threadStatus>

  return (
    xdmp:document-insert($uri, $doc, xdmp:default-permissions(), $collection),
    xdmp:trace($trace-event-name, "Exiting ps:create-thread-status-document")
  )
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
  (: TODO: would a lock-for-update make this safer? :)
  let $job-status-doc := ps:get-job-status-doc($job-id)
  let $threads-complete := ps:are-threads-complete($job-id, $thread-id)
  return (
    xdmp:node-replace(
      $job-status-doc/ps:jobStatus/ps:threads/ps:thread[ps:threadId = $thread-id]/ps:threadStatus,
      <ps:threadStatus>{$thread-status}</ps:threadStatus>
    ),
    if ($threads-complete) then (
      xdmp:trace($trace-event-name, "Completed main job"),
      ps:execute-post-batch-module($job-id)
    ) else ()
  )
};

(: execute post-batch module (only if applicable) :)
declare function ps:execute-post-batch-module(
  $job-id as xs:string (: the UUID for the job :)
) as empty-sequence()
{
  let $_ := xdmp:trace($trace-event-name, "Checking for post-batch module")
  let $job-status-doc := ps:get-job-status-doc($job-id)
  let $module-path := $job-status-doc/ps:jobStatus/ps:postBatchModule/ps:path/fn:string()
  (: return if there is no post-batch module :)
  return if (fn:empty($module-path)) then
    xdmp:trace($trace-event-name, "No post-batch module")
  else
    let $_ := xdmp:trace($trace-event-name, "Executing post-batch module at " || $module-path)
    let $variables := map:new((
      for $var in $job-status-doc/ps:jobStatus/ps:postBatchModule/ps:variables/ps:variable
        return map:entry($var/ps:name/fn:string(), $var/ps:value/fn:string())
    ))
    let $_ := try
    {
      xdmp:invoke($module-path, $variables),
      xdmp:node-replace($job-status-doc/ps:jobStatus/ps:postBatchModule/ps:status, <ps:status>{$status-successful}</ps:status>)
    }
    catch ($e)
    {
      xdmp:trace($trace-event-name, $e),
      (: TODO: add error info to status document :)
      xdmp:node-replace($job-status-doc/ps:jobStatus/ps:postBatchModule/ps:status, <ps:status>{$status-unsuccessful}</ps:status>)
      (:xdmp:node-insert-after($job-status-doc/ps:jobStatus/ps:postBatchModule/ps:status, <ps:error>{$e}</ps:error>):)
    }
    return xdmp:trace($trace-event-name, "post-batch module complete")
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
  $base-uri || $job-id || ".xml"
};

(: return the job status document :)
declare function ps:get-job-status-doc(
  $job-id as xs:string (: the UUID for the job :)
) as document-node()
{
  fn:doc(
    ps:get-job-status-doc-uri($job-id)
  )
};

declare function ps:get-thread-status-doc-uri(
  $thread-id as xs:string (: the UUID for the thread :)
  ) as xs:string (: URI for the thread status document :)
{
  $base-uri || $thread-id || ".xml"
};

declare function ps:get-thread-status-doc(
  $thread-id as xs:string (: the UUID for the thread :)
  ) as document-node()* (: thread status document for the thread ID :)
{
  fn:doc(
    ps:get-thread-status-doc-uri($thread-id)
  )
};

declare function ps:get-thread-status-docs-for-job(
  $job-id as xs:string (: the UUID for the job :)
  ) as document-node()* (: thread status document(s) for the job :)
{
  for $thread-id in ps:get-job-status-doc($job-id)/ps:jobStatus/ps:threads/ps:thread/ps:threadId/fn:string()
    return ps:get-thread-status-doc($thread-id)
};


(: ====== Status retrieval functions start here ====== :)


(: return the overall status of a job :)
declare function ps:get-job-status(
  $job-id as xs:string (: the UUID for the job :)
  ) as xs:string (: job status :)
{
  let $job-status-doc := ps:get-job-status-doc($job-id)
  let $_ := if (fn:empty($job-status-doc)) then fn:error(xs:QName("INVALIDJOBID"), "Job ID does not exist") else ()
  let $post-batch-status := $job-status-doc/ps:jobStatus/ps:postBatchModule/ps:status/fn:string()
  let $thread-statuses := $job-status-doc/ps:jobStatus/ps:threads/ps:thread/ps:threadStatus/fn:string()
  return if (some $status in ($thread-statuses, $post-batch-status) satisfies $status = $status-incomplete) then $status-incomplete
    else if (some $status in ($thread-statuses, $post-batch-status) satisfies $status = $status-unsuccessful) then $status-unsuccessful
    else $status-successful
};

(: return fn:true() if this job is complete :)
declare function ps:are-threads-complete(
  $job-id as xs:string (: the UUID for the job :)
  ) as xs:boolean (: whether all threads for this job are complete :)
{
  let $job-status := ps:get-job-status($job-id)
  return $job-status = $status-successful or $job-status = $status-unsuccessful
};

(: return fn:true() if this all threads in job except the one with the ID passed are complete :)
(: TODO: make the above call this with no thread IDs? :)
declare function ps:are-threads-complete(
  $job-id as xs:string, (: the UUID for the job :)
  $except-thread-id as xs:string (: the UUID for the thread to exclude :)
  ) as xs:boolean (: whether all threads for this job except those excluded are complete :)
{
  every $thread-status
    in ps:get-job-status-doc($job-id)/ps:jobStatus/ps:threads/ps:thread[ps:threadId/fn:string() != $except-thread-id]/ps:threadStatus/fn:string()
    satisfies $thread-status = $status-successful or $thread-status = $status-unsuccessful
};

(: return the status of each thread within a job :)
declare function ps:get-thread-statuses(
  $job-id as xs:string (: the UUID for the job :)
  ) as element()* (: thread status elements :)
{
  let $job-status-doc := ps:get-job-status-doc($job-id)
  let $threads := $job-status-doc/ps:jobStatus/ps:threads/ps:thread
  return $threads
};

(: return the status of the post-batch-module :)
declare function ps:get-post-batch-status(
  $job-id as xs:string (: the UUID for the job :)
  ) as xs:string (: post-batch status :)
{
  let $job-status-doc := ps:get-job-status-doc($job-id)
  return $job-status-doc/ps:jobStatus/ps:postBatchModule/ps:status/fn:string()
};

(:
declare function ps:get-status-for-all-documents(
  $job-id as xs:string (: the UUID for the job :)
  ) as element()*
{
  let $job-status-doc := ps:get-job-status-doc($job-id)
};
:)