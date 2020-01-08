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

declare variable $invoke-options :=
  <options xmlns="xdmp:eval">
    <isolation>same-statement</isolation>
  </options>;


declare variable $update-options :=
  <options xmlns="xdmp:eval">
    <transaction-mode>update-auto-commit</transaction-mode>
  </options>;

declare variable $chunk-size := 5000;
declare variable $trace-event-name := "pink-slipper";
declare variable $base-uri := "http://marklogic.com/pink-slipper/";

(: job status :)
declare variable $job-status-processing := "Processing";
declare variable $job-status-successful := "Successful"; (: all the modules (including all the documents in the URIs module) have processed successfully :)
declare variable $job-status-complete-with-errors := "Complete with errors"; (: at least 1 module completed with an error (FAIL-ON-ERROR is false) :)
declare variable $job-status-unsuccessful := "Unsuccessful"; (: at least 1 module failed processing and the job stopped (FAIL-ON-ERROR is true) :)

(: module status :)
declare variable $module-status-pending := "Pending";
declare variable $module-status-processing := "Processing"; (: only applicable to PROCESS module :)
declare variable $module-status-successful := "Successful"; (: the module (including all the documents if the PROCESS module) has processed successfully :)
declare variable $module-status-complete-with-errors := "Complete with errors"; (: only applicable to PROCESS module - at least 1 document failed processing (FAIL-ON-ERROR is false) :)
declare variable $module-status-unsuccessful := "Unsuccessful"; (: the module failed processing (if PROCESS module, FAIL-ON-ERROR is true) :)

(: chunk status (in the chunk status doc and job status doc) :)
declare variable $chunk-status-pending := "Pending"; (: the chunk status document has been created, but work has not yet begun on this chunk :)
declare variable $chunk-status-queued := "Queued"; (: all the documents in this chunk have been queued into a task queue being worked by a thread :)
declare variable $chunk-status-successful := "Successful"; (: all the documents in this chunk have processed successfully :)
declare variable $chunk-status-complete-with-errors := "Complete with errors"; (: all the documents in this chunk have processed and at least 1 failed (FAIL-ON-ERROR is false) :)
declare variable $chunk-status-unsuccessful := "Unsuccessful"; (: at least 1 document in this chunk failed processing and the job stopped (FAIL-ON-ERROR is true) :)

(: document status (in the chunk status doc) :)
declare variable $document-status-pending := "Pending"; (: the document ID has been selected by the selector module :)
declare variable $document-status-queued := "Queued"; (: the document ID has been queued into a task queue being worked by a thread :)
declare variable $document-status-successful := "Successful"; (: the document ID has finished processing successfully :)
declare variable $document-status-unsuccessful := "Unsuccessful"; (: the document ID has finished processing unsuccessfully :)


declare variable $thread-count := 8; (: TODO make this a corb param :)
declare variable $update-frequency := 1; (: TODO make this a param :)

declare variable $batch-size := 1; (: TODO make this a corb param :)
declare variable $batch-uri-delim := ";"; (: TODO make this a corb param :)

(:~
 : Run a job with the supplied parameters. Return a job ID that can be used to check job status.
 : @param $params mostly corb properties (eg URIS-MODULE), but also any applicable pink slipper params
 : @return The job ID for the job
 : @error An invalid combination of corb options was provided
:)
declare function ps:run(
  $params as map:map
) as xs:string
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:run")
  (: confirm required corb properties are present :)
  let $selector-module-path := map:get($params, "URIS-MODULE")
  let $process-module-path := map:get($params, "PROCESS-MODULE")
  let $_ :=
    if (fn:empty($selector-module-path) or fn:empty($process-module-path))
    then fn:error(xs:QName("MISSINGREQUIREDCORBPROPERTIES"), "CORB properties URIS-MODULE and PROCESS-MODULE are required")
    else ()

  let $job-id := sem:uuid-string()
  let $start-time := fn:current-dateTime()


  (: corb properties :)
  let $init-vars := map:map()
  let $uris-vars := map:map()
  let $pre-batch-vars := map:map()
  let $process-vars := map:map()
  let $post-batch-vars := map:map()
  let $_ :=
    for $param in map:keys($params)
    return
      if (fn:starts-with($param, "INIT-MODULE."))
      then map:put($init-vars, fn:substring-after($param, "INIT-MODULE."), map:get($params, $param))
      else if (fn:starts-with($param, "URIS-MODULE."))
      then map:put($uris-vars, fn:substring-after($param, "URIS-MODULE."), map:get($params, $param))
      else if (fn:starts-with($param, "PRE-BATCH-MODULE."))
      then map:put($pre-batch-vars, fn:substring-after($param, "PRE-BATCH-MODULE."), map:get($params, $param))
      else if (fn:starts-with($param, "PROCESS-MODULE."))
      then map:put($process-vars, fn:substring-after($param, "PROCESS-MODULE."), map:get($params, $param))
      else if (fn:starts-with($param, "POST-BATCH-MODULE."))
      then map:put($post-batch-vars, fn:substring-after($param, "POST-BATCH-MODULE."), map:get($params, $param))
      else ()

  (: create job status document :)
  (: set up INIT-MODULE :)
  let $init-module := map:get($params, "INIT-MODULE")
  let $init-element :=
    if (fn:exists($init-module))
    then
      <ps:initModule>
        <ps:moduleStatus>{$module-status-pending}</ps:moduleStatus>
        <ps:modulePath>{$init-module}</ps:modulePath>
        {ps:create-variables-element($init-vars)}
      </ps:initModule>
    else ()

  (: set up URIS-MODULE :)
  let $uris-module := map:get($params, "URIS-MODULE")
  let $uris-element :=
    if (fn:exists($uris-module))
    then
      <ps:urisModule>
        <ps:moduleStatus>{$module-status-pending}</ps:moduleStatus>
        <ps:modulePath>{$uris-module}</ps:modulePath>
        {ps:create-variables-element($uris-vars)}
      </ps:urisModule>
    else ()

  (: set up PRE-BATCH-MODULE :)
  let $pre-batch-module := map:get($params, "PRE-BATCH-MODULE")
  let $pre-batch-element :=
    if (fn:exists($pre-batch-module))
    then
      <ps:preBatchModule>
        <ps:moduleStatus>{$module-status-pending}</ps:moduleStatus>
        <ps:modulePath>{$pre-batch-module}</ps:modulePath>
        {ps:create-variables-element($pre-batch-vars)}
      </ps:preBatchModule>
    else ()

  (: set up PROCESS-MODULE :)
  let $process-module := map:get($params, "PROCESS-MODULE")
  let $process-element :=
    if (fn:exists($process-module))
    then
      <ps:processModule>
        <ps:moduleStatus>{$module-status-pending}</ps:moduleStatus>
        <ps:modulePath>{$process-module}</ps:modulePath>
        {ps:create-variables-element($process-vars)}
        <ps:chunks/>
      </ps:processModule>
    else ()

  (: set up POST-BATCH-MODULE :)
  let $post-batch-module := map:get($params, "POST-BATCH-MODULE")
  let $post-batch-element :=
    if (fn:exists($post-batch-module))
    then
      <ps:postBatchModule>
        <ps:moduleStatus>{$module-status-pending}</ps:moduleStatus>
        <ps:modulePath>{$post-batch-module}</ps:modulePath>
        {ps:create-variables-element($post-batch-vars)}
      </ps:postBatchModule>
    else ()

  let $job-status-doc :=
    <ps:job>
      <ps:jobId>{$job-id}</ps:jobId>
      <ps:jobStatus>{$job-status-processing}</ps:jobStatus>
      <ps:threadCount>{$thread-count}</ps:threadCount>
      <ps:startTime>{$start-time}</ps:startTime>
      <ps:endTime/>
      <ps:modules>
        {$init-element}
        {$uris-element}
        {$pre-batch-element}
        {$process-element}
        {$post-batch-element}
      </ps:modules>
    </ps:job>

  return (
    (: insert the job status doc :)
    xdmp:document-insert(
      ps:get-job-status-doc-uri($job-id),
      $job-status-doc
    ),
    (: spawn init module :)
    xdmp:spawn-function(function() {ps:execute-init-module($job-id)}, $update-options),
    (: return the job id :)
    $job-id
  )
};

(:~
 : Set job status to complete.
 : @param $job-status-doc The job status document
 : @param $module-status One of $module-status-successful, $module-status-unsuccessful, or empty sequence (if there was no module executed as part of this transaction)
 : @return The empty sequence
:)
declare function ps:complete-job(
  $job-status-doc as document-node(),
  $module-status as xs:string?
) as empty-sequence()
{
  let $module-statuses := ($job-status-doc/ps:job/ps:modules/*[not(. instance of element(ps:postBatchModule))]/ps:moduleStatus/text()/fn:string(), $module-status)
  let $job-status :=
    if (every $module-status in $module-statuses satisfies $module-status eq $module-status-successful)
    then $job-status-successful
    else $job-status-complete-with-errors
  return (
    xdmp:node-replace(
      $job-status-doc/ps:job/ps:jobStatus/text(),
      text {$job-status}
    ),
    xdmp:node-insert-child(
      $job-status-doc/ps:job/ps:endTime,
      text {fn:current-dateTime()}
    )
  )
};

(:~
 : Create a map of variables for the given module.
 : @param $module The module element (eg ps:processModule) to look in for variables
 : @return A map containing the variables
:)
declare function ps:create-variables-map(
  $module as element()
) as map:map
{
  let $variables := map:map()
  let $_ :=
    for $variable in $module/ps:moduleVariables/ps:variable
    return map:put($variables, $variable/ps:name/fn:string(), $variable/ps:value/fn:string())
  return $variables
};

(:~
 : Execute the module defined by $module.
 : @param $job-status-doc The job status document
 : @param $module the module element (eg ps:processModule) to execute
 : @param $last-module Whether or not this is the last module (so we know whether to update job status)
 : @return Whatever the module returns
:)
declare function ps:execute-module(
  $job-status-doc as document-node(),
  $module as element()?,
  $last-module as xs:boolean
) as xs:anyAtomicType*
{
  if (fn:exists($module))
  then
    try {
      let $module-path := $module/ps:modulePath/fn:string()
      let $variables := ps:create-variables-map($module)
      return (
        xdmp:invoke(
          $module-path,
          $variables,
          $invoke-options
        ),
        (: update job status doc module status :)
        xdmp:node-replace(
          $module/ps:moduleStatus/text(),
          text {$module-status-successful}
        ),
        (: update job status doc overall status if applicable:)
        if ($last-module)
        then ps:complete-job($job-status-doc, $job-status-successful)
        else ()
      )
    } catch ($e)
    {
      (: update job status doc module status :)
      (: TODO: more error info :)
      (: TODO: stop the job? :)
      xdmp:node-replace(
        $module/ps:moduleStatus/text(),
        text {$module-status-unsuccessful}
      ),
      (: update job status doc overall status if applicable:)
      if ($last-module)
      then ps:complete-job($job-status-doc, $module-status-unsuccessful)
      else ()
    }
  else if ($last-module)
  then ps:complete-job($job-status-doc, ())
  else ()
};

(:~
 : Execute the init module.
 : @param $job-id The job ID for the job
 : @return The empty sequence
:)
declare function ps:execute-init-module(
  $job-id as xs:string
) as empty-sequence()
{
  let $job-status-doc := ps:get-job-status-doc($job-id, fn:true())
  let $module := $job-status-doc/ps:job/ps:modules/ps:initModule
  let $_ := ps:execute-module($job-status-doc, $module, fn:false())
  (: spawn the next module :)
  return xdmp:spawn-function(function() {ps:execute-uris-module($job-id)}, $update-options)
};

(:~
 : Execute the uris (selector) module.
 : @param $job-id The job ID for the job
 : @return The empty sequence
 : @error An invalid corb option was provided
:)
declare function ps:execute-uris-module(
  $job-id as xs:string
) as empty-sequence()
{
  let $job-status-doc := ps:get-job-status-doc($job-id, fn:true())
  let $module := $job-status-doc/ps:job/ps:modules/ps:urisModule
  let $job-document-ids := ps:execute-module($job-status-doc, $module, fn:false())

  (: handle additional corb parameters from selector module (before count of URIs) :)
  let $document-count-index := ps:get-count-index($job-document-ids)
  let $params := fn:subsequence($job-document-ids, 1, $document-count-index - 1)
  let $job-document-ids := fn:subsequence($job-document-ids, $document-count-index + 1)

  let $_ :=
    for $param in $params
    return
      if (fn:starts-with($param, "PRE-BATCH-MODULE."))
      then
        let $key-value := fn:tokenize(fn:substring-after($param, "PRE-BATCH-MODULE."), "=")
        let $variables-element := $job-status-doc/ps:job/ps:modules/ps:preBatchModule/ps:moduleVariables
        return ps:add-or-replace-module-vars($variables-element, $key-value[1], $key-value[2])
      else if (fn:starts-with($param, "PROCESS-MODULE."))
      then
        let $key-value := fn:tokenize(fn:substring-after($param, "PROCESS-MODULE."), "=")
        let $variables-element := $job-status-doc/ps:job/ps:modules/ps:processModule/ps:moduleVariables
        return ps:add-or-replace-module-vars($variables-element, $key-value[1], $key-value[2])
      else if (fn:starts-with($param, "POST-BATCH-MODULE."))
      then
        let $key-value := fn:tokenize(fn:substring-after($param, "POST-BATCH-MODULE."), "=")
        let $variables-element := $job-status-doc/ps:job/ps:modules/ps:postBatchModule/ps:moduleVariables
        return ps:add-or-replace-module-vars($variables-element, $key-value[1], $key-value[2])
      else fn:error(xs:QName("INVALIDCUSTOMINPUT"), $param || " is not a valid custom input for CORB")

  (: calculate chunk documents for tracking PROCESS-MODULE execution :)
  (: create a map of chunk IDs to document IDs :)
  (:let $chunk-id-to-doc-ids := map:map():)
  let $total-chunk-count := fn:ceiling(fn:count($job-document-ids) div $chunk-size)
  let $_ :=
    for $chunk in 1 to $total-chunk-count
    let $chunk-id := sem:uuid-string()
    let $start := ($chunk - 1) * $chunk-size + 1
    let $chunk-document-ids := fn:subsequence($job-document-ids, $start, $chunk-size)
    return (
      (: add chunks to job status doc :)
      xdmp:node-insert-child(
        $job-status-doc/ps:job/ps:modules/ps:processModule/ps:chunks,
        <ps:chunk>
          <ps:chunkId>{$chunk-id}</ps:chunkId>
          <ps:chunkStatus>{$chunk-status-pending}</ps:chunkStatus>
        </ps:chunk>
      ),
      (: TODO: if I'm spawning creation of chunk docs, how do I know when they're done and process module can begin? :)
      (: TODO: for now, don't spawn this :)
      (: spawn a task to create chunk status doc :)
      (:xdmp:spawn-function(
        function() {:)
          ps:create-initial-chunk-status-document(
            $job-id,
            $chunk-id,
            $chunk-document-ids
          )
        (:},
        $update-options
      ):)
    )

  (: spawn the next module :)
  return xdmp:spawn-function(function() {ps:execute-pre-batch-module($job-id)}, $update-options)
};

(:~
 : Execute the pre-batch module.
 : @param $job-id The job ID for the job
 : @return The empty sequence
:)
declare function ps:execute-pre-batch-module(
  $job-id as xs:string
) as empty-sequence()
{
  let $job-status-doc := ps:get-job-status-doc($job-id, fn:true())
  let $module := $job-status-doc/ps:job/ps:modules/ps:preBatchModule
  let $_ := ps:execute-module($job-status-doc, $module, fn:false())
  (: spawn the next module :)
  return xdmp:spawn-function(function() {ps:execute-process-module($job-id)}, $update-options)
};

(:~
 : Execute the process module.
 : @param $job-id The job ID for the job
 : @return The empty sequence
:)
declare function ps:execute-process-module(
  $job-id as xs:string
) as empty-sequence()
{
  let $job-status-doc := ps:get-job-status-doc($job-id, fn:true())
  let $module := $job-status-doc/ps:job/ps:modules/ps:processModule
  let $module-path := $module/ps:modulePath/fn:string()
  let $variables := ps:create-variables-map($module)
  let $thread-count := $job-status-doc/ps:job/ps:threadCount/fn:data()
  let $chunk-to-documents := ps:populate-task-queue($job-id, $thread-count)
  return
    for $chunk-id at $thread-number in map:keys($chunk-to-documents)
    return
      ps:process-document(
        $module-path,
        $variables,
        $job-id,
        $chunk-id,
        $thread-number,
        map:get($chunk-to-documents, $chunk-id),
        map:map(),
        map:map()
      )
};

(:~
 : Execute the post-batch module.
 : @param $job-id The job ID for the job
 : @return The empty sequence
:)
declare function ps:execute-post-batch-module(
  $job-id as xs:string
) as empty-sequence()
{
  let $job-status-doc := ps:get-job-status-doc($job-id, fn:true())
  let $module := $job-status-doc/ps:job/ps:modules/ps:postBatchModule
  let $_ := ps:execute-module($job-status-doc, $module, fn:true())
  return ()
};

(:~
 : Add or replace module variables for the given module.
 : @param $variables-element The ps:moduleVariables element for a module in the job status document.
 : @param $name The name of the variable
 : @param $value The value of the variable
 : @return The empty sequence
:)
declare function ps:add-or-replace-module-vars(
  $variables-element as element(ps:moduleVariables),
  $name as xs:string,
  $value as xs:string
) as empty-sequence()
{
  let $new-variable-element := ps:create-variable-element($name, $value)
  let $existing-variable-element := $variables-element/ps:variable[ps:name/fn:string eq $name]
  return
    if (fn:exists($existing-variable-element))
    then xdmp:node-replace($existing-variable-element, $new-variable-element)
    else xdmp:node-insert-child($variables-element, $new-variable-element)
};

(:~
 : Create and return the ps:moduleVariables element from the given variables map.
 : @param $variabless A map of variable names to values
 : @return The constructed ps:moduleVariables element
:)
declare function ps:create-variables-element(
  $variables as map:map
) as element(ps:moduleVariables)
{
  <ps:moduleVariables>
    {
      for $var in map:keys($variables)
      return ps:create-variable-element($var, map:get($variables, $var))
    }
  </ps:moduleVariables>
};

(:~
 : Create and return the ps:variable element from the given name and value.
 : @param $name The name of the variable
 : @param $value The value of the variable
 : @return The constructed ps:variable element
:)
declare function ps:create-variable-element(
  $name as xs:string,
  $value as xs:string
) as element(ps:variable)
{
  <ps:variable>
    <ps:name>{$name}</ps:name>
    <ps:value>{$value}</ps:value>
  </ps:variable>
};

(:~
 : Process $batch-size documents.
 : @param $process-module-path The path to the process module
 : @param $process-vars Process variables in addition to $URI
 : @param $job-id The job ID
 : @param $chunk-id The chunk ID
 : @param $thread-number The number for this thread (from 1 to thread count)
 : @param $document-ids Document IDs (often URIs) of documents that this thread will process ($batch-size at a time)
 : @param $successful A map of successful document IDs
 : @param $failed A map of failed document IDs to errors
 : @return The empty sequence
:)
(: TODO: must take $batch-size as a param :)
declare function ps:process-document(
  $process-module-path as xs:string,
  $process-vars as map:map?,
  $job-id as xs:string,
  $chunk-id as xs:string,
  $thread-number as xs:int,
  $document-ids as xs:string*,
  $successful as map:map,
  $failed as map:map
) as empty-sequence()
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:process-document")
  let $f := function() {
    let $_ := xdmp:trace($trace-event-name, "inside anonymous function")

    (: dequeue batch-size document IDs and process them :)
    let $documents-to-process := $document-ids[1 to $batch-size] (: these are the document IDs to process this iteration :)
    let $document-ids := fn:subsequence($document-ids, $batch-size + 1) (: remove these document IDs for the next iteration :)
    let $_ :=
      try {
        let $local-process-vars := ps:add-uri-to-vars($process-vars, fn:string-join($documents-to-process, $batch-uri-delim))
        let $_ := xdmp:invoke($process-module-path, $local-process-vars)
        return
          for $document-to-process in $documents-to-process
          return map:put($successful, $document-to-process, $document-to-process)
      }
      catch ($e) {
        xdmp:trace($trace-event-name, $e),
        for $document-to-process in $documents-to-process
        return map:put($failed, $document-to-process, $e)
      }

    (: update status if applicable :)
    let $update-had-errors := map:count($failed) gt 0 (: will need to know this if the job is done :)
    let $_ :=
      if (fn:count($document-ids) mod $update-frequency eq 0) (: TODO: this won't work with chunk size > 1 (or $update-frequency > 1???) :)
      then
        (
          (: update chunk status doc :)
          ps:update-chunk-status-doc(
            $chunk-id,
            map:keys($successful),
            $failed
          ),
          (: clear memory-tracked status :)
          map:clear($successful),
          map:clear($failed)
        )
      else ()

    (: get new chunk ID and document IDs (if applicable) and spawn next iteration :)
    let $old-chunk-id := $chunk-id (: we'll need this later to determine if all other chunks are done :)
    let $new-chunk-id-and-document-ids :=
      if (fn:empty($document-ids))
      then ( (: this chunk is complete :)
        (: no more document IDs in task queue, grab some more :)
        ps:populate-task-queue($job-id, 1),
        (: update job status doc for overall chunk status and chunk status doc :)
        let $job-status-doc := ps:get-job-status-doc($job-id, fn:true())
        let $chunk-status-doc := ps:get-chunk-status-doc($chunk-id, fn:true())
        let $chunk-status :=
          if ($update-had-errors or
            (some $document-status in $job-status-doc/ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk/ps:chunkStatus/fn:string()
            satisfies $document-status eq $document-status-unsuccessful)
          )
          then $chunk-status-complete-with-errors
          else $chunk-status-successful
        return (
          xdmp:node-replace(
            $chunk-status-doc/ps:chunk/ps:chunkStatus/text(),
            text {$chunk-status}
          ),
          xdmp:node-replace(
            $job-status-doc/ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk/ps:chunkStatus/text(),
            text {$chunk-status}
          ),
          if (ps:all-chunks-complete($job-id, $chunk-id))
          then (: all chunks are complete :)
            (: update job status (module status) :)
            (:let $job-status-doc := ps:get-job-status-doc($job-id, fn:true()):)
            let $module-status :=
              if (
                (: this update had errors :)
                $update-had-errors
                or (: this chunk had errors :)
                $chunk-status eq $chunk-status-complete-with-errors
                or (: some other chunk had errors :)
                (some $chunk-status in $job-status-doc/ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk/ps:chunkStatus/fn:string()
                satisfies $chunk-status eq $chunk-status-complete-with-errors)
              )
              then $module-status-complete-with-errors
              else $module-status-successful
            return (
              xdmp:node-replace(
                $job-status-doc/ps:job/ps:modules/ps:processModule/ps:moduleStatus/text(),
                text {$module-status}
              ),
              (: execute post batch module :)
              xdmp:spawn-function(function() {ps:execute-post-batch-module($job-id)}, $update-options)
              (:ps:execute-post-batch-module($job-id):)
            )
          else ()
        )
      )
      else map:entry($chunk-id, $document-ids)
    (: this makes no changes if we still had document IDs to process :)
    let $chunk-id := map:keys($new-chunk-id-and-document-ids)
    let $document-ids :=
      if (fn:exists($chunk-id))
      then map:get($new-chunk-id-and-document-ids, $chunk-id)
      else ()

    let $_ :=
      if (fn:exists($document-ids))
      then (: create another task for the next document ID :)
        ps:process-document(
          $process-module-path,
          $process-vars,
          $job-id,
          $chunk-id,
          $thread-number,
          $document-ids,
          $successful,
          $failed
        )
        else ()
    return ()
  } (: end anonymous function :)

  return xdmp:spawn-function($f, $update-options)
};

(:~
 : Return true if all chunks except the one for $chunk-id have been processed.
 : @param $job-id The job ID
 : @param $chunk-id The chunk ID of the chunk to exclude
 : @return Whether all chunks are complete
:)
declare function ps:all-chunks-complete(
  $job-id as xs:string,
  $chunk-id as xs:string
) as xs:boolean
{
  fn:empty(
    ps:get-job-status-doc($job-id, fn:true())/ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk
    [ps:chunkId/fn:string() ne $chunk-id and ps:chunkStatus/fn:string() eq $chunk-status-pending]
  )
};

(:~
 : Update the chunk status document.
 : @param $chunk-id The chunk ID
 : @param $successful-document-ids A sequence of successful document IDs
 : @param $unsuccessful-document-ids A map of failed document IDs to errors
 : @return The empty sequence
:)
declare function ps:update-chunk-status-doc(
  $chunk-id as xs:string,
  $successful-document-ids as xs:string*,
  $unsuccessful-document-ids as map:map
) as empty-sequence()
{
  let $chunk-status-doc-documents := ps:get-chunk-status-doc($chunk-id, fn:true())/ps:chunk/ps:documents
  return (
    for $successful-document-id in $successful-document-ids
    return
      xdmp:node-replace(
        $chunk-status-doc-documents/ps:document[ps:documentId eq $successful-document-id]/ps:documentStatus/text(),
        text { $document-status-successful }
      ),
    for $unsuccessful-document-id in map:keys($unsuccessful-document-ids)
    let $this-document := $chunk-status-doc-documents/ps:document[ps:documentId eq $unsuccessful-document-id]
    return (
      xdmp:node-replace(
        $this-document/ps:documentStatus/text(),
        text { $document-status-unsuccessful }
      ),
      xdmp:node-insert-child(
        $this-document,
        <ps:error>{map:get($unsuccessful-document-ids, $unsuccessful-document-id)}</ps:error>
      )
    )
  )
};

(:~
 : Create and insert the initial chunk status document.
 : @param $job-id The job ID
 : @param $chunk-id The chunk ID
 : @param $document-ids document IDs (often a URI) of documents to be processed by this chunk
 : @return The empty sequence
:)
declare function ps:create-initial-chunk-status-document(
  $job-id as xs:string,
  $chunk-id as xs:string,
  $document-ids as xs:string*
) as empty-sequence()
{
  let $_ := xdmp:trace($trace-event-name, "Entering ps:create-initial-chunk-status-document")
  let $uri := ps:get-chunk-status-doc-uri($chunk-id)
  let $doc :=
    <ps:chunk>
      <ps:jobId>{$job-id}</ps:jobId>
      <ps:chunkId>{$chunk-id}</ps:chunkId>
      <ps:chunkStatus>{$chunk-status-pending}</ps:chunkStatus>
      <ps:startTime>{fn:current-dateTime()}</ps:startTime>
      <ps:endTime/>
      <ps:documents>
        {
          for $document-id in $document-ids
          return
            <ps:document>
              <ps:documentId>{$document-id}</ps:documentId>
              <ps:documentStatus>{$document-status-pending}</ps:documentStatus>
            </ps:document>
        }
      </ps:documents>
    </ps:chunk>

  let $_ := xdmp:document-insert($uri, $doc)
  return xdmp:trace($trace-event-name, "Exiting ps:create-initial-chunk-status-document")
};

(:~
 : Populate the task queue.
 : @param $job-id The job ID
 : @param $thread-count The number of threads for the job
 : @return A map of chunk ID to associated document IDs
:)
declare function ps:populate-task-queue(
  $job-id as xs:string,
  $thread-count as xs:int
) as map:map
{
  let $job-status-doc := ps:get-job-status-doc($job-id, fn:true())
  let $chunks := $job-status-doc/ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk[ps:chunkStatus/fn:string() eq $chunk-status-pending][1 to $thread-count]
  let $chunk-to-documents := map:map() (: chunk IDs to sequences of associated document IDs :)
  let $_ :=
    for $chunk in $chunks
    (: build the map :)
    let $chunk-id := $chunk/ps:chunkId/fn:string()
    let $chunk-status-doc := ps:get-chunk-status-doc($chunk-id, fn:true())
    return (
      map:put(
        $chunk-to-documents,
        $chunk-id,
        $chunk-status-doc/ps:chunk/ps:documents/ps:document[ps:documentStatus/fn:string() eq $chunk-status-pending]/ps:documentId/fn:string()
      ),
      (: update chunk status in job status doc :)
      xdmp:node-replace(
        $chunk/ps:chunkStatus/text(),
        text {$chunk-status-queued}
      )
    )
  return $chunk-to-documents
};

(:~
 : Add URI to variables map. Note that this does not actually mutate the map. It creates a new map
 : and returns it. This is necessary because otherwise multiple threads would share the same map.
 : @param $vars A map of variables (as key-value pairs)
 : @param $uri The URI to add to $vars
 : @return A new map of variables that includes the URI
:)
declare function ps:add-uri-to-vars(
  $vars as map:map?,
  $uri as xs:string
) as map:map
{
  (: create a new map because otherwise we modify the map used by multiple threads :)
  let $return-vars := map:new($vars)
  let $_ := map:put($return-vars, "URI", $uri)
  return $return-vars
};

(:~
 : Return the (1-based) index of the sequence containing the count of URIs.
 : @param $seq A sequence containing only 1 integer
 : @return The (1-based) index of the sequence containing the count of URIs
:)
declare function ps:get-count-index(
  $seq as item()*
) as  xs:integer
{
  let $item-location :=
    for $i at $pos in $seq
    return if ($i instance of xs:integer) then $pos else ()
  return $item-location[1] (: there should only be 1 integer, but just in case... :)
};

(:~
 : Return the URI of the job status document.
 : @param $job-id The job ID
 : @return The URI of the job status document
:)
declare function ps:get-job-status-doc-uri(
  $job-id as xs:string
) as xs:string
{
  $base-uri || $job-id || ".xml"
};

(:~
 : Return the job status document.
 : @param $job-id The job ID
 : @return The job status document
:)
declare function ps:get-job-status-doc(
  $job-id as xs:string
) as document-node()
{
  ps:get-job-status-doc($job-id, fn:false())
};

(:~
 : Return the job status document.
 : @param $job-id The job ID
 : @param $lock Whether to lock the job status document for update
 : @return The job status document
:)
declare function ps:get-job-status-doc(
  $job-id as xs:string,
  $lock as xs:boolean?
) as document-node()
{
  if ($lock)
  then xdmp:lock-for-update(ps:get-job-status-doc-uri($job-id))
  else (),
  fn:doc(ps:get-job-status-doc-uri($job-id))
};

(:~
 : Return the URI of the chunk status document.
 : @param $chunk-id The chunk ID
 : @return The URI of the chunk status document
:)
declare function ps:get-chunk-status-doc-uri(
  $chunk-id as xs:string
) as xs:string
{
  $base-uri || $chunk-id || ".xml"
};

(:~
 : Return the chunk status document.
 : @param $chunk-id The chunk ID
 : @return The chunk status document
:)
declare function ps:get-chunk-status-doc(
  $chunk-id as xs:string
) as document-node()*
{
  ps:get-chunk-status-doc($chunk-id, fn:false())
};

(:~
 : Return the chunk status document.
 : @param $chunk-id The chunk ID
 : @param $lock Whether to lock the chunk status document for update
 : @return The chunk status document
:)
declare function ps:get-chunk-status-doc(
  $chunk-id as xs:string,
  $lock as xs:boolean?
) as document-node()*
{
  if ($lock)
  then xdmp:lock-for-update(ps:get-chunk-status-doc-uri($chunk-id))
  else (),
  fn:doc(ps:get-chunk-status-doc-uri($chunk-id))
};

(:~
 : Return the chunk status.
 : @param $chunk-id The chunk ID
 : @return The chunk status
:)
declare function ps:get-chunk-status(
  $chunk-id as xs:string
) as xs:string
{
  fn:doc(ps:get-chunk-status-doc-uri($chunk-id))/ps:chunk/ps:chunkStatus/fn:string()
};


(: ====== Status retrieval functions start here ====== :)

(:~
 : Return the job status.
 : @param $job-id The job ID
 : @return The job status
:)
declare function ps:get-job-status(
  $job-id as xs:string
) as xs:string
{
  ps:get-job-status-doc($job-id)/ps:job/ps:jobStatus/fn:string()
};

(:~
 : Return the init module status.
 : @param $job-id The job ID
 : @return The init module status
:)
declare function ps:get-init-status(
  $job-id as xs:string
) as xs:string
{
  ps:get-job-status-doc($job-id)/ps:job/ps:modules/ps:initModule/ps:moduleStatus/fn:string()
};

(:~
 : Return the uris module status.
 : @param $job-id The job ID
 : @return The uris module status
:)
declare function ps:get-uris-status(
  $job-id as xs:string
) as xs:string
{
  ps:get-job-status-doc($job-id)/ps:job/ps:modules/ps:urisModule/ps:moduleStatus/fn:string()
};

(:~
 : Return the pre-batch module status.
 : @param $job-id The job ID
 : @return The pre-batch module status
:)
declare function ps:get-pre-batch-status(
  $job-id as xs:string
) as xs:string
{
  ps:get-job-status-doc($job-id)/ps:job/ps:modules/ps:preBatchModule/ps:moduleStatus/fn:string()
};

(:~
 : Return the process module status.
 : @param $job-id The job ID
 : @return The process module status
:)
declare function ps:get-process-status(
  $job-id as xs:string
) as xs:string
{
  ps:get-job-status-doc($job-id)/ps:job/ps:modules/ps:processModule/ps:moduleStatus/fn:string()
};

(:~
 : Return the post-batch module status.
 : @param $job-id The job ID
 : @return The post-batch module status
:)
declare function ps:get-post-batch-status(
  $job-id as xs:string
) as xs:string
{
  ps:get-job-status-doc($job-id)/ps:job/ps:modules/ps:postBatchModule/ps:moduleStatus/fn:string()
};

(:~
 : Return the URIs that failed processing.
 : @param $job-id The job ID
 : @return The URIs that failed processing
:)
declare function ps:get-failed-uris(
  $job-id as xs:string
) as xs:string
{
  for $chunk-id in ps:get-job-status-doc($job-id)
    /ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk
    [ps:chunkStatus/fn:string() = ($chunk-status-complete-with-errors, $chunk-status-unsuccessful)]/ps:chunkId/fn:string()
  return ps:get-chunk-status-doc($chunk-id)
    /ps:chunk/ps:documents/ps:document[ps:documentStatus/fn:string() = ($chunk-status-complete-with-errors, $chunk-status-unsuccessful)]/ps:documentId/fn:string()
};

(:~
 : Return the chunk IDs for the job.
 : @param $job-id The job ID
 : @return The chunk IDs for the job
:)
declare function ps:get-job-chunk-ids(
  $job-id as xs:string
) as xs:string*
{
  ps:get-job-status-doc($job-id)/ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk/ps:chunkId/fn:string()
};
