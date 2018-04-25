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

(: this is a test for javascript selector and process modules happy path :)
xquery version "1.0-ml";

import module namespace test = "http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";

declare option xdmp:mapping "false";

declare variable $test-name := "javascript";
declare variable $doc-count := 10;

for $count in (1 to $doc-count)
  let $count := fn:format-number($count, "00") (: pad to 2 digits :)
  return test:load-test-file($test-name || "/" || $count || ".xml", xdmp:database(), "/testing/" || $test-name || "/" || $count || ".xml")
;

xquery version "1.0-ml";

import module namespace test = "http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";
import module namespace tu = "http://marklogic.com/pink-slipper/test-util" at "/test/suites/pink-slipper/lib/test-util.xqy";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

declare option xdmp:mapping "false";

declare variable $test-name := "javascript";
declare variable $client-module-base-path := "/test/suites/pink-slipper/lib/" || $test-name;

(: kick off job right away in a different transaction :)
let $job-id := xdmp:eval(
  '
  xquery version "1.0-ml";
  import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";
  declare namespace tu="http://marklogic.com/pink-slipper/test-util";
  declare variable $tu:client-module-base-path as xs:string external;
  ps:run(
    map:map(
      <map:map xmlns:map="http://marklogic.com/xdmp/map" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <map:entry key="URIS-MODULE">
          <map:value xsi:type="xs:string">{$tu:client-module-base-path || "/selector.sjs"}</map:value>
        </map:entry>
        <map:entry key="PROCESS-MODULE">
          <map:value xsi:type="xs:string">{$tu:client-module-base-path || "/process.sjs"}</map:value>
        </map:entry>
      </map:map>
    )
  )
  ',
  (
    xs:QName("tu:client-module-base-path"), $client-module-base-path
  ),
  <options xmlns="xdmp:eval">
    <isolation>different-transaction</isolation>
  </options>
  )

let $_ := tu:wait-for-job-to-complete($job-id, ())
let $job-status := tu:get-job-status($job-id)
let $job-status-doc := ps:get-job-status-doc($job-id)
return (
  (: assert job is successful :)
  test:assert-equal($ps:job-status-successful, $job-status),
  (: assert start and end time exist :)
  tu:assert-dateTime-exists($job-status-doc/ps:job/ps:startTime/text()),
  tu:assert-dateTime-exists($job-status-doc/ps:job/ps:endTime/text()),
  (: assert URIs module status is successful :)
  test:assert-equal($ps:module-status-successful, ps:get-uris-status($job-id)),
  (: assert process module status is successful :)
  test:assert-equal($ps:module-status-successful, ps:get-process-status($job-id)),
  
  for $chunk in $job-status-doc/ps:job/ps:modules/ps:processModule/ps:chunks/ps:chunk
    let $chunk-id := $chunk/ps:chunkId/fn:string()
    let $chunk-status-doc := ps:get-chunk-status-doc($chunk-id)
    return (
      (: assert chunk has correct job ID :)
      test:assert-equal($job-id, $chunk-status-doc/ps:chunk/ps:jobId/fn:string()),
      (: assert chunk has correct chunk ID :)
      test:assert-equal($chunk-id, $chunk-status-doc/ps:chunk/ps:chunkId/fn:string()),
      (: assert all chunk statuses in the job status doc are successful :)
      test:assert-equal($ps:chunk-status-successful, $chunk/ps:chunkStatus/fn:string()),
      (: assert all chunk statuses in the chunk status docs are successful :)
      test:assert-equal($ps:chunk-status-successful, ps:get-chunk-status($chunk-id)),
      (: assert all documents have success status :)
      for $document-status in $chunk-status-doc/ps:chunk/ps:documents/ps:document/ps:documentStatus/fn:string()
        return test:assert-equal($ps:document-status-successful, $document-status)
    ),
  (: assert each doc has been updated :)
  for $count in (1 to 10)
    let $count := fn:format-number($count, "00") (: pad to 2 digits :)
    return test:assert-equal("Updated", tu:doc("/testing/" || $test-name || "/" || $count || ".xml")/root/val/fn:string())
)
