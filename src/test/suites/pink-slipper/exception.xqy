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

(: this is a test for an exception in the job :)
xquery version "1.0-ml";

import module namespace test = "http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";

declare option xdmp:mapping "false";

declare variable $test-name := "exception";

for $count in (1 to 10)
  let $count := fn:format-number($count, "00") (: pad to 2 digits :)
  return test:load-test-file($test-name || "/" || $count || ".xml", xdmp:database(), "/testing/" || $test-name || "/" || $count || ".xml")
;

xquery version "1.0-ml";

import module namespace test = "http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";
import module namespace tu = "http://marklogic.com/pink-slipper/test-util" at "/test/suites/pink-slipper/lib/test-util.xqy";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

declare option xdmp:mapping "false";

declare variable $test-name := "exception";
declare variable $client-module-base-path := "/test/suites/pink-slipper/lib/" || $test-name;

let $process-vars := map:map()
(: kick off job right away in a different transaction :)
let $job-id := xdmp:eval(
  '
  xquery version "1.0-ml";
  import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";
  declare namespace tu="http://marklogic.com/pink-slipper/test-util";
  declare variable $tu:client-module-base-path as xs:string external;
  declare variable $tu:process-vars as map:map external;
  ps:run(
    map:map(
      <map:map xmlns:map="http://marklogic.com/xdmp/map" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <map:entry key="URIS-MODULE">
          <map:value xsi:type="xs:string">{$tu:client-module-base-path || "/selector.xqy"}</map:value>
        </map:entry>
        <map:entry key="PROCESS-MODULE">
          <map:value xsi:type="xs:string">{$tu:client-module-base-path || "/process.xqy"}</map:value>
        </map:entry>
      </map:map>
    ),
    1
  )
  ',
  (
    xs:QName("tu:client-module-base-path"), $client-module-base-path,
    xs:QName("tu:process-vars"), $process-vars
  ),
  <options xmlns="xdmp:eval">
    <isolation>different-transaction</isolation>
  </options>
  )

let $_ := tu:wait-for-job-to-complete($job-id, ())
let $job-status := tu:get-job-status($job-id)
let $thread-statuses := tu:get-thread-statuses($job-id)
let $thread-status-docs := tu:get-thread-status-docs-for-job($job-id)
return (
  (: assert job is unsuccessful :)
  test:assert-equal($ps:status-unsuccessful, $job-status),
  (: assert each thread is unsuccessful :)
  for $thread-status in $thread-statuses/ps:threadStatus/fn:string()
    return test:assert-equal($ps:status-unsuccessful, $thread-status),
  (: assert each thread status contains the error :)
  for $thread-status-error in $thread-status-docs/ps:documentStatus/ps:unsuccessfulDocuments/ps:error/error:error/error:name/fn:string()
    return test:assert-equal("TESTINGERROR", $thread-status-error)
)
