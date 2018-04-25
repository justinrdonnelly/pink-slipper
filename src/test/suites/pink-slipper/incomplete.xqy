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

(: this is a test for getting the status of an incomplete job :)
xquery version "1.0-ml";

import module namespace test = "http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

declare option xdmp:mapping "false";

declare variable $test-name := "incomplete";
declare variable $job-id := "f44ba6c8-01bb-4831-bf8c-7609b3f13bad";
declare variable $chunk-ids := (
  "a76ae3f8-8226-497f-ba9c-46fb35476262",
  "a76ae3f8-8226-497f-ba9c-46fb35476263"
);

test:load-test-file($test-name || "/job-status.xml", xdmp:database(), ps:get-job-status-doc-uri($job-id)),
for $chunk-id in $chunk-ids
  return test:load-test-file($test-name || "/chunk-status-" || $chunk-id || ".xml", xdmp:database(), ps:get-chunk-status-doc-uri($chunk-id))
;

xquery version "1.0-ml";

import module namespace test = "http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

declare option xdmp:mapping "false";

declare variable $job-id := "f44ba6c8-01bb-4831-bf8c-7609b3f13bad";
declare variable $chunk-ids := (
  "a76ae3f8-8226-497f-ba9c-46fb35476262",
  "a76ae3f8-8226-497f-ba9c-46fb35476263"
);

test:assert-equal($ps:job-status-processing, ps:get-job-status($job-id)),
test:assert-equal($ps:module-status-processing, ps:get-process-status($job-id)),
test:assert-equal($ps:chunk-status-successful, ps:get-chunk-status($chunk-ids[1])),
test:assert-equal($ps:chunk-status-queued, ps:get-chunk-status($chunk-ids[2]))
