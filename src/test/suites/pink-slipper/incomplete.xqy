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

declare option xdmp:mapping "false";

declare variable $test-name := "incomplete";
declare variable $job-id := "f44ba6c8-01bb-4831-bf8c-7609b3f13bad";

test:load-test-file($test-name || "/job-status.xml", xdmp:database(), "http://marklogic.com/pink-slipper/" || $job-id || ".xml"),
for $count in (1 to 10)
  let $thread-id := "02b415c5-9f6a-4e90-8ef5-743d606ea0" || fn:format-number($count, "00") (: pad to 2 digits :)
  let $uri := "http://marklogic.com/pink-slipper/" || $thread-id || ".xml"
  return test:load-test-file($test-name || "/thread-status-" || $thread-id || ".xml", xdmp:database(), $uri)
;

xquery version "1.0-ml";

import module namespace test = "http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

declare option xdmp:mapping "false";

declare variable $job-id := "f44ba6c8-01bb-4831-bf8c-7609b3f13bad";
declare variable $complete-doc-count := 50;
declare variable $total-doc-count := 100;

test:assert-equal($total-doc-count, ps:get-total-document-count($job-id)),
test:assert-equal($complete-doc-count, ps:get-processed-document-count($job-id)),
test:assert-equal(
  $ps:status-incomplete || " - " || $complete-doc-count || "/" || $total-doc-count,
  ps:get-job-status($job-id)
)
