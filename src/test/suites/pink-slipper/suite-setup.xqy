xquery version "1.0-ml";

(: insert test data for all tests :)
import module namespace test="http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";

(: TODO: make a function for this that takes test name and count (maybe pad to 3 digitis) (need to confirm count is not too high for padding) :)
(: basic :)
let $test-name := "basic"
let $_ := for $count in (1 to 10)
  let $count := fn:format-number($count, "00") (: pad to 2 digits :)
  return test:load-test-file($test-name || "/" || $count || ".xml", xdmp:database(), "/testing/" || $test-name || "/" || $count || ".xml")

(: process-module-custom-inputs :)
let $test-name := "process-module-custom-inputs"
let $_ := for $count in (1 to 10)
  let $count := fn:format-number($count, "00") (: pad to 2 digits :)
  return test:load-test-file($test-name || "/" || $count || ".xml", xdmp:database(), "/testing/" || $test-name || "/" || $count || ".xml")

(: javascript :)
let $test-name := "javascript"
let $_ := for $count in (1 to 10)
  let $count := fn:format-number($count, "00") (: pad to 2 digits :)
  return test:load-test-file($test-name || "/" || $count || ".xml", xdmp:database(), "/testing/" || $test-name || "/" || $count || ".xml")

return ()