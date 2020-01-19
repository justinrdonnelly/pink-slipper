xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $test-name := "post-batch-module";

let $uris := cts:uris((), (), cts:directory-query("/testing/" || $test-name || "/"))
return (fn:count($uris), $uris)