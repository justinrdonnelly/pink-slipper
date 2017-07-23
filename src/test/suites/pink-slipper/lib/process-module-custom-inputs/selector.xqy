xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $test-name := "process-module-custom-inputs";

let $uris := cts:uris((), (), cts:directory-query("/testing/" || $test-name || "/"))
return (fn:count($uris), $uris)
