xquery version "1.0-ml";

declare variable $test-name := "exception";

let $uris := cts:uris((), (), cts:directory-query("/testing/" || $test-name || "/"))
return (fn:count($uris), $uris)
