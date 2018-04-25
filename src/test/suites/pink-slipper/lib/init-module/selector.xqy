xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $init-mod-new-uri as xs:string external;
declare variable $init-mod-new-val as xs:string external;
declare variable $test-name := "init-module";

(: check existence and contents of doc created during init module :)
let $init-mod-doc := fn:doc($init-mod-new-uri)
let $_ := if (fn:empty($init-mod-doc))
then fn:error(xs:QName("TESTERROR"), "Doc should have been created by init module", $init-mod-new-uri)
else ()
let $init-mod-val := $init-mod-doc/val/fn:string()
let $_ := if ($init-mod-new-val ne $init-mod-val)
then fn:error(xs:QName("TESTERROR"), "Doc created by init module has incorrect contents", $init-mod-new-val)
else ()

let $uris := cts:uris((), (), cts:directory-query("/testing/" || $test-name || "/"))
return (fn:count($uris), $uris)
