xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $URI as xs:string external;
declare variable $pre-batch-mod-new-uri as xs:string external;
declare variable $pre-batch-mod-new-val as xs:string external;

declare variable $new-value-doc-uri as xs:string := "/pre-batch-module-test-new-value.xml";

(: check existence and contents of doc created during pre-batch module :)
let $pre-batch-mod-doc := fn:doc($pre-batch-mod-new-uri)
let $_ := if (fn:empty($pre-batch-mod-doc))
then fn:error(xs:QName("TESTERROR"), "Doc should have been created by pre-batch module", $pre-batch-mod-new-uri)
else ()
let $pre-batch-mod-val := $pre-batch-mod-doc/val/fn:string()
let $_ := if ($pre-batch-mod-new-val ne $pre-batch-mod-val)
then fn:error(xs:QName("TESTERROR"), "Doc created by pre-batch module has incorrect contents", $pre-batch-mod-new-val)
else ()

return xdmp:node-replace(fn:doc($URI)/root/val, <val>Updated</val>)
