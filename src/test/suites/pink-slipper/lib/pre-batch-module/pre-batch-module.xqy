xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $new-val as xs:string external;

declare variable $uri as xs:string := "/pre-batch-module-test-new-value.xml";

xdmp:document-insert($uri, <val>{$new-val}</val>, xdmp:default-permissions())
