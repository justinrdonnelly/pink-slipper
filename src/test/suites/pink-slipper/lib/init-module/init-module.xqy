xquery version "1.0-ml";
declare variable $new-val as xs:string external;

declare variable $uri as xs:string := "/init-module-test-new-value.xml";

xdmp:document-insert($uri, <val>{$new-val}</val>, xdmp:default-permissions())
