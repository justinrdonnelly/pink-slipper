xquery version "1.0-ml";

declare variable $doc-value as xs:string external;
declare variable $uri as xs:string := "/post-batch-module-test.xml";

xdmp:log("executing post batch"),
xdmp:document-insert($uri, <postBatch>{$doc-value}</postBatch>, xdmp:default-permissions()),
xdmp:log("done post batch")
