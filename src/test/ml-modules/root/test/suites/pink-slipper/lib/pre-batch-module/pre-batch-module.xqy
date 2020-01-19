xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $pre-batch-mod-new-uri as xs:string external;
declare variable $pre-batch-mod-new-val as xs:string external;

xdmp:document-insert($pre-batch-mod-new-uri, <val>{$pre-batch-mod-new-val}</val>, xdmp:default-permissions())
