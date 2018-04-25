xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $init-mod-new-uri as xs:string external;
declare variable $init-mod-new-val as xs:string external;

xdmp:document-insert($init-mod-new-uri, <val>{$init-mod-new-val}</val>, xdmp:default-permissions())
