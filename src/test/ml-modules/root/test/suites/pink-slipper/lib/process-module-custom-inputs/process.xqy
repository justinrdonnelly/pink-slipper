xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $URI as xs:string external;
declare variable $new-val1 as xs:string external;
declare variable $new-val2 as xs:string external;

xdmp:node-replace(fn:doc($URI)/root/val, <val>{$new-val1 || " - " || $new-val2}</val>)
