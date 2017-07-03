xquery version "1.0-ml";
declare variable $URI as xs:string external;

xdmp:node-replace(fn:doc($URI)/root/val, <val>Updated</val>)
