xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $URI as xs:string external;

declare variable $new-value-doc-uri as xs:string := "/pre-batch-module-test-new-value.xml";

xdmp:node-replace(
  fn:doc($URI)/root/val,
  <val>{fn:doc($new-value-doc-uri)/val/fn:string()}</val>
  )
