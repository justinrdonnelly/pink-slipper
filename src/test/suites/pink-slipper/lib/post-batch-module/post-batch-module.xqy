xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $post-batch-mod-new-uri as xs:string external;
declare variable $post-batch-mod-new-val as xs:string external;
declare variable $last-process-doc-uri as xs:string external;

(: check the first and last doc updated by the process module :)
if ("Updated" ne fn:doc($last-process-doc-uri)/root/val/fn:string())
then fn:error(xs:QName("TESTERROR"), "Doc should have been updated by process module", $last-process-doc-uri)
else (),

xdmp:document-insert($post-batch-mod-new-uri, <val>{$post-batch-mod-new-val}</val>, xdmp:default-permissions())
