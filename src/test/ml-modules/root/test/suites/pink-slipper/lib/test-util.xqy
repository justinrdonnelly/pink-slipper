xquery version "1.0-ml";

module namespace tu="http://marklogic.com/pink-slipper/test-util";

import module namespace test="http://marklogic.com/test" at "/test/test-helper.xqy";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/pink-slipper.xqy";

declare namespace t="http://marklogic.com/test";

declare option xdmp:mapping "false";

declare variable $default-wait-time := 1;
declare variable $eval-options :=
  <options xmlns="xdmp:eval">
    <isolation>different-transaction</isolation>
    <transaction-mode>query</transaction-mode>
  </options>
;

(: return the job status from the pink-slipper module (abstract away the hassle of doing things in another transaction) :)
declare function tu:get-job-status(
  $job-id as xs:string (: the job ID :)
) as xs:string
{
 xdmp:eval(
  '
    xquery version "1.0-ml";
    import module namespace ps = "http://marklogic.com/pink-slipper" at "/pink-slipper.xqy";
    declare variable $ps:job-id as xs:string external;
    ps:get-job-status($ps:job-id)
  ',
  (
    xs:QName("ps:job-id"), $job-id
  ),
  $eval-options
  )
};

(: return the thread statuses from the pink-slipper module (abstract away the hassle of doing things in another transaction) :)
declare function tu:get-thread-statuses(
  $job-id as xs:string (: the job ID :)
) as element()* (: thread status elements :)
{
 xdmp:eval(
  '
    xquery version "1.0-ml";
    import module namespace ps = "http://marklogic.com/pink-slipper" at "/pink-slipper.xqy";
    declare variable $ps:job-id as xs:string external;
    ps:get-thread-statuses($ps:job-id)
  ',
  (
    xs:QName("ps:job-id"), $job-id
  ),
  $eval-options
  )
};

(: return the thread status documents from the pink-slipper module (abstract away the hassle of doing things in another transaction) :)
declare function tu:get-thread-status-docs-for-job(
  $job-id as xs:string (: the job ID :)
) as document-node()* (: thread status documents :)
{
 xdmp:eval(
  '
    xquery version "1.0-ml";
    import module namespace ps = "http://marklogic.com/pink-slipper" at "/pink-slipper.xqy";
    declare variable $ps:job-id as xs:string external;
    ps:get-thread-status-docs-for-job($ps:job-id)
  ',
  (
    xs:QName("ps:job-id"), $job-id
  ),
  $eval-options
  )
};

(: return the post-batch status from the pink-slipper module (abstract away the hassle of doing things in another transaction) :)
declare function tu:get-post-batch-status(
  $job-id as xs:string (: the job ID :)
) as xs:string (: post-batch status :)
{
 xdmp:eval(
  '
    xquery version "1.0-ml";
    import module namespace ps = "http://marklogic.com/pink-slipper" at "/pink-slipper.xqy";
    declare variable $ps:job-id as xs:string external;
    ps:get-post-batch-status($ps:job-id)
  ',
  (
    xs:QName("ps:job-id"), $job-id
  ),
  $eval-options
  )
};

(: return the document (abstract away the hassle of doing things in another transaction) :)
declare function tu:doc(
  $uri as xs:string*
) as document-node()*
{
 xdmp:eval(
  '
    xquery version "1.0-ml";
    declare variable $local:uri as xs:string external;
    fn:doc($local:uri)
  ',
  (
    xs:QName("local:uri"), $uri
  ),
  $eval-options
  )
};

declare function tu:assert-dateTime-exists(
  $date-time as text() (: should be a text node that is castable as a dateTime :)
) as element(t:result)*
{
  test:assert-exists($date-time),
  test:assert-true($date-time castable as xs:dateTime)
};

(: return the empty sequence after the job with the specified ID is completed :)
declare function tu:wait-for-job-to-complete(
  $job-id as xs:string, (: the job ID :)
  $wait-time as xs:unsignedInt? (: the number of seconds to wait between checks :)
) (: returns empty sequence, but leave off to allow for tail-recursion :)
{
  let $wait-time := ($wait-time, $default-wait-time)[1]
  let $job-status := xdmp:eval(
  '
    xquery version "1.0-ml";
    import module namespace ps ="http://marklogic.com/pink-slipper" at "/pink-slipper.xqy";
    declare variable $ps:job-id as xs:string external;
    ps:get-job-status($ps:job-id)
  ',
  (
    xs:QName("ps:job-id"), $job-id
  ),
  $eval-options
  )
  return if ($job-status ne $ps:job-status-processing)
  then ()
  else (
    xdmp:sleep(1000 * $wait-time),
    (:xdmp:log("waiting for job to finish"),:)
    tu:wait-for-job-to-complete($job-id, $wait-time)
  )
};
