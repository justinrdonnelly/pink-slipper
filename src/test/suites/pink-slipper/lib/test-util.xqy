xquery version "1.0-ml";

module namespace tu="http://marklogic.com/pink-slipper/test-util";
import module namespace test="http://marklogic.com/roxy/test-helper" at "/test/test-helper.xqy";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

declare variable $default-wait-time := 1;

(: return the job status from the pink-slipper module (abstract away the hassle of doing things in another transaction) :)
declare function tu:get-job-status(
  $job-id as xs:string (: the job ID :)
) as xs:string
{
 xdmp:eval(
  '
    xquery version "1.0-ml";
    import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";
    declare variable $ps:job-id as xs:string external;
    ps:get-job-status($ps:job-id)
  ',
  (
    xs:QName("ps:job-id"), $job-id
  ),
  <options xmlns="xdmp:eval">
    <isolation>different-transaction</isolation>
  </options>
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
    import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";
    declare variable $ps:job-id as xs:string external;
    ps:get-thread-statuses($ps:job-id)
  ',
  (
    xs:QName("ps:job-id"), $job-id
  ),
  <options xmlns="xdmp:eval">
    <isolation>different-transaction</isolation>
  </options>
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
  <options xmlns="xdmp:eval">
    <isolation>different-transaction</isolation>
  </options>
  )
};

(: return the empty sequence after the job with the specified ID is completed :)
declare function tu:wait-for-job-to-complete(
  $job-id as xs:string, (: the job ID :)
  $wait-time as xs:unsignedInt? (: the number of seconds to wait between checks :)
) as empty-sequence()
{
  let $wait-time := ($wait-time, $default-wait-time)[1]
  return xdmp:eval(
  '
    xquery version "1.0-ml";
    import module namespace tu="http://marklogic.com/pink-slipper/test-util" at "/test/suites/pink-slipper/lib/test-util.xqy";
    declare namespace ps = "http://marklogic.com/pink-slipper";
    declare variable $ps:job-id as xs:string external;
    declare variable $tu:wait-time as xs:unsignedInt? external;
    tu:_wait-for-job-to-complete($ps:job-id, $tu:wait-time)
  ',
  (
    xs:QName("ps:job-id"), $job-id,
    xs:QName("tu:wait-time"), $wait-time
  ),
  <options xmlns="xdmp:eval">
    <isolation>different-transaction</isolation>
  </options>
  )
};

(: return the empty sequence after the job with the specified ID is completed :)
(: although not private, this function should not be called directly, use the
   "public" version to call this in a different transaction :)
declare function tu:_wait-for-job-to-complete(
  $job-id as xs:string, (: the job ID :)
  $wait-time as xs:unsignedInt (: the number of seconds to wait between checks :)
) as empty-sequence()
{
  let $status := ps:get-job-status($job-id) 
  let $_ := if (fn:not($status = $ps:status-successful or $status = $ps:status-unsuccessful))
  then
    let $_ := xdmp:sleep(1000 * $wait-time)
    return tu:wait-for-job-to-complete($job-id, $wait-time)
  else ()
  return ()
};
