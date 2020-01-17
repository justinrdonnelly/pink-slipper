xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $URI as xs:string external;

fn:error(xs:QName("TESTINGERROR"), "Testing error")
