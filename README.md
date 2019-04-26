# Pink Slipper

Pink Slipper is a MarkLogic tool to execute [CoRB](https://github.com/marklogic-community/corb2) code entirely within MarkLogic.  No command line.  No Java.  Simply kick your job off and wait.  Job status tracking allows you to check if your job is complete.  Pink Slipper gives you all the familiarity of CoRB with the piece of mind provided through job status.

The main goals of Pink Slipper are:

* CoRB compatability - Write your code just like you would for CoRB
* Status tracking - Track your job, to confirm successful completion
* Simplicity and ease of use - Copy a single XQuery module into your project

How to use Pink Slipper:

1. Copy [src/app/lib/pink-slipper.xqy](src/app/lib/pink-slipper.xqy) into your project
2. Write your CoRB modules
3. Import Pink Slipper into your module
4. Create a map containing your [CoRB options](https://github.com/marklogic-community/corb2#options) (eg URIS-MODULE)
5. Call `ps:run()`
6. Check job status as needed with `ps:get-job-status()`

Code example to start job:
```XQuery
xquery version "1.0-ml";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

let $corb-properties := map:map()
let $_ := map:put($corb-properties, "URIS-MODULE", "/path/to/uris/module.xqy")
let $_ := map:put($corb-properties, "PROCESS-MODULE", "/path/to/process/module.xqy")
let $job-id := ps:run($corb-properties)
```

Code example to check job status (MAKE SURE YOU DON'T DO THIS IN THE SAME TRANSACTION)
```XQuery
xquery version "1.0-ml";
import module namespace ps = "http://marklogic.com/pink-slipper" at "/app/lib/pink-slipper.xqy";

let $job-status := ps:get-job-status($job-id)
let $_ :=
  if ($job-status = $ps:status-incomplete) then "keep waiting"
  else if ($job-status = $ps:status-unsuccessful) then "uh-oh, better look into this"
  else if ($job-status = $ps:status-successful) then "woo-hoo!"
  else () (: this should never happen :)
```
