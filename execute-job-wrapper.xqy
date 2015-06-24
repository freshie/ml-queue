(:~
 : The module called from the broker to execute the queued job
 :
 : Copyright (c) 2014 MarkLogic Corporation
 :
 : Licensed under the Apache License, Version 2.0 (the "License");
 : you may not use this file except in compliance with the License.
 : You may obtain a copy of the License at
 :
 :  http://www.apache.org/licenses/LICENSE-2.0
 :
 : Unless required by applicable law or agreed to in writing, software
 : distributed under the License is distributed on an "AS IS" BASIS,
 : WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 : See the License for the specific language governing permissions and
 : limitations under the License.
 :
 : @author Tyler Replogle
 :)


xquery version "1.0-ml";

import module namespace core = "http://github.com/freshie/ml-queue/core" at "/core.xqy";

declare namespace host = "http://marklogic.com/xdmp/status/host";

declare variable $job-uri as xs:string external;
declare variable $job-xml as node() external;

let $job-id := $job-xml/@id
let $function := xdmp:function(fn:QName($job-xml/action/namespace, $job-xml/action/function), $job-xml/action/path)
let $host-id := xdmp:host()
let $host-task-server-id := xdmp:host-status($host-id)/host:task-server/host:task-server-id/text()
let $request-id := xdmp:request()

(:Eval function for updating the execution specs (host-id, server-id, request-id) in the execution-specs document:)
let $eval-function := xs:string('xquery version "1.0-ml";

import module namespace core = "http://github.com/freshie/ml-queue/core" at "/core.xqy";

declare variable $host-id as xs:string external;
declare variable $server-id as xs:string external;
declare variable $request-id as xs:string external;
declare variable $job-uri as xs:string external;
declare variable $job-id as xs:string external;

let $new-uri := fn:replace($job-uri,".xml", "-execution-specs.xml")
let $_ := core:add-execution-specifications($new-uri, "host-id", $host-id)
let $_ := core:add-execution-specifications($new-uri, "server-id", $server-id)
let $_ := core:add-execution-specifications($new-uri, "request-id", $request-id)
return ()')

let $_ :=
  xdmp:eval(
    $eval-function,
    (
      xs:QName("host-id"), xs:string($host-id),
      xs:QName("server-id"), xs:string($host-task-server-id),
      xs:QName("request-id"), xs:string($request-id),
      xs:QName('job-uri'), xs:string($job-uri),
      xs:QName('job-id'), xs:string($job-id)
     ),
     <options xmlns="xdmp:eval">
            <transaction-mode>update</transaction-mode>
    </options>
  )

return
  try
  {
    let $lastStatus := fn:doc($job-uri)/element()/workflow/status[fn:last()]/@type
    where fn:not($lastStatus eq ('Error','Stopped''Removed'))
    return
         (
          if (fn:exists($job-xml/content)) then
              xdmp:apply($function, map:map($job-xml/content/map:map))
          else
              xdmp:apply($function)
         ),
         core:add-job-status($job-uri, "Completed"),
         core:update-job-status($job-uri, "Completed")
  }
  catch($e)
  {
    core:add-job-status($job-uri, "Error"),
    core:update-job-status($job-uri, "Error")
  }
