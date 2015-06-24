(:~
 : The module is a scheduled task running in the host to invoke a function to execute the jobs in the queue
 :
 : Copyright (c) 2015 MarkLogic Corporation
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

import module namespace broker = "http://github.com/freshie/ml-queue/broker" at "/broker.xqy";
import module namespace config = "http://github.com/freshie/ml-queue/config" at "/config.xqy";

declare namespace host = "http://marklogic.com/xdmp/status/host";
declare namespace status ="http://marklogic.com/xdmp/status/server";


let $host-id := $config:currentHost
let $task-server-id :=xdmp:host-status($host-id)/host:task-server/host:task-server-id/text()
let $server-status := xdmp:server-status($host-id,$task-server-id)
let $host-free-threads := xs:int($server-status/status:max-threads) - xs:int($server-status/status:threads)

let $_ :=
  if ($host-free-threads > 0) then
		broker:run()
	else ()

(: checks for long running jobs :)
for $request-status in $server-status/status:request-statuses/status:request-status
	let $request-id := $request-status/status:request-id/text()
	let $job-start-time := xs:dateTime($request-status/status:start-time/text())
	let $job-running-time := minutes-from-duration(fn:current-dateTime() - $job-start-time)
	let $_ :=
    if ($job-running-time > $config:jobTimeOut) then
		    broker:stop-long-running-jobs($request-id)
		  else ()
return ()