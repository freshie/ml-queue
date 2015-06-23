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

import module namespace broker = "http://aetna.com/edh/libraries/edh-job-queue-broker" at "/app/lib/edh-job-queue-broker.xqy";
import module namespace config = "http://marklogic.com/roxy/config" at "/app/config/config.xqy";
import module namespace edh-message = "http://aetna.com/edh/libraries/edh-message" at "/app/lib/edh-message.xqy";

let $host-id := xdmp:host()
let $task-server-id :=xdmp:host-status($host-id)/*:task-server/*:task-server-id/text()
let $server-status := xdmp:server-status($host-id,$task-server-id)
let $host-free-threads := fn:data($server-status//*:max-threads) - fn:data($server-status//*:threads)
let $_ := if ($host-free-threads > 0) then
						edh-job-queue-broker:run()
					else ()

for $request-status in $server-status//*:request-status
	let $request-id := $request-status//*:request-id/text()

  	let $job-start-time := xs:dateTime($request-status//*:start-time/text())
  	let $job-running-time := minutes-from-duration(fn:current-dateTime() - $job-start-time)
  	let $_ := if ($job-running-time > $config:REQUEST-TIMEOUT-TIME) then
		     (
                edh-message:log-job-queuing("log",
                                            "job-queuing-info",
                                            "job-queue",
                                            "",
                                            "",
                                            fn:concat("Job running time is ",$job-running-time, " and it exceeded the time limit")
                                           ),
			    edh-job-queue-broker:stop-long-running-jobs($request-id)
		      )
			  else ()
return ()


