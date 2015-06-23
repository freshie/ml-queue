(:~
 : The module provides the functions which handle the requests in the job queue
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

module namespace edh-job-queue-broker = "http://github.com/freshie/ml-queue/broker";
import module namespace req = "http://marklogic.com/roxy/request" at "/roxy/lib/request.xqy";
import module namespace edh-controller = "http://aetna.com/edh/libraries/edh-controller" at "/app/lib/edh-controller.xqy";
import module namespace edh-resolver = "http://aetna.com/edh/libraries/edh-resolver" at "/app/lib/edh-resolver.xqy";
import module namespace config = "http://marklogic.com/roxy/config" at "/app/config/config.xqy";
import module namespace edh-core = "http://aetna.com/edh/libraries/edh-core" at "/app/lib/edh-core.xqy";
import module namespace edh-message = "http://aetna.com/edh/libraries/edh-message" at "/app/lib/edh-message.xqy";
import module namespace edh-job-queue-core = "http://aetna.com/edh/libraries/edh-job-queue-core" at "/app/lib/edh-job-queue-core.xqy";

(:
 : Broker function used to schedule a job request in the host based on the free threads available
:)
declare function edh-job-queue-broker:run() as item()*{
  
    let $host-name := xdmp:host-name()
    let $request-type-map := edh-job-queue-core:get-request-type-for-host($host-name)

    let $job-xmls := for $type in map:keys($request-type-map)
                        let $xmls := if($type = "default") then
                                     (
                                        cts:search(fn:collection(),
                                                   cts:and-query((
                                                  	 cts:directory-query($config:EDH-OPTIONS/job-xml-base, "infinity"),
                                                  	 cts:element-attribute-value-query(xs:QName('type'), xs:QName('value'), $type),
                                                  	 cts:or-query((cts:element-value-query(xs:QName('job-status'), 'Added'),
                                                  	               cts:element-value-query(xs:QName('job-status'), 'Error')))
                                                  	)))
                                     )
                                     else
                                     (
                                        cts:search(fn:collection(),
                                                   cts:and-query((
                                                  	 cts:directory-query($config:EDH-OPTIONS/job-xml-base, "infinity"),
                                                  	 cts:element-value-query(xs:QName('type'), $type),
                                                  	 cts:or-query((cts:element-value-query(xs:QName('job-status'), 'Added'),
                                                  	               cts:element-value-query(xs:QName('job-status'), 'Error')))
                                                  )))
                                     )
                        let $xmls := for $xml in $xmls
                                      order by xs:int($xml/@priority) descending, xs:dateTime($xml/job/workflow/status[@type="Added"]/@dateTime) ascending, xs:dateTime($xml/job/workflow/status[@type="Error"]/@dateTime)
                                     return $xml
                        return $xmls[1 to map:get($request-type-map, $type)]
	let $request-count := fn:count($job-xmls)
	let $task-server-id :=xdmp:host-status(xdmp:host($host-name))/*:task-server/*:task-server-id/text()
    let $server-status := xdmp:server-status(xdmp:host($host-name),$task-server-id)
    let $host-free-threads := fn:data($server-status//*:max-threads) - fn:data($server-status//*:threads)
    let $job-xmls := if($host-free-threads >= $request-count) then
                        $job-xmls
                     else $job-xmls[1 to $host-free-threads]
	(:let $jobs :=
		cts:search(
		  fn:collection(),
		  cts:and-query((
		    cts:directory-query($config:EDH-OPTIONS/job-xml-base, "infinity")
		  ))
		):)
	
	for $job in $job-xmls
		let $xml := $job/element()
		let $_ := if($xml//workflow/status[fn:last()]/@type = 'Added' or ($xml//workflow/status[fn:last()]/@type = 'Error' and $xml//type/@num-of-retry > 0)) then
		          (		   
		            
		            let $job-uri := edh-job-queue-core:create-uri($xml/@id, $xml/@batch)
              		let $add-status := if($xml//workflow/status[fn:last()]/@type = 'Added') then 
              		                        edh-job-queue-core:add-job-status($job-uri, "Running")
              		                   else
              		                   (
              		                        edh-job-queue-core:add-job-status($job-uri, "Restarted"),
              		                        edh-job-queue-core:add-job-status($job-uri, "Running"),
              		                        edh-job-queue-core:set-num-of-retry($job-uri, $xml//type/text(), $xml//type/@num-of-retry)
              		                   )
              		let $_ := edh-job-queue-core:update-job-status($job-uri, "Running")
              		(:Insert a document for capturing execution specs for the job:)
              		let $_ := xdmp:document-insert(fn:replace($job-uri,".xml", "-execution-specs.xml"), <execution job-id="{$xml/@id}" batch-id="{$xml/@batch}"></execution>)
                    let $log := edh-message:log-job-queuing("log",
                                                            "job-queuing-error",
                                                            "job-queue",
                                                            "",
                                                            "",
                                                            fn:concat($job-uri," : Job exection specification document created")
                                                            )
                    let $run := try
                          		{
                                  if(fn:doc($job-uri)//workflow/status[fn:last()]/@type = 'Stopped' or fn:doc($job-uri)//workflow/status[fn:last()]/@type = 'Removed') then ()
                          		  else
                          		  (
                                     xdmp:spawn(
                                         "/app/lib/edh-execute-queued-job.xqy",
                                         (xs:QName("job-uri"), $job-uri, xs:QName("job-xml"), $xml),
                                         <options xmlns="xdmp:eval">
                                          <transaction-mode>update</transaction-mode>
                                         </options>
                                     ),
                                     xdmp:commit()
                                  )
                                  }
                                  catch($e)
                                  {
                                    let $log := edh-message:log-job-queuing("log",
                                                                    "job-queuing-error",
                                                                    "job-queue",
                                                                    "",
                                                                    "",
                                                                    $e
                                               )
                                    let $_ := edh-job-queue-core:add-job-status($job-uri, "Error")
                                    return edh-job-queue-core:update-job-status($job-uri, "Error")
                                 }
                    return () 
                  )
                  else ()
		order by xs:int($xml/@priority) descending, xs:dateTime($xml/workflow/status[@type="Added"]/@dateTime), xs:dateTime($xml/workflow/status[@type="restarted"]/@dateTime)
	return 
	 	$xml/@id
	 	
};


(:
 : Function used to stop the request running greater than request time out time and update the status based on the retry option
 : @param @request-id - id of the request which is running for long time
:)
declare function edh-job-queue-broker:stop-long-running-jobs($request-id as xs:string)  {
  let $jobs :=
		cts:search(
		  fn:collection(),
		  cts:and-query((
		    cts:directory-query($config:EDH-OPTIONS/job-xml-base, "infinity"),cts:element-value-query(xs:QName("request-id"),$request-id)
		  ))
		)
	for $job in $jobs
			let $job-exec-xml := $job/element()
			let $job-id := $job//@job-id
		    let $batch-id := $job//@batch-id
			let $job-uri := edh-job-queue-core:create-uri($job-id, $batch-id)
			let $job-exec-uri := fn:replace($job-uri,".xml", "-execution-specs.xml")
			let $response := try {
							         let $_ := xdmp:request-cancel($job-exec-xml/host-id/text(), $job-exec-xml/server-id/text(), $job-exec-xml/request-id/text())	
                                     let $log := edh-message:log-job-queuing("log",
                                                                            "job-queuing-error",
                                                                            "job-queue",
                                                                            "",
                                                                            "",
                                                                            concat($job-uri," : Job request cancelled due to time out")
                                                )
									 let $_ := if(xs:integer(fn:doc($job-uri)//type/@num-of-retry) > 0) then
									           (
									               edh-job-queue-core:add-job-status($job-uri, "Error"),
									               edh-job-queue-core:update-job-status($job-uri, "Error")
									           )
									           else
									           (
									               edh-job-queue-core:add-job-status($job-uri, "Error"),
									               edh-job-queue-core:add-job-status($job-uri, "Removed"),
									               edh-job-queue-core:update-job-status($job-uri, "Removed")
									           )
									 let $_ := xdmp:document-delete($job-exec-uri)
                                     let $log := edh-message:log-job-queuing("log",
                                                                            "job-queuing-error",
                                                                            "job-queue",
                                                                            "",
                                                                            "",
                                                                            concat($job-uri," : Job execution specification document deleted")
                                                )                            
									 return ()
								 }
								 catch($e) {
										      let $log := edh-message:log-job-queuing("log",
                                                            "job-queuing-error",
                                                            "job-queue",
                                                            "",
                                                            "",
                                                            $e
                                               )
                       		 return ()
                       }
		return ()
};
