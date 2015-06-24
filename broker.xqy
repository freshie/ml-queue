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

module namespace broker = "http://github.com/freshie/ml-queue/broker";

import module namespace config = "http://github.com/freshie/ml-queue/config" at "/config.xqy";
import module namespace core = "http://github.com/freshie/ml-queue/broker" at "/core.xqy";

declare namespace host = "http://marklogic.com/xdmp/status/host";
declare namespace status ="http://marklogic.com/xdmp/status/server";

(:
 : Broker function used to schedule a job request in the host based on the free threads available
:)
declare function broker:run() as item()* {

    let $request-type-map := core:get-request-type-for-host($config:currentHost)

    let $job-xmls :=
    	for $type in map:keys($request-type-map)
	        let $xmls :=
	        	if($type = "default") then
	                cts:search(fn:collection(),
	                           cts:and-query((
	                          	 cts:directory-query($config:job-xml-base, "infinity"),
	                          	 cts:element-attribute-value-query(xs:QName('type'), xs:QName('value'), $type),
	                          	 cts:or-query((cts:element-value-query(xs:QName('job-status'), 'Added'),
	                          	               cts:element-value-query(xs:QName('job-status'), 'Error')))
	                          	)))
	             else
	                cts:search(fn:collection(),
	                           cts:and-query((
	                          	 cts:directory-query($config:job-xml-base, "infinity"),
	                          	 cts:element-value-query(xs:QName('type'), $type),
	                          	 cts:or-query((cts:element-value-query(xs:QName('job-status'), 'Added'),
	                          	               cts:element-value-query(xs:QName('job-status'), 'Error')))
	                          )))

    	return
    	(
    		for $xml in $xmls
        	order by xs:int($xml/@priority) descending, xs:dateTime($xml/job/workflow/status[@type="Added"]/@dateTime) ascending, xs:dateTime($xml/job/workflow/status[@type="Error"]/@dateTime)
        	return $xml
        )[1 to map:get($request-type-map, $type)]

    let $hostID := xdmp:host($config:currentHost)
	let $request-count := fn:count($job-xmls)
	let $task-server-id := xdmp:host-status($hostID)/host:task-server/host:task-server-id/text()
    let $server-status :=  xdmp:server-status($hostID,$task-server-id)
    let $host-free-threads := xs:int($server-status/status:max-threads) - xs:int($server-status/status:threads)
    let $job-xmls :=
    	if($host-free-threads >= $request-count) then
            $job-xmls
        else
        	$job-xmls[1 to $host-free-threads]

	for $job in $job-xmls
		let $xml := $job/element()
		let $lastStatus := $xml/element()/workflow/status[fn:last()]/@type
		let $typeElement := $xml/element()/type
		let $_ :=
			if ($lastStatus eq 'Added' or ($lastStatus eq 'Error' and $typeElement/@num-of-retry > 0)) then
		          (
		            let $job-uri := core:create-uri($xml/@id, $xml/@batch)
              		let $add-status :=
              			if($lastStatus eq 'Added') then
	                        core:add-job-status($job-uri, "Running")
	                    else
	                    (
	                        core:add-job-status($job-uri, "Restarted"),
	                        core:add-job-status($job-uri, "Running"),
	                        core:set-num-of-retry($job-uri, $typeElement/text(), $typeElement/@num-of-retry)
	                    )
              		let $_ := core:update-job-status($job-uri, "Running")

              		(:Insert a document for capturing execution specs for the job:)
              		let $_ := xdmp:document-insert(fn:replace($job-uri,".xml", "-execution-specs.xml"), <execution job-id="{$xml/@id}" batch-id="{$xml/@batch}"></execution>, $config:permissions)

                    let $run :=
                    	try
                  		{
                          if ($lastStatus eq ('Stopped','Removed')) then
                          	()
                  		  else
                  		  (
                             xdmp:spawn(
                                 "/execute-job-wrapper.xqy",
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
                            let $_ := core:add-job-status($job-uri, "Error")
                            return core:update-job-status($job-uri, "Error")
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
declare function broker:stop-long-running-jobs($request-id as xs:string)  {
  	let $jobs :=
		cts:search(
		  fn:collection(),
		  cts:and-query((
		    cts:directory-query($config:EDH-OPTIONS/job-xml-base, "infinity"),
		    cts:element-value-query(xs:QName("request-id"),$request-id)
		  ))
		)

	for $job in $jobs
		let $job-exec-xml := $job/element()
		let $job-id := $job-exec-xml/@job-id
	    let $batch-id := $job-exec-xml/@batch-id
		let $job-uri := core:create-uri($job-id, $batch-id)
		let $job-exec-uri := fn:replace($job-uri,".xml", "-execution-specs.xml")
		let $response :=
			let $_ := xdmp:request-cancel($job-exec-xml/host-id/text(), $job-exec-xml/server-id/text(), $job-exec-xml/request-id/text())
			let $_ := if(xs:integer($job-exec-xml/type/@num-of-retry) > 0) then
			       (
			           core:add-job-status($job-uri, "Error"),
			           core:update-job-status($job-uri, "Error")
			       )
			       else
			       (
			           core:add-job-status($job-uri, "Error"),
			           core:add-job-status($job-uri, "Removed"),
			           core:update-job-status($job-uri, "Removed")
			       )
			let $_ := xdmp:document-delete($job-exec-uri)
			return ()
		return ()
};