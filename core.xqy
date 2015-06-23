(:~
 : The module provides the functions used to add the requests to the queue for serving in future
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

module namespace core = "http://github.com/freshie/ml-queue/core";

import module namespace config = "http://github.com/freshie/ml-queue/config" at "/config.xqy";
import module namespace edh-governance = 'http://aetna.com/edh/libraries/edh-governance' at '/app/lib/edh-governance.xqy';

declare variable $currentHost := xdmp:host-name();

(:~
 : Function used to generate the unique id
:)
declare function core:get-guid() as xs:string {
  fn:generate-id(<xml/>)
};

(:~
 : Function used to get the priority of the type of request from the configuration file
 : @param $type - type of the request
:)
declare function core:get-priority(
  $type as xs:string
) as xs:int {
  (
    ($config:configuration/nodes/node[@name eq $currentHost]types/type[@name eq $type]/@priority)[1],
    $config:configuration/nodes/node[@name eq $currentHost]types/type[@name eq 'default']/@priority
  )[1]
};

(:~
 :uses get-priority
:)
declare function core:get-hosts(
  $type as xs:string
) as xs:int {
   core:get-priority($type)
};

(:~
 : Function used to get the number of retries for the type of the request
 : @param $type - type of the request
:)
declare function core:get-num-of-retry(
  $type as xs:string
) as xs:int {
   core:get-num-of-retry($type, $currentHost)
};

declare function core:get-num-of-retry(
  $type as xs:string,
  $host as xs:string
) as xs:int {
    (
    ($config:configuration/nodes/node[@name eq $currentHost]types/type[@name eq $type]/@num-of-retry)[1],
    $config:configuration/nodes/node[@name eq $currentHost]types/type[@name eq 'default']/@num-of-retry
    )[1]
};

(:~
 : Function used to set the number of retry in the job xml
 : @param $job-uri - uri of the job xml
 : @param $type - type of the request
 : @param $num-of-retry - number of retries
:)
declare function core:set-num-of-retry(
  $job-uri as xs:string,
  $type as xs:string,
  $num-of-retry as xs:int
) as item()* {
    let $retry := xs:integer($num-of-retry)-1
    let $value := fn:doc($job-uri)//type/@value
    return
      if($value = '' or fn:empty($value)) then
        xdmp:node-replace(fn:doc($job-uri)//type,  <type num-of-retry="{$retry}">{$type}</type>)
      else
        xdmp:node-replace(fn:doc($job-uri)//type,  <type num-of-retry="{$retry}" value="{$value}">{$type}</type>)
};

(:~
 : Function used to add the requests to queue
 : @param $namespace - Module namespace
 : @param $function - name of the function to trigger
 : @param $path - Module path
 : @param $type - type of the request
 : @param $content - map object input to the function
:)
declare function core:add-job(
  $namespace as xs:string,
  $function as xs:string,
  $path as xs:string,
  $type as xs:string,
  $content as item()*
) as xs:string {
   core:add-job($namespace, $function, $path, $type, $content, core:get-priority($type), core:get-guid())
};

(:~
 : Function used to add the requests to queue
 : @param $namespace - Module namespace
 : @param $function - name of the function to trigger
 : @param $path - Module path
 : @param $type - type of the request
 : @param $content - map object input to the function
 : @param $priorityIN - Customized priority for the type of the request
:)
declare function core:add-job(
  $namespace as xs:string,
  $function as xs:string,
  $path as xs:string,
  $type as xs:string,
  $content as item()*,
  $priorityIN as xs:int?
) as xs:string {
   let $priority :=
     if (fn:empty($priorityIN)) then
      core:get-priority($type)
     else
      $priorityIN
   return
    core:add-job($namespace, $function, $path, $type, $content, $priority, core:get-guid())
};

(:~
 : Function used to add the requests to queue
 : @param $namespace - Module namespace
 : @param $function - name of the function to trigger
 : @param $path - Module path
 : @param $type - type of the request
 : @param $content - map object input to the function
 : @param $priorityIN - Customized priority for the type of the request
 : @param $batchId - Unique id to identify the type of request in the same batch
:)
declare function core:add-job(
  $namespace as xs:string,
  $function as xs:string,
  $path as xs:string,
  $type as xs:string,
  $content as item()*,
  $priorityIN as xs:int?,
  $batchId as xs:string
) as xs:string {
  let $jobId := core:get-guid()
  let $priority :=
     if (fn:empty($priorityIN)) then core:get-priority($type)
     else $priorityIN
  let $value-type := $type

  (:Creating a job xml for the request:)
  let $newJob :=
    <job id="{$jobId}" batch="{$batchId}" priority="{$priority}">
      {if($value-type = "default") then
          <type num-of-retry="{core:get-num-of-retry($type)}" value ="{$value-type}">{$type}</type>
       else
          <type num-of-retry="{core:get-num-of-retry($type)}">{$type}</type>
      }
      <action>
        <namespace>{$namespace}</namespace>
        <function>{$function}</function>
        <path>{$path}</path>
      </action>
      {
        if (fn:exists($content)) then (
        <content>{$content}</content>
        ) else ()
      }
      <workflow>
        <status type="Added" dateTime="{fn:current-dateTime()}"/>
      </workflow>
      <job-status>Added</job-status>
    </job>
  let $uri := core:create-uri($jobId, $batchId)
  let $save := edh-governance:ingest-document($uri, $newJob)
  return $jobId
};

(:~
 : Function used to create the job xml uri
 : @param $jobId - unique id for the job
 : @param $batchId - uniqueid to identify the type of request in the same batch
:)
declare function core:create-uri(
  $jobId as xs:string,
  $batchId as xs:string
) as xs:string{
  $config:EDH-OPTIONS/job-xml-base || "batch/" || $batchId || "/job/" || $jobId || ".xml"
};


(:~
 : Function used to update the status of the job
 : @param $job-uri - uri of the job in which we need to update the status
 : @param $status - status to be updated
:)
declare function core:add-job-status(
  $job-uri as xs:string,
  $status as xs:string
){
    xdmp:node-insert-child(fn:doc($job-uri)//workflow, <status type="{$status}" dateTime="{fn:current-dateTime()}"/>)
};

(:~
 : Stub function used to add the requests to queue (process the request request imediatly)
 : @param $namespace - Module namespace
 : @param $function - name of the function to trigger
 : @param $path - Module path
 : @param $type - type of the request
 : @param $content - map object input to the function
:)
declare function core:add-job-to-queue(
  $namespace as xs:string,
  $function as xs:string,
  $path as xs:string,
  $type as xs:string,
  $content as item()*
) {
    if (fn:empty($content)) then
        xdmp:apply(
              xdmp:function(
                fn:QName($namespace, $function),
                $path
              )
            )
    else
      xdmp:apply(
              xdmp:function(
                fn:QName($namespace, $function),
                $path
              ),
              $content
            )
};

(:~
 : Function used to add the execution specifications (host-id, server-id, request-id) in the provided uri
 : @param $job-uri - uri in which the updates has to happen
 : @param $element-name - name of element which we are updating
 : @param $element-value - value of the element
:)
declare function core:add-execution-specifications(
  $job-uri as xs:string,
  $element-name as xs:string,
  $element-value as item()
) as item()* {
    xdmp:invoke-function(
      function() {
        (
           if($element-name = 'host-id' and fn:not(fn:exists(fn:doc($job-uri)//execution/host-id))) then
            xdmp:node-insert-child(fn:doc($job-uri)//execution, <host-id>{$element-value}</host-id>)
           else if($element-name = 'server-id' and fn:not(fn:exists(fn:doc($job-uri)//execution/server-id))) then
            xdmp:node-insert-child(fn:doc($job-uri)//execution, <server-id>{$element-value}</server-id>)
           else if($element-name = 'request-id' and fn:not(fn:exists(fn:doc($job-uri)//execution/request-id))) then
            xdmp:node-insert-child(fn:doc($job-uri)//execution, <request-id>{$element-value}</request-id>)
           else ()
         ),
          xdmp:commit()
        },
      <options xmlns="xdmp:eval">
        <transaction-mode>update</transaction-mode>
      </options>)
};

(:~
 : Function used to update the job status
 : @param $job-uri - uri in which the updates has to happen
 : @param $status - status of the job
:)
declare function core:update-job-status(
  $job-uri as xs:string,
  $status as xs:string
)as item()* {
    xdmp:node-replace(fn:doc($job-uri)//job-status, <job-status>{$status}</job-status>),
};

(:~
 : Function used to get types of the request which can be executed in host
 : @param $host-name - name of the host
:)
declare function core:get-request-type-for-host(
    $host-name as xs:string
) as map:map() {
    let $host-name :=
      if ($config:environment = 'LOCAL')
      then 'localhost'
      else $host-name
    let $host-configuration := $config:configuration/nodes/node[@name eq $host-name]
    let $request-type-map := map:map()
    let $_ :=
      for $type in $host-configuration/types/type
      return map:put($request-type-map, $type/@name, $type/@threads)
    return
      $request-type-map
};
