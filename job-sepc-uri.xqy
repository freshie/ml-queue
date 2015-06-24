(:~
 : The module used to get the job execution specification xml
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

import module namespace config = "http://github.com/freshie/ml-queue/config" at "/config.xqy";

declare variable $job-id as xs:string external;

let $job-specs-uri := cts:uris((), 'document', cts:and-query((
																cts:directory-query($config:job-xml-base, 'infinity'),
                                                            	cts:element-attribute-value-query(xs:QName('execution'), xs:QName('job-id'), $job-id))
															))
return fn:doc($job-specs-uri)