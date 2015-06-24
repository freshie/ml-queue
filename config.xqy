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

module namespace config = "http://github.com/freshie/ml-queue/job";

(: is ussed to decided what host to look for jobs :)
declare variable $currentHost := xdmp:host-name();

(: the base uri for the jobs :)
declare variable $job-xml-base :=  "/queue/"

(: used when saving the job documents :)
declare variable $permissions := xdmp:default-permissions()

(: is expect to be put into an xml doc and queriable  :)
declare variable $environment := "LOCAL"

(: the amount of time that a job can run for  in minutes:)
declare variable $jobTimeOut := 30

(:
 sets the priority, number of threads, host details etc for a type of request
 This is expected to be pushed into an xml doc so that it could change for each environment

 the name attribute should be the hostname.

 ex: name="host1.domain.com"

 :)
declare variable $configuration :=
<config>
    <nodes>
        <node name="localhost">
            <types>
				<type priority='5' threads='2' name='loading' num-of-retry="3"/>
                <type priority='7' threads='4' name='transforming' num-of-retry="3"/>
                <type priority="4" threads="2" name="change-request" num-of-retry="3"/>
                <type priority='1' threads='1' name='default' num-of-retry="3"/>
            </types>
        </node>
    </nodes>
</config>

(: 	multi Node example :)
(:
<config>
    <nodes>
        <node name="host1.domain.com">
            <types>
                <type priority='5' threads='2' name='loading' num-of-retry="3"/>
                <type priority='7' threads='4' name='transforming' num-of-retry="3"/>
                <type priority="4" threads="2" name="change-request" num-of-retry="3"/>
                <type priority='1' threads='1' name='default' num-of-retry="3"/>
            </types>
        </node>
        <node name="host3.domain.com">
            <types>
                <type priority='5' threads='2' name='loading' num-of-retry="3"/>
                <type priority='7' threads='4' name='transforming' num-of-retry="3"/>
                <type priority="4" threads="2" name="change-request" num-of-retry="3"/>
                <type priority='1' threads='1' name='default' num-of-retry="3"/>
            </types>
        </node>
        <node name="host3.domain.com">
            <types>
                <type priority='5' threads='2' name='loading' num-of-retry="3"/>
                <type priority='7' threads='4' name='individual' num-of-retry="3"/>
                <type priority="4" threads="2" name="change-request" num-of-retry="3"/>
                <type priority='1' threads='1' name='default' num-of-retry="3"/>
                <type priority="1" threads="1" name="publishing" num-of-retry="3"/>
            </types>
        </node>
    </nodes>
</config>
:)