---
title:  "Web Client"
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Flink provides a web interface to upload jobs, inspect their execution plans, and execute them. The interface is a great tool to showcase programs, debug execution plans, or demonstrate the system as a whole.

* This will be replaced by the TOC
{:toc}

## Starting, Stopping, and Configuring the Web Interface

Start the web interface by executing:

    ./bin/start-webclient.sh

and stop it by calling:

    ./bin/stop-webclient.sh

The web interface runs on port 8080 by default. To specify a custom port set the ```webclient.port``` property in the *./conf/flink.yaml* configuration file. Jobs are submitted to the JobManager specified by ```jobmanager.rpc.address``` and ```jobmanager.rpc.port```. Please consult the [configuration]({{ site.baseurl }}/setup/config.html#webclient) page for details and further configuration options.

## Using the Web Interface

The web interface provides two views:

1.  The **job view** to upload, preview, and submit Flink programs.
2.  The **plan view** to analyze the optimized execution plans of Flink programs.

### Job View

<div class="row">
	<div class="col-md-4">
		The interface starts serving the job view. <br>
		<br>
		You can <strong>upload</strong> a Flink program as a jar file. <br>
                <br>
		To <strong>execute</strong> an uploaded program:
		<ul>
			<li>select it from the job list on the left,</li>
			<li>enter (optional) execution options in the <em>"Flink Options"</em> field (bottom left),</li>
			<li>enter (optional) program arguments in the <em>"Program Arguments"</em> field (bottom left), and</li>
			<li>click on the <em>"Run Job"</em> button (bottom right).</li>
		</ul>
		If the <em>“Show optimizer plan”</em> option is enabled (default), the <em>plan view</em> is display next, otherwise the job is directly submitted to the JobManager for execution.
	</div>
	<div class="col-md-8">
		<a data-lightbox="compiler" href="{{ site.baseurl }}/page/img/webclient_job_view.png" data-lightbox="example-1"><img class="img-responsive" src="{{ site.baseurl }}/page/img/webclient_job_view.png" /></a>
	</div>
</div>

<br>

The web interface can also handle multiple Flink jobs within a single jar file. To use this feature, package all required class files of all jobs into a single jar and specify the entry classes for each job as comma-separated-values in *program-class* argument within the jar's manifest file. The job view displays each entry class and you can pick any of them to preview the plan and/or submit the job to the JobManager. In case the jar's manifest file does not specify any entry class, you can specify it in the options field as:

```
-c <assemblerClass>
```

It is also possible to set the default parallelism for the execution in options field as:

```
-p <dop>
```

Furthermore, for each entry class implementing ```ProgramDescription``` interface, the provided description is shown as tooltip for the job (see {% gh_link flink-examples/flink-java-examples/src/main/java/org/apache/flink/examples/java/wordcount/WordCountMeta.java  "WordCountMeta example" %}).

### Plan View

<div class="row">
	<div class="col-md-4">
		The plan view shows the optimized execution plan of the submitted program in the upper half of the page. The bottom part of the page displays detailed information about the currently selected plan operator including:
		<ul>
			<li>the chosen shipping strategies (local forward, hash partition, range partition, broadcast, ...),</li>
			<li>the chosen local strategy (sort, hash join, merge join, ...),</li>
			<li>inferred data properties (partitioning, grouping, sorting), and </li>
			<li>used optimizer estimates (data size, I/O and network costs, ...).</li>
		</ul>

		To submit the job for execution, click again on the <em>"Run Job"</em> button in the bottom right.
	</div>
	<div class="col-md-8">
		<a data-lightbox="compiler" href="{{ site.baseurl }}/page/img/webclient_plan_view.png" data-lightbox="example-1"><img class="img-responsive" src="{{ site.baseurl }}/page/img/webclient_plan_view.png" /></a>
	</div>
</div>
