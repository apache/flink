---
title: "Quick Start: Run K-Means Example"
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

* This will be replaced by the TOC
{:toc}

This guide walks you through the steps of executing an example program ([K-Means clustering](http://en.wikipedia.org/wiki/K-means_clustering)) on Flink. On the way, you will see the a visualization of the program, the optimized execution plan, and track the progress of its execution.

## Setup Flink
Follow the [instructions](setup_quickstart.html) to setup Flink and enter the root directory of your Flink setup.

## Generate Input Data
Flink contains a data generator for K-Means.

~~~bash
# Assuming you are in the root directory of your Flink setup
mkdir kmeans
cd kmeans
# Run data generator
java -cp ../examples/KMeans.jar:../lib/flink-dist-{{ site.version }}.jar \
  org.apache.flink.examples.java.clustering.util.KMeansDataGenerator \
  -points 500 -k 10 -stddev 0.08 -output `pwd`
~~~

The generator has the following arguments (arguments in `[]` are optional):

~~~bash
-points <num> -k <num clusters> [-output <output-path>] [-stddev <relative stddev>] [-range <centroid range>] [-seed <seed>]
~~~

The _relative standard deviation_ is an interesting tuning parameter. It determines the closeness of the points to randomly generated centers.

The `kmeans/` directory should now contain two files: `centers` and `points`. The `points` file contains the points to cluster and the `centers` file contains initial cluster centers.


## Inspect the Input Data
Use the `plotPoints.py` tool to review the generated data points. [Download Python Script](plotPoints.py)

~~~ bash
python plotPoints.py points ./points input
~~~ 

Note: You might have to install [matplotlib](http://matplotlib.org/) (`python-matplotlib` package on Ubuntu) to use the Python script.

You can review the input data stored in the `input-plot.pdf`, for example with Evince (`evince input-plot.pdf`).

The following overview presents the impact of the different standard deviations on the input data.

|relative stddev = 0.03|relative stddev = 0.08|relative stddev = 0.15|
|:--------------------:|:--------------------:|:--------------------:|
|<img src="{{ site.baseurl }}/page/img/quickstart-example/kmeans003.png" alt="example1" style="width: 275px;"/>|<img src="{{ site.baseurl }}/page/img/quickstart-example/kmeans008.png" alt="example2" style="width: 275px;"/>|<img src="{{ site.baseurl }}/page/img/quickstart-example/kmeans015.png" alt="example3" style="width: 275px;"/>|


## Start Flink
Start Flink and the web job submission client on your local machine.

~~~ bash
# return to the Flink root directory
cd ..
# start Flink
./bin/start-local.sh
# Start the web client
./bin/start-webclient.sh
~~~

## Inspect and Run the K-Means Example Program
The Flink web client allows to submit Flink programs using a graphical user interface.

<div class="row" style="padding-top:15px">
	<div class="col-md-6">
		<a data-lightbox="compiler" href="{{ site.baseurl }}/page/img/webclient_job_view.png" data-lightbox="example-1"><img class="img-responsive" src="{{ site.baseurl }}/page/img/webclient_job_view.png" /></a>
	</div>
	<div class="col-md-6">
		1. Open web client on  <a href="http://localhost:8080/launch.html">localhost:8080</a> <br>
		2. Upload the K-Mean job JAR file. 
			{% highlight bash %}
			./examples/KMeans.jar
			{% endhighlight %} </br>
		3. Select it in the left box to see how the operators in the plan are connected to each other. <br>
		4. Enter the arguments and options in the lower left box: <br>
            Arguments: <br>
			{% highlight bash %}
			file://<pathToFlink>/kmeans/points file://<pathToFlink>/kmeans/centers file://<pathToFlink>/kmeans/result 10
			{% endhighlight %}
			For example:
			{% highlight bash %}
			file:///tmp/flink/kmeans/points file:///tmp/flink/kmeans/centers file:///tmp/flink/kmeans/result 10
			{% endhighlight %}
            Options (optional): (set the default parallelims, e.g., to 4) <br>
			{% highlight bash %}
            -p 4
			{% endhighlight %}
	</div>
</div>
<hr>
<div class="row" style="padding-top:15px">
	<div class="col-md-6">
		<a data-lightbox="compiler" href="{{ site.baseurl }}/page/img/webclient_plan_view.png" data-lightbox="example-1"><img class="img-responsive" src="{{ site.baseurl }}/page/img/webclient_plan_view.png" /></a>
	</div>

	<div class="col-md-6">
		1. Press the <b>RunJob</b> to see the optimizer plan. <br>
		2. Inspect the operators and see the properties (input sizes, cost estimation) determined by the optimizer.
	</div>
</div>
<hr>
<div class="row" style="padding-top:15px">
	<div class="col-md-6">
		<a data-lightbox="compiler" href="{{ site.baseurl }}/page/img/jobmanager.png" data-lightbox="example-1"><img class="img-responsive" src="{{ site.baseurl }}/page/img/jobmanager.png" /></a>
	</div>
	<div class="col-md-6">
		1. Press the <b>Continue</b> button to start executing the job. <br>
		2. <a href="http://localhost:8080/launch.html">Open Flink's monitoring interface</a> to see the job's progress. (Due to the small input data, the job will finish really quick!)<br>
		3. Once the job has finished, you can analyze the runtime of the individual operators.
	</div>
</div>

## Shutdown Flink
Stop Flink when you are done.

~~~ bash
# stop Flink
./bin/stop-local.sh
# Stop the Flink web client
./bin/stop-webclient.sh
~~~

## Analyze the Result
Use the [Python Script](plotPoints.py) again to visualize the result.

~~~bash
cd kmeans
python plotPoints.py result ./result clusters
~~~

The following three pictures show the results for the sample input above. Play around with the parameters (number of iterations, number of clusters) to see how they affect the result.


|relative stddev = 0.03|relative stddev = 0.08|relative stddev = 0.15|
|:--------------------:|:--------------------:|:--------------------:|
|<img src="{{ site.baseurl }}/page/img/quickstart-example/result003.png" alt="example1" style="width: 275px;"/>|<img src="{{ site.baseurl }}/page/img/quickstart-example/result008.png" alt="example2" style="width: 275px;"/>|<img src="{{ site.baseurl }}/page/img/quickstart-example/result015.png" alt="example3" style="width: 275px;"/>|

