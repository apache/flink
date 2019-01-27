---
title:  "Flink Interpreter for Apache Zeppelin"
nav-title: Flink on Zeppelin
nav-parent_id: ops
nav-pos: 11
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

* ToC
{:toc}

## Overview
[Apache Flink](https://flink.apache.org) is an open source platform for distributed stream and batch data processing. Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. Flink also builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization.


## How to configure Flink interpreter

Here's a list of properties that could be configured to customize Flink interpreter.

<table class="table table-bordered">
  <tr>
    <th>Property</th>
    <th>Default value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>flink.execution.mode</td>
    <td>local</td>
    <td>execution mode of flink. It could be local, yarn or remote</td>
  </tr>
  <tr>
    <td>flink.execution.remote.host</td>
    <td></td>
    <td>host name of job manager in remote mode</td>
  </tr>
  <tr>
    <td>flink.execution.remote.port</td>
    <td></td>
    <td>port of job manager rest service in remote mode</td>
  </tr>
  <tr>
    <td>flink.yarn.appName</td>
    <td></td>
    <td>Yarn app name of flink session</td>
  </tr>
  <tr>
    <td>flink.yarn.jm.memory</td>
    <td>1024</td>
    <td>Memory(mb) of JobManager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.memory</td>
    <td>1024</td>
    <td>Memory(mb) of TaskManager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.num</td>
    <td>2</td>
    <td>Number of TaskManager</td>
  </tr>
  <tr>
    <td>flink.yarn.tm.slot</td>
    <td>1</td>
    <td>Slot number per TaskManager</td>
  </tr>
  <tr>
    <td>flink.yarn.queue</td>
    <td>default</td>
    <td>Queue name for yarn app</td>
  </tr>  
  <tr>
    <td>zeppelin.flink.printREPLOutput</td>
    <td>true</td>
    <td>Whether to print repl output</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.maxResult</td>
    <td>1000</td>
    <td>Max rows of result for batch Sql output</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.concurrentBatchSql</td>
    <td>10</td>
    <td>Max number of batch sql executed concurrently</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.concurrentStreamSql</td>
    <td>10</td>
    <td>Max number of stream sql executed concurrently</td>
  </tr> 
  <tr>
    <td>zeppelin.flink.scala.color</td>
    <td>true</td>
    <td>Whether to enable color output of Scala Shell</td>
  </tr>                     
</table>

Besides these properties, you can also configure any flink properties that will override the value in `flink-conf.yaml`.
For more information about Flink configuration, you can find it [here](config.html).

### Run Flink in local mode

By default, Flink interpreter run in local mode as the default value of `flink.execution.mode` is `local`.
In local mode, Flink will launch one MiniCluster which include JobManager and TaskManagers in one JVM. But you can still customize the MiniCluster via the following properties:

* `local.number-taskmanage` This property specify how many TaskManagers in MiniCluster.
* `taskmanager.numberOfTaskSlot` This property specify how many slots for each TaskManager. By default it is 1.

### Run Flink in yarn mode

If you want to run Flink in yarn mode, you have to set the following properties:

* `flink.execution.mode` to be `yarn`
* `HADOOP_CONF_DIR` must be specified either in `zeppelin-env.sh` or in interpreter properties.

You can also customize the yarn mode via the following properties:

* `flink.yarn.jm.memory` Memory of JobManager
* `flink.yarn.tm.memory` Memory of TaskManager
* `flink.yarn.tm.num` Number of TaskManager
* `flink.yarn.tm.slot` Slot number per TaskManager
* `flink.yarn.queue` Queue name of yarn app

You have to set `query.proxy.ports` and `query.server.ports` to be a port range otherwise it is impossible to launch multiple TaskManager in one machine.

### Run Flink in standalone mode

If you want to run Flink in standalone mode, you have to set the following properties:

* `flink.execution.mode` to be `remote`
* `flink.execution.remote.host` to be the host name of JobManager
* `flink.execution.remote.port` to be the port of rest server of JobManager

## What can Flink Interpreter do

Zeppelin's Flink interpreter support 3 kinds of interpreter:

* `%flink` (FlinkScalaInterpreter, Run scala code)
* `%flink.bsql` (FlinkBatchSqlInterpreter, Run flink batch sql)
* `%flink.ssql` (FlinkStreamSqlInterpreter, Run flink stream sql)

### FlinkScalaInterpreter(`%flink`)

FlinkScalaInterpreter allow user to run scala code in zeppelin. 4 variables are created for users:

* senv  (StreamExecutionEnvironment)
* benv  (ExecutionEnvironment)
* stenv (StreamTableEnvironment)
* btenv (BatchTableEnvironment)

Users can use these variables to run DataSet/DataStream/BatchTable/StreamTable related job.

e.g. The following code snippet use `benv` to run a batch style WordCount

{% highlight scala %}
%flink

val data = benv.fromElements("hello world", "hello flink", "hello hadoop")
data.flatMap(line => line.split("\\s"))
  .map(w => (w, 1))
  .groupBy(0)
  .sum(1)
  .print()
{% endhighlight %}

The following use `senv` to run a stream style WordCount

{% highlight scala %}
%flink

val data = senv.fromElements("hello world", "hello flink", "hello hadoop")
data.flatMap(line => line.split("\\s"))
  .map(w => (w, 1))
  .keyBy(0)
  .sum(1)
  .print

senv.execute()
{% endhighlight %}

### FlinkBatchSqlInterpreter(`%flink.bsql`)

`FlinkBatchSqlInterpreter` support to run sql to query tables registered in `BatchTableEnvironment`(btenv).

e.g. We can query the `wc` table which is registered in scala code.

{% highlight scala %}
%flink

val data = senv.fromElements("hello world", "hello flink", "hello hadoop").
    flatMap(line => line.split("\\s")).
    map(w => (w, 1))

btenv.registerOrReplaceBoundedStream("wc", 
    data, 
    'word,'number)

{% endhighlight %}

{% highlight sql %}

%flink.bsql

select word, sum(number) as c from wc group by word 

{% endhighlight %}

### FlinkStreamSqlInterpreter(`%flink.ssql`)

Flink Interpreter also support stream sql via FlinkStreamSqlInterpreter(`%flink.ssql`) and also visualize the streaming data.

Overall there're 3 kinds of streaming sql supported by `%flink.ssql`:

* SingleRow
* Retract
* TimeSeries

#### SingleRow

This kind of sql only return one row of data, but this row will be updated continually. Usually this is used for tracking the aggregation result of some metrics. e.g.
total page view, total transactions and etc. Regarding this kind of sql, you can visualize it via html. Here's one example which calculate the total page view and visualize it via html.

{% highlight sql %}

%flink.ssql(type=single, parallelism=1, refreshInterval=3000, template=<h1>{1}</h1> until <h2>{0}</h2>, enableSavePoint=true, runWithSavePoint=true)

select max(rowtime), count(1) from log
{% endhighlight %}

<img src="{{BASE_PATH}}/page/img/zeppelin/flink_total_pv.png" width="700"/>

#### Retract

This kind of sql will return a fixed number of rows, but will be updated continually. Usually this is used for tracking the aggregation result of some metrics by some dimensions.
e.g. total page view per page, total transaction per country and etc. Regarding this kind of sql, you can visualize it via the built-in visualization charts of Zeppelin, such as barchart, linechart and etc.
Here's one example which calculate the total page view per page and visualize it via barchart.

{% highlight sql %}
%flink.ssql(type=retract, refreshInterval=2000, parallelism=2, enableSavePoint=true, runWithSavePoint=true)

select 
    url, 
    count(1) as pv
from log 
    group by url
{% endhighlight %}

<img src="{{BASE_PATH}}/page/img/zeppelin/flink_pv_per_page.png" width="700"/>

#### TimeSeries

This kind of sql will return a fixed number of rows regularly in timeseries. This is usually used for tracking metrics by time window.
e.g. Here's one example which calculate the page view for each 5 seconds window.

{% highlight sql %}
%flink.ssql(type=ts, refreshInterval=2000, enableSavePoint=false, runWithSavePoint=false, threshold=60000)

select
    TUMBLE_START(rowtime, INTERVAL '5' SECOND) as start_time,
    url,
    count(1) as pv
from log
    group by TUMBLE(rowtime, INTERVAL '5' SECOND), url
{% endhighlight  %}

<img src="{{BASE_PATH}}/page/img/zeppelin/flink_pv_ts.png" width="800"/>


#### Local Properties to customize Flink Stream Sql

Here's a list of properties that you can use to customize Flink stream sql

<table class="table table-bordered">
  <tr>
    <th>Property</th>
    <th>Default value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>type</td>
    <td></td>
    <td>single | retract | ts</td>
  </tr>
  <tr>
    <td>refreshInterval</td>
    <td>3000</td>
    <td>How oftern to refresh the result, it is in milliseconds.</td>
  </tr>
    <tr>
      <td>template</td>
      <td>{0}</td>
      <td>This is used for display the result of type singlerow. `{i}` represent the placehold of the `ith` field. You can also use html in the template, such as &lt;h1&gt;{0}&lt;/h1&gt;</td>
    </tr>
  <tr>
    <td>parallelism</td>
    <td></td>
    <td>The parallelism of this stream sql job</td>
  </tr>
  <tr>
    <td>enableSavePoint</td>
    <td>false</td>
    <td>Whether do savepoint when canceling job</td>
  </tr>
  <tr>
    <td>runWithSavePoint</td>
    <td>false</td>
    <td>Whether to run job from savepoint</td>
  </tr>
  <tr>
    <td>threshold</td>
    <td>3600000</td>
    <td>How much history data to keep for TimeSeries StreamJob, 1 hour by default</td>
  </tr>           
</table>


### Other Features

* Job Canceling
    - User can cancel job via the job cancel button
* Flink Job url association
    - Zeppelin will display the job url in paragraph
* Code completion
    - Like other interpreters, user can use `tab` for code completion
* ZeppelinContext
    - Flink interpreter also integrates ZeppelinContext. For how to use ZeppelinContext, please refer this [link](http://zeppelin.apache.org/docs/0.8.0/usage/other_features/zeppelin_context.html).
   
## FAQ

* Most of time, you will get clear error message when some unexpected happens. But you can still check the interpreter log in case the error message in frontend is not clear to you.
  The flink interpreter log is located in `ZEPPELIN_HOME/logs`
