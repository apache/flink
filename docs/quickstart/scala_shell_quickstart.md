---
title: "Scala Shell Examples"
nav-title: Scala Shell Examples
nav-parent_id: examples
nav-pos: 30
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

Flink comes with an integrated interactive Scala Shell.
It can be used in a local setup as well as in a cluster setup.

To get an overview of what options the Scala Shell provides, please use

{% highlight bash %}
$ ./bin/start-scala-shell.sh --help
 Flink Scala Shell
 Usage: start-scala-shell.sh [local|remote|yarn] [options] <args>...
 
 Command: local [options]
 Starts Flink scala shell with a local Flink cluster
   -a, --addclasspath <path/to/jar>
                            Specifies additional jars to be used in Flink
 Command: remote [options] <host> <port>
 Starts Flink scala shell connecting to a remote cluster
   <host>                   Remote host name as string
   <port>                   Remote port as integer
 
   -a, --addclasspath <path/to/jar>
                            Specifies additional jars to be used in Flink
 Command: yarn [options]
 Starts Flink scala shell connecting to a yarn cluster
   -n, --container arg      Number of YARN container to allocate (= Number of TaskManagers)
   -jm, --jobManagerMemory arg
                            Memory for JobManager container [in MB]
   -nm, --name <value>      Set a custom name for the application on YARN
   -qu, --queue <arg>       Specifies YARN queue
   -s, --slots <arg>        Number of slots per TaskManager
   -tm, --taskManagerMemory <arg>
                            Memory per TaskManager container [in MB]
   -a, --addclasspath <path/to/jar>
                            Specifies additional jars to be used in Flink
   --configDir <value>      The configuration directory.
   -h, --help               Prints this usage text
{% endhighlight %}

## Setup

If the standalone cluster has already started, you need to stop it first.
{% highlight bash %}
$ ./bin/stop-cluster.sh
{% endhighlight %}

Run the shell with an integrated Flink standalone cluster just execute:

{% highlight bash %}
$ ./bin/start-scala-shell.sh local
{% endhighlight %}

You will see the welcome message when start shell success.

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-welcome.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-welcome.png" alt="Scala Shell Example: logo"/></a>

Then, you can write the code under the scala shell.

Please refer [Scala Shell Documents]({{ site.baseurl }}/dev/scala_shell.html#setup) to to run scala shell in other cluster mode.

## Examples

The shell supports Batch and Streaming.
Two different ExecutionEnvironments are automatically prebound after startup.
Use "benv" and "senv" to access the Batch and Streaming environment respectively.

### SQL Query Example
In Scala Shell, users can also execute SQL queries calling sqlQuery() as following code shows:

{% highlight scala %}
Scala> val data = Seq(
    ("US", "Red", 10),
    ("UK", "Blue", 20),
    ("CN", "Yellow", 30),
    ("US", "Blue",40),
    ("UK","Red", 50),
    ("CN", "Red",60),
    ("US", "Yellow", 70),
    ("UK", "Yellow", 80),
    ("CN", "Blue", 90),
    ("US", "Blue", 100)
  )
Scala> val batchTable = btenv.fromCollection(data,'country,'color,'cnt)
Scala> btenv.registerTable("MyTable",batchTable)
Scala> val result = btenv.sqlQuery("SELECT * FROM MyTable WHERE cnt < 50").collect
{% endhighlight %}

The result will print on the shell.

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-sql-example-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-sql-example-run.png" alt="Scala Shell Example: SQL"/></a>

### DataStream Example

Similar to the batch program above, we can execute a streaming program through the DataStream API:

{% highlight scala %}
Scala> val textStreaming = senv.fromElements(
  "To be, or not to be,--that is the question:--",
  "Whether 'tis nobler in the mind to suffer",
  "The slings and arrows of outrageous fortune",
  "Or to take arms against a sea of troubles,")
Scala> val countsStreaming = textStreaming.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.keyBy(0).sum(1)
Scala> countsStreaming.print()
Scala> senv.execute("Streaming Wordcount")
{% endhighlight %}

The result will print on the shell.

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-datastream-example-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-datastream-example-run.png" alt="Scala Shell Example: DataStream"/></a>

### DataSet Example

The following example will execute the wordcount program in the Scala shell:

{% highlight scala %}
Scala> val text = benv.fromElements(
  "To be, or not to be,--that is the question:--",
  "Whether 'tis nobler in the mind to suffer",
  "The slings and arrows of outrageous fortune",
  "Or to take arms against a sea of troubles,")
Scala> val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
Scala> counts.print()
{% endhighlight %}

The print() command will automatically send the specified tasks to the JobManager for execution and will show the result of the computation in the terminal.

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-dataset-example-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-dataset-example-run.png" alt="Scala Shell Example: DataSet"/></a>

Note, that in the Streaming case, the print operation does not trigger execution directly.

### Table API Example

Scala Shell also support Table API. Users can execute a streaming Table program with `stenv` and 
a batch Table program with `btenv`. 

The `Table` API supports interactive programming, which allows users to cache an intermediate 
table for later usage. For example, in the following Scala Shell command sequence, table `t1` 
is cached and the result may be reused in later code.

{% highlight scala %}
Scala> val data = Seq(
    ("US", "Red", 10),
    ("UK", "Blue", 20),
    ("CN", "Yellow", 30),
    ("US", "Blue",40),
    ("UK","Red", 50),
    ("CN", "Red",60),
    ("US", "Yellow", 70),
    ("UK", "Yellow", 80),
    ("CN", "Blue", 90),
    ("US", "Blue", 100)
  )

Scala> val t = btenv.fromCollection(data).as ('country, 'color, 'amount)
Scala> val t1 = t.filter('amount < 100)
Scala> t1.cache
Scala> // t1 is cached after it is generated for the first time.
Scala> val x = t1.print

Scala> // When t1 is used again to generate t2, it may not be regenerated.
Scala> val t2 = t1.groupBy('country).select('country, 'amount.sum as 'sum)
Scala> val res2 = t2.print

Scala> // Similarly when t1 is used again to generate t2, it may not be regenerated.
Scala> val t3 = t1.groupBy('color).select('color, 'amount.avg as 'avg)
Scala> val res3 = t3.print
{% endhighlight %}

The result will print on the shell.

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-tableapi-example-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-scala-shell-tableapi-example-run.png" alt="Scala Shell Example: TableAPI"/></a>

Note: The cached tables will be cleaned up when the Scala Shell exit.

To see more information, please refer to [Scala Shell Documents]({{ site.baseurl }}/dev/scala_shell.html).

{% top %}
