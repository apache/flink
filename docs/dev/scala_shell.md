---
title: "Scala Shell"
nav-parent_id: start
nav-pos: 5
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

To use the shell with an integrated Flink cluster just execute:

{% highlight bash %}
bin/start-scala-shell.sh local
{% endhighlight %}

in the root directory of your binary Flink directory. To run the Shell on a
cluster, please see the Setup section below.

## Usage

The shell supports DataSet, DataStream, Table API and SQL.
Four different Environments are automatically prebound after startup.
Use "benv" and "senv" to access the Batch and Streaming ExecutionEnvironment respectively.
Use "btenv" and "stenv" to access BatchTableEnvironment and StreamTableEnvironment respectively.

### DataSet API

The following example will execute the wordcount program in the Scala shell:

{% highlight scala %}
Scala> val text = benv.fromElements(
  "To be, or not to be,--that is the question:--",
  "Whether 'tis nobler in the mind to suffer",
  "The slings and arrows of outrageous fortune",
  "Or to take arms against a sea of troubles,")
Scala> val counts = text.
    flatMap { _.toLowerCase.split("\\W+") }.
    map { (_, 1) }.groupBy(0).sum(1)
Scala> counts.print()
{% endhighlight %}

The print() command will automatically send the specified tasks to the JobManager for execution and will show the result of the computation in the terminal.

It is possible to write results to a file. However, in this case you need to call `execute`, to run your program:

{% highlight scala %}
Scala> benv.execute("MyProgram")
{% endhighlight %}

### DataStream API

Similar to the batch program above, we can execute a streaming program through the DataStream API:

{% highlight scala %}
Scala> val textStreaming = senv.fromElements(
  "To be, or not to be,--that is the question:--",
  "Whether 'tis nobler in the mind to suffer",
  "The slings and arrows of outrageous fortune",
  "Or to take arms against a sea of troubles,")
Scala> val countsStreaming = textStreaming.
    flatMap { _.toLowerCase.split("\\W+") }.
    map { (_, 1) }.keyBy(0).sum(1)
Scala> countsStreaming.print()
Scala> senv.execute("Streaming Wordcount")
{% endhighlight %}

Note, that in the Streaming case, the print operation does not trigger execution directly.

### Table API

The example below is a wordcount program using Table API:
<div class="codetabs" markdown="1">
<div data-lang="stream" markdown="1">
{% highlight scala %}
Scala> import org.apache.flink.table.api.functions.TableFunction
Scala> val textSource = stenv.fromDataStream(
  senv.fromElements(
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,"),
  'text)
Scala> class $Split extends TableFunction[String] {
    def eval(s: String): Unit = {
      s.toLowerCase.split("\\W+").foreach(collect)
    }
  }
Scala> val split = new $Split
Scala> textSource.join(split('text) as 'word).
    groupBy('word).select('word, 'word.count as 'count).
    toRetractStream[(String, Long)].print
Scala> senv.execute("Table Wordcount")
{% endhighlight %}
</div>
<div data-lang="batch" markdown="1">
{% highlight scala %}
Scala> import org.apache.flink.table.api.functions.TableFunction
Scala> val textSource = btenv.fromElements("To be, or not to be,--that is the question:--",
                 "Whether 'tis nobler in the mind to suffer",
                 "The slings and arrows of outrageous fortune",
                 "Or to take arms against a sea of troubles,").as('text)
Scala> class $Split extends TableFunction[String] {
    def eval(s: String): Unit = {
      s.toLowerCase.split("\\W+").foreach(collect)
    }
  }
Scala> val split = new $Split
Scala> val result = textSource.join(split('text) as 'word).
    groupBy('word).select('word, 'word.count as 'count).collect
Scala> result.foreach(println)
{% endhighlight %}
</div>
</div>

Note, that using $ as a prefix for the class name of TableFunction is a workaround of the issue that scala incorrectly generated inner class name.

### Interactive Programming

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
Scala> val x = t1.print

Scala> val t2 = t1.groupBy('country).select('country, 'amount.sum as 'sum)
Scala> val res2 = t2.print

Scala> val t3 = t1.groupBy('color).select('color, 'amount.avg as 'avg)
Scala> val res3 = t3.print
{% endhighlight %}

The result will print on the shell.

Note: The cached tables will be cleaned up when the Scala Shell exit.

### SQL

The following example is a wordcount program written in SQL:
<div class="codetabs" markdown="1">
<div data-lang="stream" markdown="1">
{% highlight scala %}
Scala> import org.apache.flink.table.functions.TableFunction
Scala> val textSource = stenv.fromDataStream(
  senv.fromElements(
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,"),
  'text)
Scala> stenv.registerTable("text_source", textSource)
Scala> class $Split extends TableFunction[String] {
    def eval(s: String): Unit = {
      s.toLowerCase.split("\\W+").foreach(collect)
    }
  }
Scala> stenv.registerFunction("split", new $Split)
Scala> val result = stenv.sqlQuery("""SELECT T.word, count(T.word) AS `count`
    FROM text_source
    JOIN LATERAL table(split(text)) AS T(word)
    ON TRUE
    GROUP BY T.word""")
Scala> result.toRetractStream[(String, Long)].print
Scala> senv.execute("SQL Wordcount")
{% endhighlight %}
</div>
<div data-lang="batch" markdown="1">
{% highlight scala %}
Scala> import org.apache.flink.table.functions.TableFunction
Scala> val textSource = btenv.fromElements("To be, or not to be,--that is the question:--",
       "Whether 'tis nobler in the mind to suffer",
       "The slings and arrows of outrageous fortune",
       "Or to take arms against a sea of troubles,").as('text)
Scala> btenv.registerTable("text_source", textSource)
Scala> class $Split extends TableFunction[String] {
    def eval(s: String): Unit = {
      s.toLowerCase.split("\\W+").foreach(collect)
    }
  }
Scala> btenv.registerFunction("split", new $Split)
Scala> val result = btenv.sqlQuery("""SELECT T.word, count(T.word) AS `count`
    FROM text_source
    JOIN LATERAL table(split(text)) AS T(word)
    ON TRUE
    GROUP BY T.word""")
Scala> result.collect.foreach(println)
{% endhighlight %}
</div>
</div>

## Adding external dependencies

It is possible to add external classpaths to the Scala-shell. These will be sent to the Jobmanager automatically alongside your shell program, when calling execute.

Use the parameter `-a <path/to/jar.jar>` or `--addclasspath <path/to/jar.jar>` to load additional classes.

{% highlight bash %}
bin/start-scala-shell.sh [local | remote <host> <port> | yarn] --addclasspath <path/to/jar.jar>
{% endhighlight %}


## Setup

To get an overview of what options the Scala Shell provides, please use

{% highlight bash %}
bin/start-scala-shell.sh --help
{% endhighlight %}

### Local

To use the shell with an integrated Flink cluster just execute:

{% highlight bash %}
bin/start-scala-shell.sh local
{% endhighlight %}


### Remote

To use it with a running cluster start the Scala Shell with the keyword `remote`
and supply the host and port of the JobManager with:

{% highlight bash %}
bin/start-scala-shell.sh remote <hostname> <portnumber>
{% endhighlight %}

Such as:

{% highlight bash %}
bin/start-scala-shell.sh remote localhost 6123
{% endhighlight %}
In this way, the Scala Shell will be started in local standalone cluster.

### Yarn Scala Shell cluster

The shell can deploy a Flink cluster to YARN, which is used exclusively by the
shell. The number of YARN containers can be controlled by the parameter `-n <arg>`.
The shell deploys a new Flink cluster on YARN and connects the
cluster. You can also specify options for YARN cluster such as memory for
JobManager, name of YARN application, etc.

For example, the following command will start a Yarn cluster named as "flink-yarn" for Scala Shell with 1 JobManager
and 2 TaskManagers. Each TaskManager starts with 1024MB memory and provides 2 slots.
{% highlight bash %}
 ./bin/start-scala-shell.sh yarn -n 2 -jm 1024 -s 2 -tm 1024 -nm flink-yarn
{% endhighlight %}

Note: Please make sure the environment variables HADOOP_HOME and
YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop have been correctly set.

For all other options, see the full reference at the bottom.


### Yarn Session

If you have previously deployed a Flink cluster using the Flink Yarn Session,
the Scala shell can connect with it using the following command:

{% highlight bash %}
 bin/start-scala-shell.sh yarn
{% endhighlight %}


## Full Reference

{% highlight bash %}
Flink Scala Shell
Usage: start-scala-shell.sh [local|remote|yarn] [options] <args>...

Command: local [options]
Starts Flink Scala Shell with a local Flink cluster
  -a <path/to/jar> | --addclasspath <path/to/jar>
        Specifies additional jars to be used in Flink
Command: remote [options] <host> <port>
Starts Flink Scala Shell connecting to a remote cluster
  <host>
        Remote host name as string
  <port>
        Remote port as integer

  -a <path/to/jar> | --addclasspath <path/to/jar>
        Specifies additional jars to be used in Flink
Command: yarn [options]
Starts Flink Scala Shell connecting to a yarn cluster
  -n arg | --container arg
        Number of YARN container to allocate (= Number of TaskManagers)
  -jm arg | --jobManagerMemory arg
        Memory for JobManager container [in MB]
  -nm <value> | --name <value>
        Set a custom name for the application on YARN
  -qu <arg> | --queue <arg>
        Specifies YARN queue
  -s <arg> | --slots <arg>
        Number of slots per TaskManager
  -tm <arg> | --taskManagerMemory <arg>
        Memory per TaskManager container [in MB]
  -a <path/to/jar> | --addclasspath <path/to/jar>
        Specifies additional jars to be used in Flink
  --configDir <value>
        The configuration directory.
  -h | --help
        Prints this usage text
{% endhighlight %}

{% top %}
