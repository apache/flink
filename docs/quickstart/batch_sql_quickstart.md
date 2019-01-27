---
title: "Batch SQL Examples"
nav-title: Batch SQL Examples
nav-parent_id: examples
nav-pos: 25
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
software distributed under the License is distribuØted on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}

## Submit SQL Query via SQL Client

First, modify the sql client config file ./conf/sql-client-defaults.yaml to set execution type to `batch`.

<a href="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-config.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-config.png" alt="Batch SQL Example: config"/></a>

Then, start local cluster:

{% highlight bash %}
$ ./bin/start-cluster.sh
{% endhighlight %}

Check the web at [http://localhost:8081](http://localhost:8081) and make sure everything is up and running. The web frontend should report a single available TaskManager instance.

Prepare the input data:
{% highlight bash %}
$ cat /tmp/pagevisit.csv
2018-10-16 09:00:00,1001,/page1,chrome
2018-10-16 09:00:20,1001,/page2,safari
2018-10-16 09:03:20,1005,/page1,chrome
2018-10-16 09:05:50,1005,/page1,safari
2018-10-16 09:05:56,1005,/page2,safari
2018-10-16 09:05:57,1006,/page2,chrome
{% endhighlight %}

Then start SQL Client shell:

{% highlight bash %}
$ ./bin/sql-client.sh embedded
{% endhighlight %}

You can see the welcome message for flink sql client.

Paste the following sql ddl text into the shell. (For more information about sql ddl refer to [SQL]({{ site.baseurl }}/dev/table/sql.html) and [Supported DDL]({{ site.baseurl }}/dev/table/supported_ddl.html))

{% highlight bash %}
create table pagevisit (
    visit_time varchar,
    user_id bigint,
    visit_page varchar,
    browser_type varchar
) with (
    type = 'csv',
    path = 'file:///tmp/pagevisit.csv'
);
{% endhighlight %}

Press 'Enter' and paste the following sql dml text.
{% highlight bash %}
select 
  date_format(visit_time, 'yyyy-MM-dd HH:mm') as `visit_time`,
  count(user_id) as pv, 
  count(distinct user_id) as uv
from pagevisit
group by date_format(visit_time, 'yyyy-MM-dd HH:mm');
{% endhighlight %}

After press 'Enter' the sql will be submitted to the standalone cluster. The result will print on the shell.

<a href="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-result.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-result.png" alt="Batch SQL Example: result"/></a>

Open [http://localhost:8081](http://localhost:8081) and you can see the job information.

<a href="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-result-web.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-result-web.png" alt="Batch SQL Example: web"/></a>

For more information please refer to [SQL]({{ site.baseurl }}/dev/table/sql.html) and [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html).

## Submit SQL Query Programmatically
SQL queries can be submitted using the `sqlQuery()` method of the TableEnvironment programmatically. 

WordCountSQL shows how the Batch SQL API is used in Java.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
// set up the execution environment
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env);

DataStreamSource<WC> input = env.fromElements(
    new WC("Hello", 1),
    new WC("Ciao", 1),
    new WC("Hello", 1));

// register the BoundedStream as table "WordCount"

tEnv.registerBoundedStream("WC", input, "word, frequency");

// run a SQL query on the Table and retrieve the result as a new Table
Table table = tEnv.sqlQuery(
    "SELECT word, SUM(frequency) as frequency FROM WC GROUP BY word");

table.print();


// user-defined data types

/**
 * Simple POJO containing a word and its respective count.
 */
public static class WC {
    public String word;
    public long frequency;

    // public constructor to make it a Flink POJO
    public WC() {}

    public WC(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WC " + word + " " + frequency;
    }
}

{% endhighlight %}

The {% gh_link flink-examples/flink-examples-table/src/main/java/org/apache/flink/table/examples/java/WordCountSQL.java  "WordCountSQL" %} implements the above described algorithm.

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// set up the execution environment
val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = TableEnvironment.getBatchTableEnvironment(execEnv)
tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
val input = execEnv.fromCollection(List(WC("hello", 1), WC("hello", 1), WC("ciao", 1)))

// register the BoundedStream as table "WordCount"
tEnv.registerBoundedStream("WordCount", input, 'word, 'frequency)

// run a SQL query on the Table and retrieve the result as a new Table
tEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word").print()


// user-defined data types
case class WC(word: String, frequency: Long)
    
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-table/src/main/scala/org/apache/flink/table/examples/scala/WordCountSQL.scala  "WordCountSQL.scala" %} implements the above described algorithm.

</div>
</div>

To run the example, issue the following command and the result will print on the shell:

{% highlight bash %}
$ ./bin/flink run ./examples/table/WordCountSQL.jar
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-programm-wordcount-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-programm-wordcount-run.png" alt="Batch SQL Example: WordCountSQL result"/></a>

Open [http://localhost:8081](http://localhost:8081) and you can see the dashboard.

<a href="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-programm-wordcount-web.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-programm-wordcount-web.png" alt="SQL Example: WordCountSQL web"/></a>

Clink the job name: "Flink Exec Table Job", and you can see the detailed info page:

<a href="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-programm-wordcount-web2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/batch-sqlclient-example-programm-wordcount-web2.png" alt="SQL Example: WordCountSQL detail"/></a>

{% top %}
