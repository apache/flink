---
title: "Language-Integrated Queries (Table API)"
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

**Language-Integrated Queries are an experimental feature**

Flink provides an API that allows specifying operations using SQL-like expressions. Instead of 
manipulating `DataSet` or `DataStream` you work with `Table` on which relational operations can
be performed. 

The following dependency must be added to your project when using the Table API:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table</artifactId>
  <version>{{site.FLINK_VERSION_SHORT }}</version>
</dependency>
{% endhighlight %}

## Scala Table API
 
The Table API can be enabled by importing `org.apache.flink.api.scala.table._`.  This enables
implicit conversions that allow
converting a DataSet or DataStream to a Table. This example shows how a DataSet can
be converted, how relational queries can be specified and how a Table can be
converted back to a DataSet:

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._ 

case class WC(word: String, count: Int)
val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
val expr = input.toTable
val result = expr.groupBy('word).select('word, 'count.sum).toSet[WC]
{% endhighlight %}

The expression DSL uses Scala symbols to refer to field names and we use code generation to
transform expressions to efficient runtime code. Please note that the conversion to and from
Tables only works when using Scala case classes or Flink POJOs. Please check out
the [programming guide](programming_guide.html) to learn the requirements for a class to be 
considered a POJO.
 
This is another example that shows how you
can join to Tables:

{% highlight scala %}
case class MyResult(a: String, b: Int)

val input1 = env.fromElements(...).as('a, 'b)
val input2 = env.fromElements(...).as('c, 'd)
val joined = input1.join(input2).where("b = a && d > 42").select("a, d").as[MyResult]
{% endhighlight %}

Notice, how a DataSet can be converted to a Table by using `as` and specifying new
names for the fields. This can also be used to disambiguate fields before a join operation. Also,
in this example we see that you can also use Strings to specify relational expressions.

Please refer to the Scaladoc (and Javadoc) for a full list of supported operations and a
description of the expression syntax. 

## Java Table API

When using Java, Tables can be converted to and from DataSet and DataStream using `TableEnvironment`.
This example is equivalent to the above Scala Example:

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
TableEnvironment tableEnv = new TableEnvironment();

DataSet<WC> input = env.fromElements(
        new WC("Hello", 1),
        new WC("Ciao", 1),
        new WC("Hello", 1));

Table table = tableEnv.toTable(input);

Table filtered = table
        .groupBy("word")
        .select("word.count as count, word")
        .filter("count = 2");

DataSet<WC> result = tableEnv.toSet(filtered, WC.class);
{% endhighlight %}

When using Java, the embedded DSL for specifying expressions cannot be used. Only String expressions
are supported. They support exactly the same feature set as the expression DSL.

Please refer to the Javadoc for a full list of supported operations and a description of the
expression syntax. 


