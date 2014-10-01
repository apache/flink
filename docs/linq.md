---
title: "Language-Integrated Queries"
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

**Language-Integrated Queries are an experimental feature and can currently only be used with
the Scala API**

Flink provides an API that allows specifying operations using SQL-like expressions.
This Expression API can be enabled by importing
`org.apache.flink.api.scala.expressions._`.  This enables implicit conversions that allow
converting a `DataSet` or `DataStream` to an `ExpressionOperation` on which relational queries
can be specified. This example shows how a `DataSet` can be converted, how expression operations
can be specified and how an expression operation can be converted back to a `DataSet`:

{% highlight scala %}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.expressions._ 

case class WC(word: String, count: Int)
val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
val expr = input.toExpression
val result = expr.groupBy('word).select('word, 'count.sum).as[WC]
{% endhighlight %}

The expression DSL uses Scala symbols to refer to field names and we use code generation to
transform expressions to efficient runtime code. Please not that the conversion to and from
expression operations only works when using Scala case classes or Flink POJOs. Please check out
the [programming guide](programming_guide.html) to learn the requirements for a class to be 
considered a POJO.
 
This is another example that shows how you
can join to operations:

{% highlight scala %}
case class MyResult(a: String, b: Int)

val input1 = env.fromElements(...).as('a, 'b)
val input2 = env.fromElements(...).as('c, 'd)
val joined = input1.join(input2).where('b == 'a && 'd > 42).select('a, 'd).as[MyResult]
{% endhighlight %}

Notice, how a `DataSet` can be converted to an expression operation by using `as` and specifying new
names for the fields. This can also be used to disambiguate fields before a join operation.

The Expression API can be used with the Streaming API, since we also have implicit conversions to
and from `DataStream`.

The following dependency must be added to your project when using the Expression API:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-expressions</artifactId>
  <version>{{site.FLINK_VERSION_SHORT }}</version>
</dependency>
{% endhighlight %}

Please refer to the scaladoc for a full list of supported operations and a description of the
expression syntax. 
