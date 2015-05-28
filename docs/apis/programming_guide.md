---
title: "Flink Programming Guide"
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

<a href="#top"></a>

Analysis programs in Flink are regular programs that implement transformations on data sets
(e.g., filtering, mapping, joining, grouping). The data sets are initially created from certain
sources (e.g., by reading files, or from collections). Results are returned via sinks, which may for
example write the data to (distributed) files, or to standard output (for example the command line
terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs.
The execution can happen in a local JVM, or on clusters of many machines.

In order to create your own Flink program, we encourage you to start with the
[program skeleton](#program-skeleton) and gradually add your own
[transformations](#transformations). The remaining sections act as references for additional
operations and advanced features.

* This will be replaced by the TOC
{:toc}

Example Program
---------------

The following program is a complete, working example of WordCount. You can copy &amp; paste the code
to run it locally. You only have to include the correct Flink's library into your project
(see Section [Linking with Flink](#linking-with-flink)) and specify the imports. Then you are ready
to go!

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
public class WordCountExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
            "Who's there?",
            "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple2<String, Integer>> wordCounts = text
            .flatMap(new LineSplitter())
            .groupBy(0)
            .sum(1);

        wordCounts.print();

        env.execute("Word Count Example");
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()

    env.execute("Scala WordCount Example")
  }
}
{% endhighlight %}
</div>

</div>

[Back to top](#top)


Linking with Flink
------------------

To write programs with Flink, you need to include the Flink library corresponding to
your programming language in your project.

The simplest way to do this is to use one of the quickstart scripts: either for
[Java]({{ site.baseurl }}/quickstart/java_api_quickstart.html) or for [Scala]({{ site.baseurl }}/quickstart/scala_api_quickstart.html). They
create a blank project from a template (a Maven Archetype), which sets up everything for you. To
manually create the project, you can use the archetype and create a project by calling:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight bash %}
mvn archetype:generate /
    -DarchetypeGroupId=org.apache.flink/
    -DarchetypeArtifactId=flink-quickstart-java /
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight bash %}
mvn archetype:generate /
    -DarchetypeGroupId=org.apache.flink/
    -DarchetypeArtifactId=flink-quickstart-scala /
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
</div>

If you want to add Flink to an existing Maven project, add the following entry to your
*dependencies* section in the *pom.xml* file of your project:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-java</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-scala</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

**Important:** When working with the Scala API you must have one of these two imports:
{% highlight scala %}
import org.apache.flink.api.scala._
{% endhighlight %}

or

{% highlight scala %}
import org.apache.flink.api.scala.createTypeInformation
{% endhighlight %}

The reason is that Flink analyzes the types that are used in a program and generates serializers
and comparaters for them. By having either of those imports you enable an implicit conversion
that creates the type information for Flink operations.
</div>
</div>

If you are using Flink together with Hadoop, the version of the dependency may vary depending on the
version of Hadoop (or more specifically, HDFS) that you want to use Flink with. Please refer to the
[downloads page]({{site.baseurl}}/downloads.html) for a list of available versions, and instructions
on how to link with custom versions of Hadoop.

In order to link against the latest SNAPSHOT versions of the code, please follow
[this guide]({{site.baseurl}}/downloads.html#nightly).

The *flink-clients* dependency is only necessary to invoke the Flink program locally (for example to
run it standalone for testing and debugging).  If you intend to only export the program as a JAR
file and [run it on a cluster](cluster_execution.html), you can skip that dependency.

[Back to top](#top)

Program Skeleton
----------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

As we already saw in the example, Flink programs look like regular Java
programs with a `main()` method. Each program consists of the same basic parts:

1. Obtain an `ExecutionEnvironment`,
2. Load/create the initial data,
3. Specify transformations on this data,
4. Specify where to put the results of your computations, and
5. Execute your program.

We will now give an overview of each of those steps but please refer to the respective sections for
more details. Note that all
{% gh_link /flink-java/src/main/java/org/apache/flink/api/java "core classes of the Java API" %}
are found in the package `org.apache.flink.api.java`.

The `ExecutionEnvironment` is the basis for all Flink programs. You can
obtain one using these static methods on class `ExecutionEnvironment`:

{% highlight java %}
getExecutionEnvironment()

createLocalEnvironment()
createLocalEnvironment(int parallelism)
createLocalEnvironment(Configuration customConfiguration)

createRemoteEnvironment(String host, int port, String... jarFiles)
createRemoteEnvironment(String host, int port, int parallelism, String... jarFiles)
{% endhighlight %}

Typically, you only need to use `getExecutionEnvironment()`, since this
will do the right thing depending on the context: if you are executing
your program inside an IDE or as a regular Java program it will create
a local environment that will execute your program on your local machine. If
you created a JAR file from you program, and invoke it through the [command line](cli.html)
or the [web interface](web_client.html),
the Flink cluster manager will
execute your main method and `getExecutionEnvironment()` will return
an execution environment for executing your program on a cluster.

For specifying data sources the execution environment has several methods
to read from files using various methods: you can just read them line by line,
as CSV files, or using completely custom data input formats. To just read
a text file as a sequence of lines, you can use:

{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<String> text = env.readTextFile("file:///path/to/file");
{% endhighlight %}

This will give you a DataSet on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataSet you can apply transformations to create a new
DataSet which you can then write to a file, transform again, or
combine with other DataSets. You apply transformations by calling
methods on DataSet with your own custom transformation function. For example,
a map transformation looks like this:

{% highlight java %}
DataSet<String> input = ...;

DataSet<Integer> tokenized = text.map(new MapFunction<String, Integer>() {
    @Override
    public Integer map(String value) {
        return Integer.parseInt(value);
    }
});
{% endhighlight %}

This will create a new DataSet by converting every String in the original
set to an Integer. For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataSet that needs to be written to disk you call one
of these methods on DataSet:

{% highlight java %}
writeAsText(String path)
writeAsCsv(String path)
write(FileOutputFormat<T> outputFormat, String filePath)

print()
{% endhighlight %}

The last method is only useful for developing/debugging on a local machine,
it will output the contents of the DataSet to standard output. (Note that in
a cluster, the result goes to the standard out stream of the cluster nodes and ends
up in the *.out* files of the workers).
The first two do as the name suggests, the third one can be used to specify a
custom data output format. Please refer
to [Data Sinks](#data-sinks) for more information on writing to files and also
about custom data output formats.

Once you specified the complete program you need to call `execute` on
the `ExecutionEnvironment`. This will either execute on your local
machine or submit your program for execution on a cluster, depending on
how you created the execution environment.

</div>
<div data-lang="scala" markdown="1">

As we already saw in the example, Flink programs look like regular Scala
programs with a `main()` method. Each program consists of the same basic parts:

1. Obtain an `ExecutionEnvironment`,
2. Load/create the initial data,
3. Specify transformations on this data,
4. Specify where to put the results of your computations, and
5. Execute your program.

We will now give an overview of each of those steps but please refer to the respective sections for
more details. Note that all core classes of the Scala API are found in the package 
{% gh_link /flink-scala/src/main/scala/org/apache/flink/api/scala "org.apache.flink.api.scala" %}.


The `ExecutionEnvironment` is the basis for all Flink programs. You can
obtain one using these static methods on class `ExecutionEnvironment`:

{% highlight scala %}
def getExecutionEnvironment

def createLocalEnvironment(parallelism: Int = Runtime.getRuntime.availableProcessors()))
def createLocalEnvironment(customConfiguration: Configuration)

def createRemoteEnvironment(host: String, port: String, jarFiles: String*)
def createRemoteEnvironment(host: String, port: String, parallelism: Int, jarFiles: String*)
{% endhighlight %}

Typically, you only need to use `getExecutionEnvironment()`, since this
will do the right thing depending on the context: if you are executing
your program inside an IDE or as a regular Scala program it will create
a local environment that will execute your program on your local machine. If
you created a JAR file from you program, and invoke it through the [command line](cli.html)
or the [web interface](web_client.html),
the Flink cluster manager will
execute your main method and `getExecutionEnvironment()` will return
an execution environment for executing your program on a cluster.

For specifying data sources the execution environment has several methods
to read from files using various methods: you can just read them line by line,
as CSV files, or using completely custom data input formats. To just read
a text file as a sequence of lines, you can use:

{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()

val text = env.readTextFile("file:///path/to/file")
{% endhighlight %}

This will give you a DataSet on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataSet you can apply transformations to create a new
DataSet which you can then write to a file, transform again, or
combine with other DataSets. You apply transformations by calling
methods on DataSet with your own custom transformation function. For example,
a map transformation looks like this:

{% highlight scala %}
val input: DataSet[String] = ...

val mapped = text.map { x => x.toInt }
{% endhighlight %}

This will create a new DataSet by converting every String in the original
set to an Integer. For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataSet that needs to be written to disk you can call one
of these methods on DataSet:

{% highlight scala %}
def writeAsText(path: String, writeMode: WriteMode = WriteMode.NO_OVERWRITE)
def writeAsCsv(
    filePath: String,
    rowDelimiter: String = "\n",
    fieldDelimiter: String = ',',
    writeMode: WriteMode = WriteMode.NO_OVERWRITE)
def write(outputFormat: FileOutputFormat[T],
    path: String,
    writeMode: WriteMode = WriteMode.NO_OVERWRITE)

def print()
{% endhighlight %}

The last method is only useful for developing/debugging on a local machine,
it will output the contents of the DataSet to standard output. (Note that in
a cluster, the result goes to the standard out stream of the cluster nodes and ends
up in the *.out* files of the workers).
The first two do as the name suggests, the third one can be used to specify a
custom data output format. Please refer
to [Data Sinks](#data-sinks) for more information on writing to files and also
about custom data output formats.

Once you specified the complete program you need to call `execute` on
the `ExecutionEnvironment`. This will either execute on your local
machine or submit your program for execution on a cluster, depending on
how you created the execution environment.

</div>
</div>


[Back to top](#top)


Lazy Evaluation
---------------

All Flink programs are executed lazily: When the program's main method is executed, the data loading
and transformations do not happen directly. Rather, each operation is created and added to the
program's plan. The operations are actually executed when one of the `execute()` methods is invoked
on the ExecutionEnvironment object. Whether the program is executed locally or on a cluster depends
on the environment of the program.

The lazy evaluation lets you construct sophisticated programs that Flink executes as one
holistically planned unit.

[Back to top](#top)


Transformations
---------------

Data transformations transform one or more DataSets into a new DataSet. Programs can combine
multiple transformations into sophisticated assemblies.

This section gives a brief overview of the available transformations. The [transformations
documentation](dataset_transformations.html) has a full description of all transformations with
examples.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>Map</strong></td>
      <td>
        <p>Takes one element and produces one element.</p>
{% highlight java %}
data.map(new MapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FlatMap</strong></td>
      <td>
        <p>Takes one element and produces zero, one, or more elements. </p>
{% highlight java %}
data.flatMap(new FlatMapFunction<String, String>() {
  public void flatMap(String value, Collector<String> out) {
    for (String s : value.split(" ")) {
      out.collect(s);
    }
  }
});
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>MapPartition</strong></td>
      <td>
        <p>Transforms a parallel partition in a single function call. The function get the partition
        as an `Iterable` stream and can produce an arbitrary number of result values. The number of
        elements in each partition depends on the degree-of-parallelism and previous operations.</p>
{% highlight java %}
data.mapPartition(new MapPartitionFunction<String, Long>() {
  public void mapPartition(Iterable<String> values, Collector<Long> out) {
    long c = 0;
    for (String s : values) {
      c++;
    }
    out.collect(c);
  }
});
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong></td>
      <td>
        <p>Evaluates a boolean function for each element and retains those for which the function
        returns true.<br/>
        
        <strong>IMPORTANT:</strong> The system assumes that the function does not modify the elements on which the predicate is applied. Violating this assumption
        can lead to incorrect results.
        </p>
{% highlight java %}
data.filter(new FilterFunction<Integer>() {
  public boolean filter(Integer value) { return value > 1000; }
});
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Reduce</strong></td>
      <td>
        <p>Combines a group of elements into a single element by repeatedly combining two elements
        into one. Reduce may be applied on a full data set, or on a grouped data set.</p>
{% highlight java %}
data.reduce(new ReduceFunction<Integer> {
  public Integer reduce(Integer a, Integer b) { return a + b; }
});
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>ReduceGroup</strong></td>
      <td>
        <p>Combines a group of elements into one or more elements. ReduceGroup may be applied on a
        full data set, or on a grouped data set.</p>
{% highlight java %}
data.reduceGroup(new GroupReduceFunction<Integer, Integer> {
  public void reduce(Iterable<Integer> values, Collector<Integer> out) {
    int prefixSum = 0;
    for (Integer i : values) {
      prefixSum += i;
      out.collect(prefixSum);
    }
  }
});
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Aggregate</strong></td>
      <td>
        <p>Aggregates a group of values into a single value. Aggregation functions can be thought of
        as built-in reduce functions. Aggregate may be applied on a full data set, or on a grouped
        data set.</p>
{% highlight java %}
Dataset<Tuple3<Integer, String, Double>> input = // [...]
DataSet<Tuple3<Integer, String, Double>> output = input.aggregate(SUM, 0).and(MIN, 2);
{% endhighlight %}
	<p>You can also use short-hand syntax for minimum, maximum, and sum aggregations.</p>
	{% highlight java %}
	Dataset<Tuple3<Integer, String, Double>> input = // [...]
DataSet<Tuple3<Integer, String, Double>> output = input.sum(0).andMin(2);
	{% endhighlight %}
      </td>
    </tr>

    </tr>
      <td><strong>Join</strong></td>
      <td>
        Joins two data sets by creating all pairs of elements that are equal on their keys.
        Optionally uses a JoinFunction to turn the pair of elements into a single element, or a
        FlatJoinFunction to turn the pair of elements into arbitararily many (including none)
        elements. See <a href="#specifying-keys">keys</a> on how to define join keys.
{% highlight java %}
result = input1.join(input2)
               .where(0)       // key of the first input (tuple field 0)
               .equalTo(1);    // key of the second input (tuple field 1)
{% endhighlight %}
        You can specify the way that the runtime executes the join via <i>Join Hints</i>. The hints
        describe whether the join happens through partitioning or broadcasting, and whether it uses
        a sort-based or a hash-based algorithm. Please refer to the 
        <a href="dataset_transformations.html#join-algorithm-hints">Transformations Guide</a> for
        a list of possible hints and an example.</br>
        If no hint is specified, the system will try to make an estimate of the input sizes and
        pick a the best strategy according to those estimates. 
{% highlight java %}
// This executes a join by broadcasting the first data set
// using a hash table for the broadcasted data
result = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST)
               .where(0).equalTo(1);
{% endhighlight %}

      </td>
    </tr>

    <tr>
      <td><strong>CoGroup</strong></td>
      <td>
        <p>The two-dimensional variant of the reduce operation. Groups each input on one or more
        fields and then joins the groups. The transformation function is called per pair of groups.
        See <a href="#specifying-keys">keys</a> on how to define coGroup keys.</p>
{% highlight java %}
data1.coGroup(data2)
     .where(0)
     .equalTo(1)
     .with(new CoGroupFunction<String, String, String>() {
         public void coGroup(Iterable<String> in1, Iterable<String> in2, Collector<String> out) {
           out.collect(...);
         }
      });
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Cross</strong></td>
      <td>
        <p>Builds the Cartesian product (cross product) of two inputs, creating all pairs of
        elements. Optionally uses a CrossFunction to turn the pair of elements into a single
        element</p>
{% highlight java %}
DataSet<Integer> data1 = // [...]
DataSet<String> data2 = // [...]
DataSet<Tuple2<Integer, String>> result = data1.cross(data2);
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Produces the union of two data sets. This operation happens implicitly if more than one
        data set is used for a specific function input.</p>
{% highlight java %}
DataSet<String> data1 = // [...]
DataSet<String> data2 = // [...]
DataSet<String> result = data1.union(data2);
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Rebalance</strong></td>
      <td>
        <p>Evenly rebalances the parallel partitions of a data set to eliminate data skew. Only Map-like transformations may follow a rebalance transformation. (Java API Only)</p>
{% highlight java %}
DataSet<String> in = // [...]
DataSet<String> result = in.rebalance()
                           .map(new Mapper());
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Hash-Partition</strong></td>
      <td>
        <p>Hash-partitions a data set on a given key. Keys can be specified as key-selector functions or field position keys.</p>
{% highlight java %}
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionByHash(0)
                            .mapPartition(new PartitionMapper());
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Custom Partitioning</strong></td>
      <td>
        <p>Manually specify a partitioning over the data.
          <br/>
          <i>Note</i>: This method works only on single field keys.</p>
{% highlight java %}
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionCustom(Partitioner<K> partitioner, key)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Sort Partition</strong></td>
      <td>
        <p>Locally sorts all partitions of a data set on a specified field in a specified order. 
          Fields can be specified as tuple positions or field expressions. 
          Sorting on multiple fields is done by chaining sortPartition() calls.</p>
{% highlight java %}
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.sortPartition(1, Order.ASCENDING)
                            .mapPartition(new PartitionMapper());
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>First-n</strong></td>
      <td>
        <p>Returns the first n (arbitrary) elements of a data set. First-n can be applied on a regular data set, a grouped data set, or a grouped-sorted data set. Grouping keys can be specified as key-selector functions or field position keys.</p>
{% highlight java %}
DataSet<Tuple2<String,Integer>> in = // [...]
// regular data set
DataSet<Tuple2<String,Integer>> result1 = in.first(3);
// grouped data set
DataSet<Tuple2<String,Integer>> result2 = in.groupBy(0)
                                            .first(3);
// grouped-sorted data set
DataSet<Tuple2<String,Integer>> result3 = in.groupBy(0)
                                            .sortGroup(1, Order.ASCENDING)
                                            .first(3);
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

----------

The following transformations are available on data sets of Tuples:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Project</strong></td>
      <td>
        <p>Selects a subset of fields from the tuples</p>
{% highlight java %}
DataSet<Tuple3<Integer, Double, String>> in = // [...]
DataSet<Tuple2<String, Integer>> out = in.project(2,0);
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

</div>
<div data-lang="scala" markdown="1">
<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>Map</strong></td>
      <td>
        <p>Takes one element and produces one element.</p>
{% highlight scala %}
data.map { x => x.toInt }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FlatMap</strong></td>
      <td>
        <p>Takes one element and produces zero, one, or more elements. </p>
{% highlight scala %}
data.flatMap { str => str.split(" ") }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>MapPartition</strong></td>
      <td>
        <p>Transforms a parallel partition in a single function call. The function get the partition
        as an `Iterator` and can produce an arbitrary number of result values. The number of
        elements in each partition depends on the degree-of-parallelism and previous operations.</p>
{% highlight scala %}
data.mapPartition { in => in map { (_, 1) } }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong></td>
      <td>
        <p>Evaluates a boolean function for each element and retains those for which the function
        returns true.<br/>
        <strong>IMPORTANT:</strong> The system assumes that the function does not modify the element on which the predicate is applied.
        Violating this assumption can lead to incorrect results.</p>
{% highlight scala %}
data.filter { _ > 1000 }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Reduce</strong></td>
      <td>
        <p>Combines a group of elements into a single element by repeatedly combining two elements
        into one. Reduce may be applied on a full data set, or on a grouped data set.</p>
{% highlight scala %}
data.reduce { _ + _ }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>ReduceGroup</strong></td>
      <td>
        <p>Combines a group of elements into one or more elements. ReduceGroup may be applied on a
        full data set, or on a grouped data set.</p>
{% highlight scala %}
data.reduceGroup { elements => elements.sum }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Aggregate</strong></td>
      <td>
        <p>Aggregates a group of values into a single value. Aggregation functions can be thought of
        as built-in reduce functions. Aggregate may be applied on a full data set, or on a grouped
        data set.</p>
{% highlight scala %}
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Doublr)] = input.aggregate(SUM, 0).aggregate(MIN, 2);
{% endhighlight %}
  <p>You can also use short-hand syntax for minimum, maximum, and sum aggregations.</p>
{% highlight scala %}
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Doublr)] = input.sum(0).min(2)
{% endhighlight %}
      </td>
    </tr>

    </tr>
      <td><strong>Join</strong></td>
      <td>
        Joins two data sets by creating all pairs of elements that are equal on their keys.
        Optionally uses a JoinFunction to turn the pair of elements into a single element, or a
        FlatJoinFunction to turn the pair of elements into arbitararily many (including none)
        elements. See <a href="#specifying-keys">keys</a> on how to define join keys.
{% highlight scala %}
// In this case tuple fields are used as keys. "0" is the join field on the first tuple
// "1" is the join field on the second tuple.
val result = input1.join(input2).where(0).equalTo(1)
{% endhighlight %}
        You can specify the way that the runtime executes the join via <i>Join Hints</i>. The hints
        describe whether the join happens through partitioning or broadcasting, and whether it uses
        a sort-based or a hash-based algorithm. Please refer to the 
        <a href="dataset_transformations.html#join-algorithm-hints">Transformations Guide</a> for
        a list of possible hints and an example.</br>
        If no hint is specified, the system will try to make an estimate of the input sizes and
        pick a the best strategy according to those estimates.
{% highlight scala %}
// This executes a join by broadcasting the first data set
// using a hash table for the broadcasted data
val result = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST)
                   .where(0).equalTo(1)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>CoGroup</strong></td>
      <td>
        <p>The two-dimensional variant of the reduce operation. Groups each input on one or more
        fields and then joins the groups. The transformation function is called per pair of groups.
        See <a href="#specifying-keys">keys</a> on how to define coGroup keys.</p>
{% highlight scala %}
data1.coGroup(data2).where(0).equalTo(1)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Cross</strong></td>
      <td>
        <p>Builds the Cartesian product (cross product) of two inputs, creating all pairs of
        elements. Optionally uses a CrossFunction to turn the pair of elements into a single
        element</p>
{% highlight scala %}
val data1: DataSet[Int] = // [...]
val data2: DataSet[String] = // [...]
val result: DataSet[(Int, String)] = data1.cross(data2)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Produces the union of two data sets.</p>
{% highlight scala %}
data.union(data2)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Hash-Partition</strong></td>
      <td>
        <p>Hash-partitions a data set on a given key. Keys can be specified as key-selector functions, tuple positions
        or case class fields.</p>
{% highlight scala %}
val in: DataSet[(Int, String)] = // [...]
val result = in.partitionByHash(0).mapPartition { ... }
{% endhighlight %}
      </td>
    </tr>
    </tr>
    <tr>
      <td><strong>Sort Partition</strong></td>
      <td>
        <p>Locally sorts all partitions of a data set on a specified field in a specified order. 
          Fields can be specified as tuple positions or field expressions. 
          Sorting on multiple fields is done by chaining sortPartition() calls.</p>
{% highlight scala %}
val in: DataSet[(Int, String)] = // [...]
val result = in.sortPartition(1, Order.ASCENDING).mapPartition { ... }
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>First-n</strong></td>
      <td>
        <p>Returns the first n (arbitrary) elements of a data set. First-n can be applied on a regular data set, a grouped data set, or a grouped-sorted data set. Grouping keys can be specified as key-selector functions,
        tuple positions or case class fields.</p>
{% highlight scala %}
val in: DataSet[(Int, String)] = // [...]
// regular data set
val result1 = in.first(3)
// grouped data set
val result2 = in.groupBy(0).first(3)
// grouped-sorted data set
val result3 = in.groupBy(0).sortGroup(1, Order.ASCENDING).first(3)
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

The [parallelism](#parallel-execution) of a transformation can be defined by `setParallelism(int)` while
`name(String)` assigns a custom name to a transformation which is helpful for debugging. The same is
possible for [Data Sources](#data-sources) and [Data Sinks](#data-sinks).

[Back to Top](#top)


Specifying Keys
-------------



Some transformations (join, coGroup) require that a key is defined on
its argument DataSets, and other transformations (Reduce, GroupReduce,
Aggregate) allow that the DataSet is grouped on a key before they are
applied.

A DataSet is grouped as
{% highlight java %}
DataSet<...> input = // [...]
DataSet<...> reduced = input
	.groupBy(/*define key here*/)
	.reduceGroup(/*do something*/);
{% endhighlight %}

The data model of Flink is not based on key-value pairs. Therefore,
you do not need to physically pack the data set types into keys and
values. Keys are "virtual": they are defined as functions over the
actual data to guide the grouping operator.

### Define keys for Tuples
{:.no_toc}

The simplest case is grouping a data set of Tuples on one or more
fields of the Tuple:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Tuple3<Integer,String,Long>> input = // [...]
DataSet<Tuple3<Integer,String,Long> grouped = input
	.groupBy(0)
	.reduceGroup(/*do something*/);
{% endhighlight %}

The data set is grouped on the first field of the tuples (the one of
Integer type). The GroupReduce function will thus receive groups of tuples with
the same value in the first field.

{% highlight java %}
DataSet<Tuple3<Integer,String,Long>> input = // [...]
DataSet<Tuple3<Integer,String,Long> grouped = input
	.groupBy(0,1)
	.reduce(/*do something*/);
{% endhighlight %}

The data set is grouped on the composite key consisting of the first and the
second field. Therefore, the GroupReduce function will receive groups
with the same value for both fields.

A note on nested Tuples: If you have a DataSet with a nested tuple, such as:

{% highlight java %}
DataSet<Tuple3<Tuple2<Integer, Float>,String,Long>> ds;
{% endhighlight %}

Specifying `groupBy(0)` will cause the system to use the full `Tuple2` as a key (with the Integer and Float being the key). If you want to "navigate" into the nested `Tuple2`, you have to use field expression keys which are explained below.

</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataSet[(Int, String, Long)] = // [...]
val grouped = input
  .groupBy(0)
  .reduceGroup(/*do something*/)
{% endhighlight %}

The data set is grouped on the first field of the tuples (the one of
Integer type). The GroupReduce function will thus receive groups of tuples with
the same value in the first field.

{% highlight scala %}
val input: DataSet[(Int, String, Long)] = // [...]
val grouped = input
  .groupBy(0,1)
  .reduce(/*do something*/)
{% endhighlight %}

The data set is grouped on the composite key consisting of the first and the
second field. Therefore, the GroupReduce function will receive groups
with the same value for both fields.

A note on nested Tuples: If you have a DataSet with a nested tuple, such as:

{% highlight scala %}
val ds: DataSet[((Int, Float), String, Long)]
{% endhighlight %}

Specifying `groupBy(0)` will cause the system to use the full `Tuple2` as a key (with the Int and
Float being the key). If you want to "navigate" into the nested `Tuple2`, you have to use field expression keys which are explained below.

</div>
</div>

### Define keys using Field Expressions
{:.no_toc}

Starting from release 0.7-incubating, you can use String-based field expressions to reference nested fields and define keys for grouping, sorting, joining, or coGrouping. In addition, field expressions can be used to define [semantic function annotations](#semantic-annotations).

Field expressions make it very easy to select fields in (nested) composite types such as [Tuple](#tuples-and-case-classes) and [POJO](#pojos) types.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

In the example below, we have a `WC` POJO with two fields "word" and "count". To group by the field `word`, we just pass its name to the `groupBy()` function.
{% highlight java %}
// some ordinary POJO (Plain old Java Object)
public class WC {
  public String word; 
  public int count;
}
DataSet<WC> words = // [...]
DataSet<WC> wordCounts = words.groupBy("word").reduce(/*do something*/);
{% endhighlight %}

**Field Expression Syntax**:

- Select POJO fields by their field name. For example `"user"` refers to the "user" field of a POJO type.

- Select Tuple fields by their field name or 0-offset field index. For example `"f0"` and `"5"` refer to the first and sixth field of a Java Tuple type, respectively.

- You can select nested fields in POJOs and Tuples. For example `"user.zip"` refers to the "zip" field of a POJO which is stored in the "user" field of a POJO type. Arbitrary nesting and mixing of POJOs and Tuples is supported such as `"f1.user.zip"` or `"user.f3.1.zip"`.

- You can select the full type using the `"*"` wildcard expressions. This does also work for types which are not Tuple or POJO types.

**Field Expression Example**:

{% highlight java %}
public static class WC {
  public ComplexNestedClass complex; //nested POJO
  private int count;
  // getter / setter for private field (count)
  public int getCount() {
    return count;
  }
  public void setCount(int c) {
    this.count = c;
  }
}
public static class ComplexNestedClass {
  public Integer someNumber;
  public float someFloat;
  public Tuple3<Long, Long, String> word;
  public IntWritable hadoopCitizen;
}
{% endhighlight %}

These are valid field expressions for the example code above:

- `"count"`: The count field in the `WC` class.

- `"complex"`: Recursively selects all fields of the field complex of POJO type `ComplexNestedClass`.

- `"complex.word.f2"`: Selects the last field of the nested `Tuple3`.

- `"complex.hadoopCitizen"`: Selects the Hadoop `IntWritable` type.

</div>
<div data-lang="scala" markdown="1">

In the example below, we have a `WC` POJO with two fields "word" and "count". To group by the field `word`, we just pass its name to the `groupBy()` function.
{% highlight java %}
// some ordinary POJO (Plain old Java Object)
class WC(var word: String, var count: Int) {
  def this() { this("", 0L) }
}
val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy("word").reduce(/*do something*/)

// or, as a case class, which is less typing
case class WC(word: String, count: Int)
val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy("word").reduce(/*do something*/)
{% endhighlight %}

**Field Expression Syntax**:

- Select POJO fields by their field name. For example `"user"` refers to the "user" field of a POJO type.

- Select Tuple fields by their 1-offset field name or 0-offset field index. For example `"_1"` and `"5"` refer to the first and sixth field of a Scala Tuple type, respectively.

- You can select nested fields in POJOs and Tuples. For example `"user.zip"` refers to the "zip" field of a POJO which is stored in the "user" field of a POJO type. Arbitrary nesting and mixing of POJOs and Tuples is supported such as `"_2.user.zip"` or `"user._4.1.zip"`.

- You can select the full type using the `"_"` wildcard expressions. This does also work for types which are not Tuple or POJO types.

**Field Expression Example**:

{% highlight scala %}
class WC(var complex: ComplexNestedClass, var count: Int) {
  def this() { this(null, 0) }
}

class ComplexNestedClass(
    var someNumber: Int,
    someFloat: Float,
    word: (Long, Long, String),
    hadoopCitizen: IntWritable) {
  def this() { this(0, 0, (0, 0, ""), new IntWritable(0)) }
}
{% endhighlight %}

These are valid field expressions for the example code above:

- `"count"`: The count field in the `WC` class.

- `"complex"`: Recursively selects all fields of the field complex of POJO type `ComplexNestedClass`.

- `"complex.word._3"`: Selects the last field of the nested `Tuple3`.

- `"complex.hadoopCitizen"`: Selects the Hadoop `IntWritable` type.

</div>
</div>

### Define keys using Key Selector Functions
{:.no_toc}

An additional way to define keys are "key selector" functions. A key selector function 
takes a single dataset element as input and returns the key for the element. The key can be of any type and be derived from arbitrary computations. 

The following example shows a key selector function that simply returns the field of an object:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// some ordinary POJO
public class WC {public String word; public int count;}
DataSet<WC> words = // [...]
DataSet<WC> wordCounts = words
                         .groupBy(
                           new KeySelector<WC, String>() {
                             public String getKey(WC wc) { return wc.word; }
                           })
                         .reduce(/*do something*/);
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// some ordinary case class
case class WC(word: String, count: Int)
val words: DataSet[WC] = // [...]
val wordCounts = words
  .groupBy( _.word ).reduce(/*do something*/)
{% endhighlight %}
</div>
</div>


[Back to top](#top)


Passing Functions to Flink
--------------------------

Operations require user-defined functions. This section lists several ways for doing this.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

#### Implementing an interface

The most basic way is to implement one of the provided interfaces:

{% highlight java %}
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
});
data.map (new MyMapFunction());
{% endhighlight %}

#### Anonymous classes

You can pass a function as an anonmymous class:
{% highlight java %}
data.map(new MapFunction<String, Integer> () {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

#### Java 8 Lambdas

Flink also supports Java 8 Lambdas in the Java API. Please see the full [Java 8 Guide](java8.html).

{% highlight java %}
DataSet<String> data = // [...]
data.filter(s -> s.startsWith("http://"));
{% endhighlight %}

{% highlight java %}
DataSet<Integer> data = // [...]
data.reduce((i1,i2) -> i1 + i2);
{% endhighlight %}

#### Rich functions

All transformations that take as argument a user-defined function can
instead take as argument a *rich* function. For example, instead of

{% highlight java %}
class MyMapFunction implements MapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

you can write

{% highlight java %}
class MyMapFunction extends RichMapFunction<String, Integer> {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

and pass the function as usual to a `map` transformation:

{% highlight java %}
data.map(new MyMapFunction());
{% endhighlight %}

Rich functions can also be defined as an anonymous class:
{% highlight java %}
data.map (new RichMapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">


#### Lambda Functions

As already seen in previous examples all operations accept lambda functions for describing
the operation:
{% highlight scala %}
val data: DataSet[String] = // [...]
data.filter { _.startsWith("http://") }
{% endhighlight %}

{% highlight scala %}
val data: DataSet[Int] = // [...]
data.reduce { (i1,i2) => i1 + i2 }
// or
data.reduce { _ + _ }
{% endhighlight %}

#### Rich functions

All transformations that take as argument a lambda function can
instead take as argument a *rich* function. For example, instead of

{% highlight scala %}
data.map { x => x.toInt }
{% endhighlight %}

you can write

{% highlight scala %}
class MyMapFunction extends RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
})
{% endhighlight %}

and pass the function to a `map` transformation:

{% highlight scala %}
data.map(new MyMapFunction())
{% endhighlight %}

Rich functions can also be defined as an anonymous class:
{% highlight scala %}
data.map (new RichMapFunction[String, Int] {
  def map(in: String):Int = { in.toInt }
})
{% endhighlight %}
</div>

</div>

Rich functions provide, in addition to the user-defined function (map,
reduce, etc), four methods: `open`, `close`, `getRuntimeContext`, and
`setRuntimeContext`. These are useful for parameterizing the function
(see [Passing Parameters to Functions](#passing-parameters-to-functions)),
creating and finalizing local state, accessing broadcast variables (see
[Broadcast Variables](#broadcast-variables), and for accessing runtime
information such as accumulators and counters (see
[Accumulators and Counters](#accumulators--counters), and information
on iterations (see [Iterations](iterations.html)).

In particular for the `reduceGroup` transformation, using a rich
function is the only way to define an optional `combine` function. See
the
[transformations documentation](dataset_transformations.html)
for a complete example.

[Back to top](#top)


Data Types
----------

Flink places some restrictions on the type of elements that are used in DataSets and as results
of transformations. The reason for this is that the system analyzes the types to determine
efficient execution strategies.

There are six different categories of data types:

1. **Java Tuples** and **Scala Case Classes**
2. **Java POJOs**
3. **Primitive Types**
4. **Regular Classes**
5. **Values**
6. **Hadoop Writables**

#### Tuples and Case Classes

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Tuples are composite types that contain a fixed number of fields with various types. 
The Java API provides classes from `Tuple1` up to `Tuple25`. Every field of a tuple
can be an arbitrary Flink type including further tuples, resulting in nested tuples. Fields of a
tuple can be accessed directly using the field's name as `tuple.f4`, or using the generic getter method
`tuple.getField(int position)`. The field indicies start at 0. Note that this stands in contrast
to the Scala tuples, but it is more consistent with Java's general indexing.

{% highlight java %}
DataSet<Tuple2<String, Integer>> wordCounts = env.fromElements(
    new Tuple2<String, Integer>("hello", 1),
    new Tuple2<String, Integer>("world", 2));

wordCounts.map(new MapFunction<Tuple2<String, Integer>, Integer>() {
    @Override
    public String map(Tuple2<String, Integer> value) throws Exception {
        return value.f1;
    }
});
{% endhighlight %}

When grouping, sorting, or joining a data set of tuples, keys can be specified as field positions or field expressions. See the [key definition section](#specifying-keys) or [data transformation section](#transformations) for details.

{% highlight java %}
wordCounts
    .groupBy(0) // also valid .groupBy("f0")
    .reduce(new MyReduceFunction());
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

Scala case classes (and Scala tuples which are a special case of case classes), are composite types that contain a fixed number of fields with various types. Tuple fields are addressed by their 1-offset names such as `_1` for the first field. Case class fields are accessed by their name.

{% highlight scala %}
case class WordCount(word: String, count: Int)
val input = env.fromElements(
    WordCount("hello", 1),
    WordCount("world", 2)) // Case Class Data Set

input.groupBy("word").reduce(...) // group by field expression "word"

val input2 = env.fromElements(("hello", 1), ("world", 2)) // Tuple2 Data Set

input2.groupBy(0, 1).reduce(...) // group by field positions 0 and 1
{% endhighlight %}

When grouping, sorting, or joining a data set of tuples, keys can be specified as field positions or field expressions. See the [key definition section](#specifying-keys) or [data transformation section](#transformations) for details.

</div>
</div>

#### POJOs

Java and Scala classes are treated by Flink as a special POJO data type if they fulfill the following requirements:

- The class must be public.

- It must have a public constructor without arguments (default constructor).

- All fields are either public or must be accessible through getter and setter functions. For a field called `foo` the getter and setter methods must be named `getFoo()` and `setFoo()`.

- The type of a field must be supported by Flink. At the moment, Flink uses [Avro](http://avro.apache.org) to serialize arbitrary objects (such as `Date`).

Flink analyzes the structure of POJO types, i.e., it learns about the fields of a POJO. As a result POJO types are easier to use than general types. Moreover, they Flink can process POJOs more efficiently than general types.  

The following example shows a simple POJO with two public fields.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class WordWithCount {

    public String word;
    public int count;

    public WordCount() {}

    public WordCount(String word, int count) {
        this.word = word;
        this.count = count;
    }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class WordWithCount(var word: String, var count: Int) {
    def this() {
      this(null, -1)
    }
}
{% endhighlight %}
</div>
</div>

When grouping, sorting, or joining a data set of POJO types, keys can be specified with field expressions. See the [key definition section](#specifying-keys) or [data transformation section](#transformations) for details.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
wordCounts
    .groupBy("word")                    // group by field expression "word"
    .reduce(new MyReduceFunction());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
wordCounts groupBy { _.word } reduce(new MyReduceFunction())
{% endhighlight %}
</div>
</div>

#### Primitive Types

Flink supports all Java and Scala primitive types such as `Integer`, `String`, and `Double`. 

#### General Class Types

Flink supports most Java and Scala classes (API and custom). 
Restrictions apply to classes containing fields that cannot be serialized, like file pointers, I/O streams, or other native
resources. Classes that follow the Java Beans conventions work well in general.

All classes that are not identified as POJO types (see POJO requirements above) are handled by Flink as general class types. 
Flink treats these data types as black boxes and is not able to access their their content (i.e., for efficient sorting). General types are de/serialized using the serialization framework [Kryo](https://github.com/EsotericSoftware/kryo). 

When grouping, sorting, or joining a data set of generic types, keys must be specified with key selector functions. See the [key definition section](#specifying-keys) or [data transformation section](#transformations) for details.


#### Values

*Value* types describe their serialization and deserialization manually. Instead of going through a
general purpose serialization framework, they provide custom code for those operations by means of
implementing the `org.apache.flinktypes.Value` interface with the methods `read` and `write`. Using
a Value type is reasonable when general purpose serialization would be highly inefficient. An
example would be a data type that implements a sparse vector of elements as an array. Knowing that
the array is mostly zero, one can use a special encoding for the non-zero elements, while the
general purpose serialization would simply write all array elements.

The `org.apache.flinktypes.CopyableValue` interface supports manual internal cloning logic in a
similar way.

Flink comes with pre-defined Value types that correspond to basic data types. (`ByteValue`,
`ShortValue`, `IntValue`, `LongValue`, `FloatValue`, `DoubleValue`, `StringValue`, `CharValue`,
`BooleanValue`). These Value types act as mutable variants of the basic data types: Their value can
be altered, allowing programmers to reuse objects and take pressure off the garbage collector.


#### Hadoop Writables

You can use types that implement the `org.apache.hadoop.Writable` interface. The serialization logic
defined in the `write()`and `readFields()` methods will be used for serialization.


#### Type Erasure & Type Inferrence

*Note: This Section is only relevant for Java.*

The Java compiler throws away much of the generic type information after compilation. This is
known as *type erasure* in Java. It means that at runtime, an instance of an object does not know
its generic type any more. For example, instances of `DataSet<String>` and `DataSet<Long>` look the
same to the JVM.

Flink requires type information at the time when it prepares the program for execution (when the
main method of the program is called). The Flink Java API tries to reconstruct the type information
that was thrown away in various ways and store it explicitly in the data sets and operators. You can
retrieve the type via `DataSet.getType()`. The method returns an instance of `TypeInformation`,
which is Flink's internal way of representing types.

The type inference has its limits and needs the "cooperation" of the programmer in some cases.
Examples for that are methods that create data sets from collections, such as
`ExecutionEnvironment.fromCollection(),` where you can pass an argument that describes the type. But
also generic functions like `MapFunction<I, O>` may need extra type information.

The
{% gh_link /flink-java/src/main/java/org/apache/flink/api/java/typeutils/ResultTypeQueryable.java "ResultTypeQueryable" %}
interface can be implemented by input formats and functions to tell the API
explicitly about their return type. The *input types* that the functions are invoked with can
usually be inferred by the result types of the previous operations.

[Back to top](#top)


Data Sources
------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Data sources create the initial data sets, such as from files or from Java collections. The general
mechanism of creating data sets is abstracted behind an
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/io/InputFormat.java "InputFormat"%}.
Flink comes
with several built-in formats to create data sets from common file formats. Many of them have
shortcut methods on the *ExecutionEnvironment*.

File-based:

- `readTextFile(path)` / `TextInputFormat` - Reads files line wise and returns them as Strings.

- `readTextFileWithValue(path)` / `TextValueInputFormat` - Reads files line wise and returns them as
  StringValues. StringValues are mutable strings.

- `readCsvFile(path)` / `CsvInputFormat` - Parses files of comma (or another char) delimited fields.
  Returns a DataSet of tuples or POJOs. Supports the basic java types and their Value counterparts as field
  types.

- `readFileOfPrimitives(path, Class)` / `PrimitiveInputFormat` - Parses files of new-line (or another char sequence) delimited primitive data types such as `String` or `Integer`. 

Collection-based:

- `fromCollection(Collection)` - Creates a data set from the Java Java.util.Collection. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator, Class)` - Creates a data set from an iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(T ...)` - Creates a data set from the given sequence of objects. All objects must be
  of the same type.

- `fromParallelCollection(SplittableIterator, Class)` - Creates a data set from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the squence of numbers in the given interval, in
  parallel.

Generic:

- `readFile(inputFormat, path)` / `FileInputFormat` - Accepts a file input format.

- `createInput(inputFormat)` / `InputFormat` - Accepts a generic input format.

**Examples**

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// read text file from local files system
DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");

// read text file from a HDFS running at nnHost:nnPort
DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");

// read a CSV file with three fields
DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
	                       .types(Integer.class, String.class, Double.class);

// read a CSV file with five fields, taking only two of them
DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
                               .includeFields("10010")  // take the first and the fourth field
	                       .types(String.class, Double.class);

// read a CSV file with three fields into a POJO (Person.class) with corresponding fields
DataSet<Person>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
                         .pojoType(Person.class, "name", "age", "zipcode");                         

// create a set from some given elements
DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");

// generate a number sequence
DataSet<Long> numbers = env.generateSequence(1, 10000000);

// Read data from a relational database using the JDBC input format
DataSet<Tuple2<String, Integer> dbData = 
    env.createInput(
      // create and configure input format
      JDBCInputFormat.buildJDBCInputFormat()
                     .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                     .setDBUrl("jdbc:derby:memory:persons")
                     .setQuery("select name, age from persons")
                     .finish(),
      // specify type information for DataSet
      new TupleTypeInfo(Tuple2.class, STRING_TYPE_INFO, INT_TYPE_INFO)
    );

// Note: Flink's program compiler needs to infer the data types of the data items which are returned
// by an InputFormat. If this information cannot be automatically inferred, it is necessary to
// manually provide the type information as shown in the examples above.
{% endhighlight %}

#### Configuring CSV Parsing

Flink offers a number of configuration options for CSV parsing:

- `types(Class ... types)` specifies the types of the fields to parse. **It is mandatory to configure the types of the parsed fields.**
  In case of the type class Boolean.class, "True" (case-insensitive), "False" (case-insensitive), "1" and "0" are treated as booleans.

- `lineDelimiter(String del)` specifies the delimiter of individual records. The default line delimiter is the new-line character `'\n'`.

- `fieldDelimiter(String del)` specifies the delimiter that separates fields of a record. The default field delimiter is the comma character `','`.

- `includeFields(boolean ... flag)`, `includeFields(String mask)`, or `includeFields(long bitMask)` defines which fields to read from the input file (and which to ignore). By default the first *n* fields (as defined by the number of types in the `types()` call) are parsed.

- `parseQuotedStrings(char quoteChar)` enables quoted string parsing. Strings are parsed as quoted strings if the first character of the string field is the quote character (leading or tailing whitespaces are *not* trimmed). Field delimiters within quoted strings are ignored. Quoted string parsing fails if the last character of a quoted string field is not the quote character. If quoted string parsing is enabled and the first character of the field is *not* the quoting string, the string is parsed as unquoted string. By default, quoted string parsing is disabled.

- `ignoreComments(String commentPrefix)` specifies a comment prefix. All lines that start with the specified comment prefix are not parsed and ignored. By default, no lines are ignored.

- `ignoreInvalidLines()` enables lenient parsing, i.e., lines that cannot be correctly parsed are ignored. By default, lenient parsing is disabled and invalid lines raise an exception.

- `ignoreFirstLine()` configures the InputFormat to ignore the first line of the input file. By default no line is ignored.


#### Recursive Traversal of the Input Path Directory

For file-based inputs, when the input path is a directory, nested files are not enumerated by default. Instead, only the files inside the base directory are read, while nested files are ignored. Recursive enumeration of nested files can be enabled through the `recursive.file.enumeration` configuration parameter, like in the following example.

{% highlight java %}
// enable recursive enumeration of nested input files
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// create a configuration object
Configuration parameters = new Configuration();

// set the recursive enumeration parameter
parameters.setBoolean("recursive.file.enumeration", true);

// pass the configuration to the data source
DataSet<String> logs = env.readTextFile("file:///path/with.nested/files")
			  .withParameters(parameters);
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

Data sources create the initial data sets, such as from files or from Java collections. The general
mechanism of creating data sets is abstracted behind an
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/io/InputFormat.java "InputFormat"%}.
Flink comes
with several built-in formats to create data sets from common file formats. Many of them have
shortcut methods on the *ExecutionEnvironment*.

File-based:

- `readTextFile(path)` / `TextInputFormat` - Reads files line wise and returns them as Strings.

- `readTextFileWithValue(path)` / `TextValueInputFormat` - Reads files line wise and returns them as
  StringValues. StringValues are mutable strings.

- `readCsvFile(path)` / `CsvInputFormat` - Parses files of comma (or another char) delimited fields.
  Returns a DataSet of tuples, case class objects, or POJOs. Supports the basic java types and their Value counterparts as field
  types.

Collection-based:

- `fromCollection(Seq)` - Creates a data set from a Seq. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator)` - Creates a data set from an Iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(elements: _*)` - Creates a data set from the given sequence of objects. All objects
  must be of the same type.

- `fromParallelCollection(SplittableIterator)` - Creates a data set from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the squence of numbers in the given interval, in
  parallel.

Generic:

- `readFile(inputFormat, path)` / `FileInputFormat` - Accepts a file input format.

- `createInput(inputFormat)` / `InputFormat` - Accepts a generic input format.

**Examples**

{% highlight scala %}
val env  = ExecutionEnvironment.getExecutionEnvironment

// read text file from local files system
val localLines = env.readTextFile("file:///path/to/my/textfile")

// read text file from a HDFS running at nnHost:nnPort
val hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile")

// read a CSV file with three fields
val csvInput = env.readCsvFile[(Int, String, Double)]("hdfs:///the/CSV/file")

// read a CSV file with five fields, taking only two of them
val csvInput = env.readCsvFile[(String, Double)](
  "hdfs:///the/CSV/file",
  includedFields = Array(0, 3)) // take the first and the fourth field

// CSV input can also be used with Case Classes
case class MyCaseClass(str: String, dbl: Double)
val csvInput = env.readCsvFile[MyCaseClass](
  "hdfs:///the/CSV/file",
  includedFields = Array(0, 3)) // take the first and the fourth field

// read a CSV file with three fields into a POJO (Person) with corresponding fields
val csvInput = env.readCsvFile[Person](
  "hdfs:///the/CSV/file",
  pojoFields = Array("name", "age", "zipcode"))

// create a set from some given elements
val values = env.fromElements("Foo", "bar", "foobar", "fubar")

// generate a number sequence
val numbers = env.generateSequence(1, 10000000);
{% endhighlight %}


#### Configuring CSV Parsing

Flink offers a number of configuration options for CSV parsing:

- `lineDelimiter: String` specifies the delimiter of individual records. The default line delimiter is the new-line character `'\n'`.

- `fieldDelimiter: String` specifies the delimiter that separates fields of a record. The default field delimiter is the comma character `','`.

- `includeFields: Array[Int]` defines which fields to read from the input file (and which to ignore). By default the first *n* fields (as defined by the number of types in the `types()` call) are parsed.

- `pojoFields: Array[String]` specifies the fields of a POJO that are mapped to CSV fields. Parsers for CSV fields are automatically initialized based on the type and order of the POJO fields.

- `parseQuotedStrings: Character` enables quoted string parsing. Strings are parsed as quoted strings if the first character of the string field is the quote character (leading or tailing whitespaces are *not* trimmed). Field delimiters within quoted strings are ignored. Quoted string parsing fails if the last character of a quoted string field is not the quote character. If quoted string parsing is enabled and the first character of the field is *not* the quoting string, the string is parsed as unquoted string. By default, quoted string parsing is disabled.

- `ignoreComments: String` specifies a comment prefix. All lines that start with the specified comment prefix are not parsed and ignored. By default, no lines are ignored.

- `lenient: Boolean` enables lenient parsing, i.e., lines that cannot be correctly parsed are ignored. By default, lenient parsing is disabled and invalid lines raise an exception.

- `ignoreFirstLine: Boolean` configures the InputFormat to ignore the first line of the input file. By default no line is ignored.

#### Recursive Traversal of the Input Path Directory

For file-based inputs, when the input path is a directory, nested files are not enumerated by default. Instead, only the files inside the base directory are read, while nested files are ignored. Recursive enumeration of nested files can be enabled through the `recursive.file.enumeration` configuration parameter, like in the following example.

{% highlight scala %}
// enable recursive enumeration of nested input files
val env  = ExecutionEnvironment.getExecutionEnvironment

// create a configuration object
val parameters = new Configuration

// set the recursive enumeration parameter
parameters.setBoolean("recursive.file.enumeration", true)

// pass the configuration to the data source
env.readTextFile("file:///path/with.nested/files").withParameters(parameters)
{% endhighlight %}

</div>
</div>
[Back to top](#top)

Data Sinks
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Data sinks consume DataSets and are used to store or return them. Data sink operations are described
using an
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/io/OutputFormat.java "OutputFormat" %}.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataSet:

- `writeAsText()` / `TextOuputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.
- `writeAsFormattedText()` / `TextOutputFormat` - Write elements line-wise as Strings. The Strings
  are obtained by calling a user-defined *format()* method for each element.
- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.
- `print()` / `printToErr()` / `print(String msg)` / `printToErr(String msg)` - Prints the *toString()* value
of each element on the standard out / strandard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.
- `write()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.
- `output()`/ `OutputFormat` - Most generic output method, for data sinks that are not file based
  (such as storing the result in a database).

A DataSet can be input to multiple operations. Programs can write or print a data set and at the
same time run additional transformations on them.

**Examples**

Standard data sink methods:

{% highlight java %}
// text data 
DataSet<String> textData = // [...]

// write DataSet to a file on the local file system
textData.writeAsText("file:///my/result/on/localFS");

// write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS");

// write DataSet to a file and overwrite the file if it exists
textData.writeAsText("file:///my/result/on/localFS", WriteMode.OVERWRITE);

// tuples as lines with pipe as the separator "a|b|c"
DataSet<Tuple3<String, Integer, Double>> values = // [...]
values.writeAsCsv("file:///path/to/the/result/file", "\n", "|");

// this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
values.writeAsText("file:///path/to/the/result/file");

// this wites values as strings using a user-defined TextFormatter object
values.writeAsFormattedText("file:///path/to/the/result/file",
    new TextFormatter<Tuple2<Integer, Integer>>() {
        public String format (Tuple2<Integer, Integer> value) {
            return value.f1 + " - " + value.f0;
        }
    });
{% endhighlight %}

Using a custom output format:

{% highlight java %}
DataSet<Tuple3<String, Integer, Double>> myResult = [...]

// write Tuple DataSet to a relational database
myResult.output(
    // build and configure OutputFormat
    JDBCOutputFormat.buildJDBCOutputFormat()
                    .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                    .setDBUrl("jdbc:derby:memory:persons")
                    .setQuery("insert into persons (name, age, height) values (?,?,?)")
                    .finish()
    );
{% endhighlight %}

#### Locally Sorted Output

The output of a data sink can be locally sorted on specified fields in specified orders using [tuple field positions](#define-keys-for-tuples) or [field expressions](#define-keys-using-field-expressions). This works for every output format.

The following examples show how to use this feature:

{% highlight java %}

DataSet<Tuple3<Integer, String, Double>> tData = // [...]
DataSet<Tuple2<BookPojo, Double>> pData = // [...]
DataSet<String> sData = // [...]

// sort output on String field in ascending order
tData.print().sortLocalOutput(1, Order.ASCENDING);

// sort output on Double field in descending and Integer field in ascending order
tData.print().sortLocalOutput(2, Order.DESCENDING).sortLocalOutput(0, Order.ASCENDING);

// sort output on the "author" field of nested BookPojo in descending order
pData.writeAsText(...).sortLocalOutput("f0.author", Order.DESCENDING);

// sort output on the full tuple in ascending order
tData.writeAsCsv(...).sortLocalOutput("*", Order.ASCENDING);

// sort atomic type (String) output in descending order
sData.writeAsText(...).sortLocalOutput("*", Order.DESCENDING);

{% endhighlight %}

Globally sorted output is not supported yet.

</div>
<div data-lang="scala" markdown="1">
Data sinks consume DataSets and are used to store or return them. Data sink operations are described
using an
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/io/OutputFormat.java "OutputFormat" %}.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataSet:

- `writeAsText()` / `TextOuputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.
- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.
- `print()` / `printToErr()` - Prints the *toString()* value of each element on the
  standard out / strandard error stream.
- `write()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.
- `output()`/ `OutputFormat` - Most generic output method, for data sinks that are not file based
  (such as storing the result in a database).

A DataSet can be input to multiple operations. Programs can write or print a data set and at the
same time run additional transformations on them.

**Examples**

Standard data sink methods:

{% highlight scala %}
// text data 
val textData: DataSet[String] = // [...]

// write DataSet to a file on the local file system
textData.writeAsText("file:///my/result/on/localFS")

// write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS")

// write DataSet to a file and overwrite the file if it exists
textData.writeAsText("file:///my/result/on/localFS", WriteMode.OVERWRITE)

// tuples as lines with pipe as the separator "a|b|c"
val values: DataSet[(String, Int, Double)] = // [...]
values.writeAsCsv("file:///path/to/the/result/file", "\n", "|")

// this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
values.writeAsText("file:///path/to/the/result/file");

// this wites values as strings using a user-defined formatting
values map { tuple => tuple._1 + " - " + tuple._2 }
  .writeAsText("file:///path/to/the/result/file")
{% endhighlight %}


#### Locally Sorted Output

The output of a data sink can be locally sorted on specified fields in specified orders using [tuple field positions](#define-keys-for-tuples) or [field expressions](#define-keys-using-field-expressions). This works for every output format.

The following examples show how to use this feature:

{% highlight scala %}

val tData: DataSet[(Int, String, Double)] = // [...]
val pData: DataSet[(BookPojo, Double)] = // [...]
val sData: DataSet[String] = // [...]

// sort output on String field in ascending order
tData.print.sortLocalOutput(1, Order.ASCENDING);

// sort output on Double field in descending and Int field in ascending order
tData.print.sortLocalOutput(2, Order.DESCENDING).sortLocalOutput(0, Order.ASCENDING);

// sort output on the "author" field of nested BookPojo in descending order
pData.writeAsText(...).sortLocalOutput("_1.author", Order.DESCENDING);

// sort output on the full tuple in ascending order
tData.writeAsCsv(...).sortLocalOutput("_", Order.ASCENDING);

// sort atomic type (String) output in descending order
sData.writeAsText(...).sortLocalOutput("_", Order.DESCENDING);

{% endhighlight %}

Globally sorted output is not supported yet.

</div>
</div>

[Back to top](#top)

Debugging
---------

Before running a data analysis program on a large data set in a distributed cluster, it is a good
idea to make sure that the implemented algorithm works as desired. Hence, implementing data analysis
programs is usually an incremental process of checking results, debugging, and improving.

Flink provides a few nice features to significantly ease the development process of data analysis
programs by supporting local debugging from within an IDE, injection of test data, and collection of
result data. This section give some hints how to ease the development of Flink programs.

### Local Execution Environment

A `LocalEnvironment` starts a Flink system within the same JVM process it was created in. If you
start the LocalEnvironement from an IDE, you can set breakpoint in your code and easily debug your
program.

A LocalEnvironment is created and used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

DataSet<String> lines = env.readTextFile(pathToTextFile);
// build your program

env.execute();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
val env = ExecutionEnvironment.createLocalEnvironment()

val lines = env.readTextFile(pathToTextFile)
// build your program

env.execute();
{% endhighlight %}
</div>
</div>

### Collection Data Sources and Sinks

Providing input for an analysis program and checking its output is cumbersome when done by creating
input files and reading output files. Flink features special data sources and sinks which are backed
by Java collections to ease testing. Once a program has been tested, the sources and sinks can be
easily replaced by sources and sinks that read from / write to external data stores such as HDFS.

Collection data sources can be used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

// Create a DataSet from a list of elements
DataSet<Integer> myInts = env.fromElements(1, 2, 3, 4, 5);

// Create a DataSet from any Java collection
List<Tuple2<String, Integer>> data = ...
DataSet<Tuple2<String, Integer>> myTuples = env.fromCollection(data);

// Create a DataSet from an Iterator
Iterator<Long> longIt = ...
DataSet<Long> myLongs = env.fromCollection(longIt, Long.class);
{% endhighlight %}

A collection data sink is specified as follows:

{% highlight java %}
DataSet<Tuple2<String, Integer>> myResult = ...

List<Tuple2<String, Integer>> outData = new ArrayList<Tuple2<String, Integer>>();
myResult.output(new LocalCollectionOutputFormat(outData));
{% endhighlight %}

**Note:** Currently, the collection data sink is restricted to local execution, as a debugging tool.

</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.createLocalEnvironment()

// Create a DataSet from a list of elements
val myInts = env.fromElements(1, 2, 3, 4, 5)

// Create a DataSet from any Collection
val data: Seq[(String, Int)] = ...
val myTuples = env.fromCollection(data)

// Create a DataSet from an Iterator
val longIt: Iterator[Long] = ...
val myLongs = env.fromCollection(longIt)
{% endhighlight %}
</div>
</div>

**Note:** Currently, the collection data source requires that data types and iterators implement
`Serializable`. Furthermore, collection data sources can not be executed in parallel (
parallelism = 1).

[Back to top](#top)

Iteration Operators
-------------------

Iterations implement loops in Flink programs. The iteration operators encapsulate a part of the
program and execute it repeatedly, feeding back the result of one iteration (the partial solution)
into the next iteration. There are two types of iterations in Flink: **BulkIteration** and
**DeltaIteration**.

This section provides quick examples on how to use both operators. Check out the [Introduction to
Iterations](iterations.html) page for a more detailed introduction.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

#### Bulk Iterations

To create a BulkIteration call the `iterate(int)` method of the DataSet the iteration should start
at. This will return an `IterativeDataSet`, which can be transformed with the regular operators. The
single argument to the iterate call specifies the maximum number of iterations.

To specify the end of an iteration call the `closeWith(DataSet)` method on the `IterativeDataSet` to
specify which transformation should be fed back to the next iteration. You can optionally specify a
termination criterion with `closeWith(DataSet, DataSet)`, which evaluates the second DataSet and
terminates the iteration, if this DataSet is empty. If no termination criterion is specified, the
iteration terminates after the given maximum number iterations.

The following example iteratively estimates the number Pi. The goal is to count the number of random
points, which fall into the unit circle. In each iteration, a random point is picked. If this point
lies inside the unit circle, we increment the count. Pi is then estimated as the resulting count
divided by the number of iterations multiplied by 4.

{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// Create initial IterativeDataSet
IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10000);

DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
    @Override
    public Integer map(Integer i) throws Exception {
        double x = Math.random();
        double y = Math.random();

        return i + ((x * x + y * y < 1) ? 1 : 0);
    }
});

// Iteratively transform the IterativeDataSet
DataSet<Integer> count = initial.closeWith(iteration);

count.map(new MapFunction<Integer, Double>() {
    @Override
    public Double map(Integer count) throws Exception {
        return count / (double) 10000 * 4;
    }
}).print();

env.execute("Iterative Pi Example");
{% endhighlight %}

You can also check out the
{% gh_link /flink-examples/flink-java-examples/src/main/java/org/apache/flink/examples/java/clustering/KMeans.java "K-Means example" %},
which uses a BulkIteration to cluster a set of unlabeled points.

#### Delta Iterations

Delta iterations exploit the fact that certain algorithms do not change every data point of the
solution in each iteration.

In addition to the partial solution that is fed back (called workset) in every iteration, delta
iterations maintain state across iterations (called solution set), which can be updated through
deltas. The result of the iterative computation is the state after the last iteration. Please refer
to the [Introduction to Iterations](iterations.html) for an overview of the basic principle of delta
iterations.

Defining a DeltaIteration is similar to defining a BulkIteration. For delta iterations, two data
sets form the input to each iteration (workset and solution set), and two data sets are produced as
the result (new workset, solution set delta) in each iteration.

To create a DeltaIteration call the `iterateDelta(DataSet, int, int)` (or `iterateDelta(DataSet,
int, int[])` respectively). This method is called on the initial solution set. The arguments are the
initial delta set, the maximum number of iterations and the key positions. The returned
`DeltaIteration` object gives you access to the DataSets representing the workset and solution set
via the methods `iteration.getWorkset()` and `iteration.getSolutionSet()`.

Below is an example for the syntax of a delta iteration

{% highlight java %}
// read the initial data sets
DataSet<Tuple2<Long, Double>> initialSolutionSet = // [...]

DataSet<Tuple2<Long, Double>> initialDeltaSet = // [...]

int maxIterations = 100;
int keyPosition = 0;

DeltaIteration<Tuple2<Long, Double>, Tuple2<Long, Double>> iteration = initialSolutionSet
    .iterateDelta(initialDeltaSet, maxIterations, keyPosition);

DataSet<Tuple2<Long, Double>> candidateUpdates = iteration.getWorkset()
    .groupBy(1)
    .reduceGroup(new ComputeCandidateChanges());

DataSet<Tuple2<Long, Double>> deltas = candidateUpdates
    .join(iteration.getSolutionSet())
    .where(0)
    .equalTo(0)
    .with(new CompareChangesToCurrent());

DataSet<Tuple2<Long, Double>> nextWorkset = deltas
    .filter(new FilterByThreshold());

iteration.closeWith(deltas, nextWorkset)
	.writeAsCsv(outputPath);
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">
#### Bulk Iterations

To create a BulkIteration call the `iterate(int)` method of the DataSet the iteration should start
at and also specify a step function. The step function gets the input DataSet for the current
iteration and must return a new DataSet. The parameter of the iterate call is the maximum number
of iterations after which to stop.

There is also the `iterateWithTermination(int)` function that accepts a step function that
returns two DataSets: The result of the iteration step and a termination criterion. The iterations
are stopped once the termination criterion DataSet is empty.

The following example iteratively estimates the number Pi. The goal is to count the number of random
points, which fall into the unit circle. In each iteration, a random point is picked. If this point
lies inside the unit circle, we increment the count. Pi is then estimated as the resulting count
divided by the number of iterations multiplied by 4.

{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()

// Create initial DataSet
val initial = env.fromElements(0)

val count = initial.iterate(10000) { iterationInput: DataSet[Int] =>
  val result = iterationInput.map { i => 
    val x = Math.random()
    val y = Math.random()
    i + (if (x * x + y * y < 1) 1 else 0)
  }
  result
}

val result = count map { c => c / 10000.0 * 4 }

result.print()

env.execute("Iterative Pi Example");
{% endhighlight %}

You can also check out the
{% gh_link /flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/clustering/KMeans.scala "K-Means example" %},
which uses a BulkIteration to cluster a set of unlabeled points.

#### Delta Iterations

Delta iterations exploit the fact that certain algorithms do not change every data point of the
solution in each iteration.

In addition to the partial solution that is fed back (called workset) in every iteration, delta
iterations maintain state across iterations (called solution set), which can be updated through
deltas. The result of the iterative computation is the state after the last iteration. Please refer
to the [Introduction to Iterations](iterations.html) for an overview of the basic principle of delta
iterations.

Defining a DeltaIteration is similar to defining a BulkIteration. For delta iterations, two data
sets form the input to each iteration (workset and solution set), and two data sets are produced as
the result (new workset, solution set delta) in each iteration.

To create a DeltaIteration call the `iterateDelta(initialWorkset, maxIterations, key)` on the
initial solution set. The step function takes two parameters: (solutionSet, workset), and must
return two values: (solutionSetDelta, newWorkset).

Below is an example for the syntax of a delta iteration

{% highlight scala %}
// read the initial data sets
val initialSolutionSet: DataSet[(Long, Double)] = // [...]

val initialWorkset: DataSet[(Long, Double)] = // [...]

val maxIterations = 100
val keyPosition = 0

val result = initialSolutionSet.iterateDelta(initialWorkset, maxIterations, Array(keyPosition)) {
  (solution, workset) =>
    val candidateUpdates = workset.groupBy(1).reduceGroup(new ComputeCandidateChanges())
    val deltas = candidateUpdates.join(solution).where(0).equalTo(0)(new CompareChangesToCurrent())

    val nextWorkset = deltas.filter(new FilterByThreshold())

    (deltas, nextWorkset)
}

result.writeAsCsv(outputPath)

env.execute()
{% endhighlight %}

</div>
</div>

[Back to top](#top)


Semantic Annotations
-----------

Semantic annotations can be used to give Flink hints about the behavior of a function. 
They tell the system which fields of a function's input the function reads and evaluates and
which fields it unmodified forwards from its input to its output. 
Semantic annotations are a powerful means to speed up execution, because they
allow the system to reason about reusing sort orders or partitions across multiple operations. Using
semantic annotations may eventually save the program from unnecessary data shuffling or unnecessary
sorts and significantly improve the performance of a program.

**Note:** The use of semantic annotations is optional. However, it is absolutely crucial to 
be conservative when providing semantic annotations! 
Incorrect semantic annotations will cause Flink to make incorrect assumptions about your program and 
might eventually lead to incorrect results. 
If the behavior of an operator is not clearly predictable, no annotation should be provided.
Please read the documentation carefully.

The following semantic annotations are currently supported.

#### Forwarded Fields Annotation

Forwarded fields information declares input fields which are unmodified forwarded by a function to the same position or to another position in the output. 
This information is used by the optimizer to infer whether a data property such as sorting or 
partitioning is preserved by a function.
For functions that operate on groups of input elements such as `GroupReduce`, `GroupCombine`, `CoGroup`, and `MapPartition`, all fields that are defined as forwarded fields must always be jointly forwarded from the same input element. The forwarded fields of each element that is emitted by a group-wise function may originate from a different element of the function's input group.

Field forward information is specified using [field expressions](#define-keys-using-field-expressions).
Fields that are forwarded to the same position in the output can be specified by their position. 
The specified position must be valid for the input and output data type and have the same type.
For example the String `"f2"` declares that the third field of a Java input tuple is always equal to the third field in the output tuple. 

Fields which are unmodified forwarded to another position in the output are declared by specifying the
source field in the input and the target field in the output as field expressions.
The String `"f0->f2"` denotes that the first field of the Java input tuple is
unchanged copied to the third field of the Java output tuple. The wildcard expression `*` can be used to refer to a whole input or output type, i.e., `"f0->*"` denotes that the output of a function is always equal to the first field of its Java input tuple.

Multiple forwarded fields can be declared in a single String by separating them with semicolons as `"f0; f2->f1; f3->f2"` or in separate Strings `"f0", "f2->f1", "f3->f2"`. When specifying forwarded fields it is not required that all forwarded fields are declared, but all declarations must be correct.

Forwarded field information can be declared by attaching Java annotations on function class definitions or 
by passing them as operator arguments after invoking a function on a DataSet as shown below.

##### Function Class Annotations

* `@ForwardedFields` for single input functions such as Map and Reduce.
* `@ForwardedFieldsFirst` for the first input of a functions with two inputs such as Join and CoGroup.
* `@ForwardedFieldsSecond` for the second input of a functions with two inputs such as Join and CoGroup.

##### Operator Arguments

* `data.map(myMapFnc).withForwardedFields()` for single input function such as Map and Reduce.
* `data1.join(data2).where().equalTo().with(myJoinFnc).withForwardFieldsFirst()` for the first input of a function with two inputs such as Join and CoGroup. 
* `data1.join(data2).where().equalTo().with(myJoinFnc).withForwardFieldsSecond()` for the second input of a function with two inputs such as Join and CoGroup.

Please note that it is not possible to overwrite field forward information which was specified as a class annotation by operator arguments. 

##### Example

The following example shows how to declare forwarded field information using a function class annotation:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@ForwardedFields("f0->f2")
public class MyMap implements 
              MapFunction<Tuple2<Integer, Integer>, Tuple3<String, Integer, Integer>> {
  @Override
  public Tuple3<String, Integer, Integer> map(Tuple2<Integer, Integer> val) {
    return new Tuple3<String, Integer, Integer>("foo", val.f1 / 2, val.f0);
  }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
@ForwardedFields("_1->_3")
class MyMap extends MapFunction[(Int, Int), (String, Int, Int)]{
   def map(value: (Int, Int)): (String, Int, Int) = {
    return ("foo", value._2 / 2, value._1)
  }
}
{% endhighlight %}

</div>
</div>

#### Non-Forwarded Fields

Non-forwarded fields information declares all fields which are not preserved on the same position in a function's output. 
The values of all other fields are considered to be preserved at the same position in the output. 
Hence, non-forwarded fields information is inverse to forwarded fields information. 
Non-forwarded field information for group-wise operators such as `GroupReduce`, `GroupCombine`, `CoGroup`, and `MapPartition` must fulfill the same requirements as for forwarded field information.

**IMPORTANT**: The specification of non-forwarded fields information is optional. However if used, 
**ALL!** non-forwarded fields must be specified, because all other fields are considered to be forwarded in place. It is safe to declare a forwarded field as non-forwarded.

Non-forwarded fields are specified as a list of [field expressions](#define-keys-using-field-expressions). The list can be either given as a single String with field expressions separated by semicolons or as multiple Strings. 
For example both `"f1; f3"` and `"f1", "f3"` declare that the second and fourth field of a Java tuple 
are not preserved in place and all other fields are preserved in place. 
Non-forwarded field information can only be specified for functions which have identical input and output types.

Non-forwarded field information is specified as function class annotations using the following annotations:

* `@NonForwardedFields` for single input functions such as Map and Reduce.
* `@NonForwardedFieldsFirst` for the first input of a function with two inputs such as Join and CoGroup.
* `@NonForwardedFieldsSecond` for the second input of a function with two inputs such as Join and CoGroup.

##### Example

The following example shows how to declare non-forwarded field information:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@NonForwardedFields("f1") // second field is not forwarded
public class MyMap implements 
              MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
  @Override
  public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> val) {
    return new Tuple2<Integer, Integer>(val.f0, val.f1 / 2);
  }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
@NonForwardedFields("_2") // second field is not forwarded
class MyMap extends MapFunction[(Int, Int), (Int, Int)]{
  def map(value: (Int, Int)): (Int, Int) = {
    return (value._1, value._2 / 2)
  }
}
{% endhighlight %}

</div>
</div>

#### Read Fields

Read fields information declares all fields that are accessed and evaluated by a function, i.e.,
all fields that are used by the function to compute its result.
For example, fields which are evaluated in conditional statements or used for computations must be marked as read when specifying read fields information.
Fields which are only unmodified forwarded to the output without evaluating their values or fields which are not accessed at all are not considered to be read.

**IMPORTANT**: The specification of read fields information is optional. However if used, 
**ALL!** read fields must be specified. It is safe to declare a non-read field as read.

Read fields are specified as a list of [field expressions](#define-keys-using-field-expressions). The list can be either given as a single String with field expressions separated by semicolons or as multiple Strings. 
For example both `"f1; f3"` and `"f1", "f3"` declare that the second and fourth field of a Java tuple are read and evaluated by the function.

Read field information is specified as function class annotations using the following annotations:

* `@ReadFields` for single input functions such as Map and Reduce.
* `@ReadFieldsFirst` for the first input of a function with two inputs such as Join and CoGroup.
* `@ReadFieldsSecond` for the second input of a function with two inputs such as Join and CoGroup.

##### Example

The following example shows how to declare read field information:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
@ReadFields("f0; f3") // f0 and f3 are read and evaluated by the function. 
public class MyMap implements 
              MapFunction<Tuple4<Integer, Integer, Integer, Integer>, 
                          Tuple2<Integer, Integer>> {
  @Override
  public Tuple2<Integer, Integer> map(Tuple4<Integer, Integer, Integer, Integer> val) {
    if(val.f0 == 42) {
      return new Tuple2<Integer, Integer>(val.f0, val.f1);
    } else {
      return new Tuple2<Integer, Integer>(val.f3+10, val.f1);
    }
  }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
@ReadFields("_1; _4") // _1 and _4 are read and evaluated by the function.
class MyMap extends MapFunction[(Int, Int, Int, Int), (Int, Int)]{
   def map(value: (Int, Int, Int, Int)): (Int, Int) = {
    if (value._1 == 42) {
      return (value._1, value._2)
    } else {
      return (value._4 + 10, value._2)
    }
  }
}
{% endhighlight %}

</div>
</div>

[Back to top](#top)


Broadcast Variables
-------------------

Broadcast variables allow you to make a data set available to all parallel instances of an
operation, in addition to the regular input of the operation. This is useful for auxiliary data
sets, or data-dependent parameterization. The data set will then be accessible at the operator as a
Collection.

- **Broadcast**: broadcast sets are registered by name via `withBroadcastSet(DataSet, String)`, and
- **Access**: accessible via `getRuntimeContext().getBroadcastVariable(String)` at the target operator.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// 1. The DataSet to be broadcasted
DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

DataSet<String> data = env.fromElements("a", "b");

data.map(new RichMapFunction<String, String>() {
    @Override
    public void open(Configuration parameters) throws Exception {
      // 3. Access the broadcasted DataSet as a Collection
      Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
    }


    @Override
    public String map(String value) throws Exception {
        ...
    }
}).withBroadcastSet(toBroadcast, "broadcastSetName"); // 2. Broadcast the DataSet
{% endhighlight %}

Make sure that the names (`broadcastSetName` in the previous example) match when registering and
accessing broadcasted data sets. For a complete example program, have a look at
{% gh_link /flink-examples/flink-java-examples/src/main/java/org/apache/flink/examples/java/clustering/KMeans.java#L96 "K-Means Algorithm" %}.
</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// 1. The DataSet to be broadcasted
val toBroadcast = env.fromElements(1, 2, 3)

val data = env.fromElements("a", "b")

data.map(new RichMapFunction[String, String]() {
    var broadcastSet: Traversable[String] = null

    override def open(config: Configuration): Unit = {
      // 3. Access the broadcasted DataSet as a Collection
      broadcastSet = getRuntimeContext().getBroadcastVariable[String]("broadcastSetName").asScala
    }

    def map(in: String): String = {
        ...
    }
}).withBroadcastSet(toBroadcast, "broadcastSetName") // 2. Broadcast the DataSet
{% endhighlight %}

Make sure that the names (`broadcastSetName` in the previous example) match when registering and
accessing broadcasted data sets. For a complete example program, have a look at
{% gh_link /flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/clustering/KMeans.scala#L96 "KMeans Algorithm" %}.
</div>
</div>

**Note**: As the content of broadcast variables is kept in-memory on each node, it should not become
too large. For simpler things like scalar values you can simply make parameters part of the closure
of a function, or use the `withParameters(...)` method to pass in a configuration.

[Back to top](#top)

Passing Parameters to Functions
-------------------

Parameters can be passed to functions using either the constructor or the `withParameters(Configuration)` method. The parameters are serialized as part of the function object and shipped to all parallel task instances.

Check also the [best practices guide on how to pass command line arguments to functions](best_practices.html#parsing-command-line-arguments-and-passing-them-around-in-your-flink-application).

#### Via Constructor

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Integer> toFilter = env.fromElements(1, 2, 3);

toFilter.filter(new MyFilter(2));

private static class MyFilter implements FilterFunction<Integer> {

  private final int limit;

  public MyFilter(int limit) {
    this.limit = limit;
  }

  @Override
  public boolean filter(Integer value) throws Exception {
    return value > limit;
  }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val toFilter = env.fromElements(1, 2, 3)

toFilter.filter(new MyFilter(2))

class MyFilter(limit: Int) extends FilterFunction[Int] {
  override def filter(value: Int): Boolean = {
    value > limit
  }
}
{% endhighlight %}
</div>
</div>

#### Via `withParameters(Configuration)`

This method takes a Configuration object as an argument, which will be passed to the [rich function](#rich-functions)'s `open()`
method. The Configuration object is a Map from String keys to different value types.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataSet<Integer> toFilter = env.fromElements(1, 2, 3);

Configuration config = new Configuration();
config.setInteger("limit", 2);

toFilter.filter(new RichFilterFunction<Integer>() {
    private int limit;

    @Override
    public void open(Configuration parameters) throws Exception {
      limit = parameters.getInteger("limit", 0);
    }

    @Override
    public boolean filter(Integer value) throws Exception {
      return value > limit;
    }
}).withParameters(config);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val toFilter = env.fromElements(1, 2, 3)

val c = new Configuration()
c.setInteger("limit", 2)

toFilter.filter(new RichFilterFunction[Int]() {
    var limit = 0

    override def open(config: Configuration): Unit = {
      limit = config.getInteger("limit", 0)
    }

    def filter(in: Int): Boolean = {
        in > limit
    }
}).withParameters(c)
{% endhighlight %}
</div>
</div>

#### Globally via the `ExecutionConfig`

Flink also allows to pass custom configuration values to the `ExecutionConfig` interface of the environment. Since the execution config is accessible in all (rich) user functions, the custom configuration will be available globally in all functions.


**Setting a custom global configuration**

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

Configuration conf = new Configuration();
conf.setString("mykey","myvalue");
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setGlobalJobParameters(conf);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
val conf = new Configuration()
conf.setString("mykey", "myvalue")
env.getConfig.setGlobalJobParameters(conf)
{% endhighlight %}
</div>
</div>

Please note that you can also pass a custom class extending the `ExecutionConfig.GlobalJobParameters` class as the global job parameters to the execution config. The interface allows to implement the `Map<String, String> toMap()` method which will in turn show the values from the configuration in the web frontend.


**Accessing values from the global configuration**

Objects in the global job parameters are accessible in many places in the system. All user functions implementing a `Rich*Function` interface have access through the runtime context.

{% highlight java %}
public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

    private String mykey;
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
      Configuration globConf = (Configuration) globalParams;
      mykey = globConf.getString("mykey", null);
    }
    // ... more here ...
{% endhighlight %}

[Back to top](#top)

Program Packaging & Distributed Execution
-----------------------------------------

As described in the [program skeleton](#program-skeleton) section, Flink programs can be executed on
clusters by using the `RemoteEnvironment`. Alternatively, programs can be packaged into JAR Files
(Java Archives) for execution. Packaging the program is a prerequisite to executing them through the
[command line interface](cli.html) or the [web interface](web_client.html).

#### Packaging Programs

To support execution from a packaged JAR file via the command line or web interface, a program must
use the environment obtained by `ExecutionEnvironment.getExecutionEnvironment()`. This environment
will act as the cluster's environment when the JAR is submitted to the command line or web
interface. If the Flink program is invoked differently than through these interfaces, the
environment will act like a local environment.

To package the program, simply export all involved classes as a JAR file. The JAR file's manifest
must point to the class that contains the program's *entry point* (the class with the public
`main` method). The simplest way to do this is by putting the *main-class* entry into the
manifest (such as `main-class: org.apache.flinkexample.MyProgram`). The *main-class* attribute is
the same one that is used by the Java Virtual Machine to find the main method when executing a JAR
files through the command `java -jar pathToTheJarFile`. Most IDEs offer to include that attribute
automatically when exporting JAR files.


#### Packaging Programs through Plans

Additionally, we support packaging programs as *Plans*. Instead of defining a progam in the main
method and calling
`execute()` on the environment, plan packaging returns the *Program Plan*, which is a description of
the program's data flow. To do that, the program must implement the
`org.apache.flink.api.common.Program` interface, defining the `getPlan(String...)` method. The
strings passed to that method are the command line arguments. The program's plan can be created from
the environment via the `ExecutionEnvironment#createProgramPlan()` method. When packaging the
program's plan, the JAR manifest must point to the class implementing the
`org.apache.flinkapi.common.Program` interface, instead of the class with the main method.


#### Summary

The overall procedure to invoke a packaged program is as follows:

1. The JAR's manifest is searched for a *main-class* or *program-class* attribute. If both
attributes are found, the *program-class* attribute takes precedence over the *main-class*
attribute. Both the command line and the web interface support a parameter to pass the entry point
class name manually for cases where the JAR manifest contains neither attribute.

2. If the entry point class implements the `org.apache.flinkapi.common.Program`, then the system
calls the `getPlan(String...)` method to obtain the program plan to execute. The
`getPlan(String...)` method was the only possible way of defining a program in the *Record API*
(see [0.4 docs](http://stratosphere.eu/docs/0.4/)) and is also supported in the new Java API.

3. If the entry point class does not implement the `org.apache.flinkapi.common.Program` interface,
the system will invoke the main method of the class.

[Back to top](#top)

Accumulators & Counters
---------------------------

Accumulators are simple constructs with an **add operation** and a **final accumulated result**,
which is available after the job ended.

The most straightforward accumulator is a **counter**: You can increment it using the
```Accumulator.add(V value)``` method. At the end of the job Flink will sum up (merge) all partial
results and send the result to the client. Accumulators are useful during debugging or if you
quickly want to find out more about your data.

Flink currently has the following **built-in accumulators**. Each of them implements the
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java "Accumulator" %}
interface.

- {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/IntCounter.java "__IntCounter__" %},
  {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/LongCounter.java "__LongCounter__" %}
  and {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/DoubleCounter.java "__DoubleCounter__" %}: 
  See below for an example using a counter.
- {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Histogram.java "__Histogram__" %}:
  A histogram implementation for a discrete number of bins. Internally it is just a map from Integer
  to Integer. You can use this to compute distributions of values, e.g. the distribution of
  words-per-line for a word count program.

__How to use accumulators:__

First you have to create an accumulator object (here a counter) in the operator function where you
want to use it. Operator function here refers to the (anonymous inner) class implementing the user
defined code for an operator.

{% highlight java %}
private IntCounter numLines = new IntCounter();
{% endhighlight %}

Second you have to register the accumulator object, typically in the ```open()``` method of the
operator function. Here you also define the name.

{% highlight java %}
getRuntimeContext().addAccumulator("num-lines", this.numLines);
{% endhighlight %}

You can now use the accumulator anywhere in the operator function, including in the ```open()``` and
```close()``` methods.

{% highlight java %}
this.numLines.add(1);
{% endhighlight %}

The overall result will be stored in the ```JobExecutionResult``` object which is returned when
running a job using the Java API (currently this only works if the execution waits for the
completion of the job).

{% highlight java %}
myJobExecutionResult.getAccumulatorResult("num-lines")
{% endhighlight %}

All accumulators share a single namespace per job. Thus you can use the same accumulator in
different operator functions of your job. Flink will internally merge all accumulators with the same
name.

A note on accumulators and iterations: Currently the result of accumulators is only available after
the overall job ended. We plan to also make the result of the previous iteration available in the
next iteration. You can use
{% gh_link /flink-java/src/main/java/org/apache/flink/api/java/operators/IterativeDataSet.java#L98 "Aggregators" %}
to compute per-iteration statistics and base the termination of iterations on such statistics.

__Custom accumulators:__

To implement your own accumulator you simply have to write your implementation of the Accumulator
interface. Feel free to create a pull request if you think your custom accumulator should be shipped
with Flink.

You have the choice to implement either
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/Accumulator.java "Accumulator" %}
or {% gh_link /flink-core/src/main/java/org/apache/flink/api/common/accumulators/SimpleAccumulator.java "SimpleAccumulator" %}.

```Accumulator<V,R>``` is most flexible: It defines a type ```V``` for the value to add, and a
result type ```R``` for the final result. E.g. for a histogram, ```V``` is a number and ```R``` i
 a histogram. ```SimpleAccumulator``` is for the cases where both types are the same, e.g. for counters.

[Back to top](#top)

Parallel Execution
------------------

This section describes how the parallel execution of programs can be configured in Flink. A Flink
program consists of multiple tasks (operators, data sources, and sinks). A task is split into
several parallel instances for execution and each parallel instance processes a subset of the task's
input data. The number of parallel instances of a task is called its *parallelism*.


The parallelism of a task can be specified in Flink on different levels.

### Operator Level

The parallelism of an individual operator, data source, or data sink can be defined by calling its
`setParallelism()` method.  For example, the parallelism of the `Sum` operator in the
[WordCount](examples.html#word-count) example program can be set to `5` as follows :


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<String> text = [...]
DataSet<Tuple2<String, Integer>> wordCounts = text
    .flatMap(new LineSplitter())
    .groupBy(0)
    .sum(1).setParallelism(5);
wordCounts.print();

env.execute("Word Count Example");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .groupBy(0)
    .sum(1).setParallelism(5)
wordCounts.print()

env.execute("Word Count Example")
{% endhighlight %}
</div>
</div>

### Execution Environment Level

Flink programs are executed in the context of an [execution environment](#program-skeleton). An
execution environment defines a default parallelism for all operators, data sources, and data sinks
it executes. Execution environment parallelism can be overwritten by explicitly configuring the
parallelism of an operator.

The default parallelism of an execution environment can be specified by calling the
`setParallelism()` method. To execute all operators, data sources, and data sinks of the
[WordCount](#example-program) example program with a parallelism of `3`, set the default parallelism of the
execution environment as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(3);

DataSet<String> text = [...]
DataSet<Tuple2<String, Integer>> wordCounts = [...]
wordCounts.print();

env.execute("Word Count Example");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment
env.setParallelism(3)

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .groupBy(0)
    .sum(1)
wordCounts.print()

env.execute("Word Count Example")
{% endhighlight %}
</div>
</div>

### Client Level

The parallelism can be set at the Client when submitting jobs to Flink. The
Client can either be a Java or a Scala program. One example of such a Client is
Flink's Command-line Interface (CLI).

For the CLI client, the parallelism parameter can be specified with `-p`. For
exampple:

    ./bin/flink run -p 10 ../examples/*WordCount-java*.jar


In a Java/Scala program, the parallelism is set as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

try {
    PackagedProgram program = new PackagedProgram(file, args);
    InetSocketAddress jobManagerAddress = RemoteExecutor.getInetFromHostport("localhost:6123");
    Configuration config = new Configuration();

    Client client = new Client(jobManagerAddress, config, program.getUserCodeClassLoader());

    // set the parallelism to 10 here
    client.run(program, 10, true);

} catch (ProgramInvocationException e) {
    e.printStackTrace();
}

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
try {
    PackagedProgram program = new PackagedProgram(file, args)
    InetSocketAddress jobManagerAddress = RemoteExecutor.getInetFromHostport("localhost:6123")
    Configuration config = new Configuration()

    Client client = new Client(jobManagerAddress, new Configuration(), program.getUserCodeClassLoader())

    // set the parallelism to 10 here
    client.run(program, 10, true)

} catch {
    case e: Exception => e.printStackTrace
}
{% endhighlight %}
</div>
</div>


### System Level

A system-wide default parallelism for all execution environments can be defined by setting the
`parallelism.default` property in `./conf/flink-conf.yaml`. See the
[Configuration]({{ site.baseurl }}/setup/config.html) documentation for details.

[Back to top](#top)

Execution Plans
---------------

Depending on various parameters such as data size or number of machines in the cluster, Flink's
optimizer automatically chooses an execution strategy for your program. In many cases, it can be
useful to know how exactly Flink will execute your program.

__Plan Visualization Tool__

Flink comes packaged with a visualization tool for execution plans. The HTML document containing
the visualizer is located under ```tools/planVisualizer.html```. It takes a JSON representation of
the job execution plan and visualizes it as a graph with complete annotations of execution
strategies.

The following code shows how to print the execution plan JSON from your program:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

...

System.out.println(env.getExecutionPlan());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

...

println(env.getExecutionPlan())
{% endhighlight %}
</div>
</div>


To visualize the execution plan, do the following:

1. **Open** ```planVisualizer.html``` with your web browser,
2. **Paste** the JSON string into the text field, and
3. **Press** the draw button.

After these steps, a detailed execution plan will be visualized.

<img alt="A flink job execution graph." src="fig/plan_visualizer.png" width="80%">


__Web Interface__

Flink offers a web interface for submitting and executing jobs. If you choose to use this interface to submit your packaged program, you have the option to also see the plan visualization.

The script to start the webinterface is located under ```bin/start-webclient.sh```. After starting the webclient (per default on **port 8080**), your program can be uploaded and will be added to the list of available programs on the left side of the interface.

You are able to specify program arguments in the textbox at the bottom of the page. Checking the plan visualization checkbox shows the execution plan before executing the actual program.

[Back to top](#top)
