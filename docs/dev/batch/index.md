---
title: "Flink DataSet API Programming Guide"
nav-id: batch
nav-title: Batch (DataSet API)
nav-parent_id: dev
nav-pos: 30
nav-show_overview: true
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

DataSet programs in Flink are regular programs that implement transformations on data sets
(e.g., filtering, mapping, joining, grouping). The data sets are initially created from certain
sources (e.g., by reading files, or from local collections). Results are returned via sinks, which may for
example write the data to (distributed) files, or to standard output (for example the command line
terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs.
The execution can happen in a local JVM, or on clusters of many machines.

Please see [basic concepts]({{ site.baseurl }}/dev/api_concepts.html) for an introduction
to the basic concepts of the Flink API.

In order to create your own Flink DataSet program, we encourage you to start with the
[anatomy of a Flink Program]({{ site.baseurl }}/dev/api_concepts.html#anatomy-of-a-flink-program)
and gradually add your own
[transformations](#dataset-transformations). The remaining sections act as references for additional
operations and advanced features.

* This will be replaced by the TOC
{:toc}

Example Program
---------------

The following program is a complete, working example of WordCount. You can copy &amp; paste the code
to run it locally. You only have to include the correct Flink's library into your project
(see Section [Linking with Flink]({{ site.baseurl }}/dev/projectsetup/dependencies.html)) and specify the imports. Then you are ready
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
  }
}
{% endhighlight %}
</div>

</div>

{% top %}

DataSet Transformations
-----------------------

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
        <p>Transforms a parallel partition in a single function call. The function gets the partition
        as an <code>Iterable</code> stream and can produce an arbitrary number of result values. The number of
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
        into one. Reduce may be applied on a full data set or on a grouped data set.</p>
{% highlight java %}
data.reduce(new ReduceFunction<Integer> {
  public Integer reduce(Integer a, Integer b) { return a + b; }
});
{% endhighlight %}
        <p>If the reduce was applied to a grouped data set then you can specify the way that the
        runtime executes the combine phase of the reduce by supplying a <code>CombineHint</code> to
        <code>setCombineHint</code>. The hash-based strategy should be faster in most cases,
        especially if the number of different keys is small compared to the number of input
        elements (eg. 1/10).</p>
      </td>
    </tr>

    <tr>
      <td><strong>ReduceGroup</strong></td>
      <td>
        <p>Combines a group of elements into one or more elements. ReduceGroup may be applied on a
        full data set or on a grouped data set.</p>
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

    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Returns the distinct elements of a data set. It removes the duplicate entries
        from the input DataSet, with respect to all fields of the elements, or a subset of fields.</p>
{% highlight java %}
data.distinct();
{% endhighlight %}
        <p>Distinct is implemented using a reduce function. You can specify the way that the
        runtime executes the combine phase of the reduce by supplying a <code>CombineHint</code> to
        <code>setCombineHint</code>. The hash-based strategy should be faster in most cases,
        especially if the number of different keys is small compared to the number of input
        elements (eg. 1/10).</p>
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        Joins two data sets by creating all pairs of elements that are equal on their keys.
        Optionally uses a JoinFunction to turn the pair of elements into a single element, or a
        FlatJoinFunction to turn the pair of elements into arbitrarily many (including none)
        elements. See the <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys section</a> to learn how to define join keys.
{% highlight java %}
result = input1.join(input2)
               .where(0)       // key of the first input (tuple field 0)
               .equalTo(1);    // key of the second input (tuple field 1)
{% endhighlight %}
        You can specify the way that the runtime executes the join via <i>Join Hints</i>. The hints
        describe whether the join happens through partitioning or broadcasting, and whether it uses
        a sort-based or a hash-based algorithm. Please refer to the
        <a href="dataset_transformations.html#join-algorithm-hints">Transformations Guide</a> for
        a list of possible hints and an example.<br>
        If no hint is specified, the system will try to make an estimate of the input sizes and
        pick the best strategy according to those estimates.
{% highlight java %}
// This executes a join by broadcasting the first data set
// using a hash table for the broadcast data
result = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST)
               .where(0).equalTo(1);
{% endhighlight %}
        Note that the join transformation works only for equi-joins. Other join types need to be expressed using OuterJoin or CoGroup.
      </td>
    </tr>

    <tr>
      <td><strong>OuterJoin</strong></td>
      <td>
        Performs a left, right, or full outer join on two data sets. Outer joins are similar to regular (inner) joins and create all pairs of elements that are equal on their keys. In addition, records of the "outer" side (left, right, or both in case of full) are preserved if no matching key is found in the other side. Matching pairs of elements (or one element and a <code>null</code> value for the other input) are given to a JoinFunction to turn the pair of elements into a single element, or to a FlatJoinFunction to turn the pair of elements into arbitrarily many (including none)         elements. See the <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys section</a> to learn how to define join keys.
{% highlight java %}
input1.leftOuterJoin(input2) // rightOuterJoin or fullOuterJoin for right or full outer joins
      .where(0)              // key of the first input (tuple field 0)
      .equalTo(1)            // key of the second input (tuple field 1)
      .with(new JoinFunction<String, String, String>() {
          public String join(String v1, String v2) {
             // NOTE:
             // - v2 might be null for leftOuterJoin
             // - v1 might be null for rightOuterJoin
             // - v1 OR v2 might be null for fullOuterJoin
          }
      });
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>CoGroup</strong></td>
      <td>
        <p>The two-dimensional variant of the reduce operation. Groups each input on one or more
        fields and then joins the groups. The transformation function is called per pair of groups.
        See the <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys section</a> to learn how to define coGroup keys.</p>
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
      <p>Note: Cross is potentially a <b>very</b> compute-intensive operation which can challenge even large compute clusters! It is advised to hint the system with the DataSet sizes by using <i>crossWithTiny()</i> and <i>crossWithHuge()</i>.</p>
      </td>
    </tr>
    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Produces the union of two data sets.</p>
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
        <p>Evenly rebalances the parallel partitions of a data set to eliminate data skew. Only Map-like transformations may follow a rebalance transformation.</p>
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
        <p>Hash-partitions a data set on a given key. Keys can be specified as position keys, expression keys, and key selector functions.</p>
{% highlight java %}
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionByHash(0)
                            .mapPartition(new PartitionMapper());
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Range-Partition</strong></td>
      <td>
        <p>Range-partitions a data set on a given key. Keys can be specified as position keys, expression keys, and key selector functions.</p>
{% highlight java %}
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionByRange(0)
                            .mapPartition(new PartitionMapper());
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Custom Partitioning</strong></td>
      <td>
        <p>Assigns records based on a key to a specific partition using a custom Partitioner function. 
          The key can be specified as position key, expression key, and key selector function.
          <br/>
          <i>Note</i>: This method only works with a single field key.</p>
{% highlight java %}
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionCustom(partitioner, key)
                            .mapPartition(new PartitionMapper());
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
    <tr>
      <td><strong>MinBy / MaxBy</strong></td>
      <td>
        <p>Selects a tuple from a group of tuples whose values of one or more fields are minimum (maximum). The fields which are used for comparison must be valid key fields, i.e., comparable. If multiple tuples have minimum (maximum) field values, an arbitrary tuple of these tuples is returned. MinBy (MaxBy) may be applied on a full data set or a grouped data set.</p>
{% highlight java %}
DataSet<Tuple3<Integer, Double, String>> in = // [...]
// a DataSet with a single tuple with minimum values for the Integer and String fields.
DataSet<Tuple3<Integer, Double, String>> out = in.minBy(0, 2);
// a DataSet with one tuple for each group with the minimum value for the Double field.
DataSet<Tuple3<Integer, Double, String>> out2 = in.groupBy(2)
                                                  .minBy(1);
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
val output: DataSet[(Int, String, Double)] = input.aggregate(SUM, 0).aggregate(MIN, 2)
{% endhighlight %}
  <p>You can also use short-hand syntax for minimum, maximum, and sum aggregations.</p>
{% highlight scala %}
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Double)] = input.sum(0).min(2)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Distinct</strong></td>
      <td>
        <p>Returns the distinct elements of a data set. It removes the duplicate entries
        from the input DataSet, with respect to all fields of the elements, or a subset of fields.</p>
      {% highlight scala %}
         data.distinct()
      {% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Join</strong></td>
      <td>
        Joins two data sets by creating all pairs of elements that are equal on their keys.
        Optionally uses a JoinFunction to turn the pair of elements into a single element, or a
        FlatJoinFunction to turn the pair of elements into arbitrarily many (including none)
        elements. See the <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys section</a> to learn how to define join keys.
{% highlight scala %}
// In this case tuple fields are used as keys. "0" is the join field on the first tuple
// "1" is the join field on the second tuple.
val result = input1.join(input2).where(0).equalTo(1)
{% endhighlight %}
        You can specify the way that the runtime executes the join via <i>Join Hints</i>. The hints
        describe whether the join happens through partitioning or broadcasting, and whether it uses
        a sort-based or a hash-based algorithm. Please refer to the
        <a href="dataset_transformations.html#join-algorithm-hints">Transformations Guide</a> for
        a list of possible hints and an example.<br />
        If no hint is specified, the system will try to make an estimate of the input sizes and
        pick the best strategy according to those estimates.
{% highlight scala %}
// This executes a join by broadcasting the first data set
// using a hash table for the broadcast data
val result = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST)
                   .where(0).equalTo(1)
{% endhighlight %}
          Note that the join transformation works only for equi-joins. Other join types need to be expressed using OuterJoin or CoGroup.
      </td>
    </tr>

    <tr>
      <td><strong>OuterJoin</strong></td>
      <td>
        Performs a left, right, or full outer join on two data sets. Outer joins are similar to regular (inner) joins and create all pairs of elements that are equal on their keys. In addition, records of the "outer" side (left, right, or both in case of full) are preserved if no matching key is found in the other side. Matching pairs of elements (or one element and a `null` value for the other input) are given to a JoinFunction to turn the pair of elements into a single element, or to a FlatJoinFunction to turn the pair of elements into arbitrarily many (including none)         elements. See the <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys section</a> to learn how to define join keys.
{% highlight scala %}
val joined = left.leftOuterJoin(right).where(0).equalTo(1) {
   (left, right) =>
     val a = if (left == null) "none" else left._1
     (a, right)
  }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>CoGroup</strong></td>
      <td>
        <p>The two-dimensional variant of the reduce operation. Groups each input on one or more
        fields and then joins the groups. The transformation function is called per pair of groups.
        See the <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys section</a> to learn how to define coGroup keys.</p>
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
        <p>Note: Cross is potentially a <b>very</b> compute-intensive operation which can challenge even large compute clusters! It is advised to hint the system with the DataSet sizes by using <i>crossWithTiny()</i> and <i>crossWithHuge()</i>.</p>
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
      <td><strong>Rebalance</strong></td>
      <td>
        <p>Evenly rebalances the parallel partitions of a data set to eliminate data skew. Only Map-like transformations may follow a rebalance transformation.</p>
{% highlight scala %}
val data1: DataSet[Int] = // [...]
val result: DataSet[(Int, String)] = data1.rebalance().map(...)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Hash-Partition</strong></td>
      <td>
        <p>Hash-partitions a data set on a given key. Keys can be specified as position keys, expression keys, and key selector functions.</p>
{% highlight scala %}
val in: DataSet[(Int, String)] = // [...]
val result = in.partitionByHash(0).mapPartition { ... }
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Range-Partition</strong></td>
      <td>
        <p>Range-partitions a data set on a given key. Keys can be specified as position keys, expression keys, and key selector functions.</p>
{% highlight scala %}
val in: DataSet[(Int, String)] = // [...]
val result = in.partitionByRange(0).mapPartition { ... }
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Custom Partitioning</strong></td>
      <td>
        <p>Assigns records based on a key to a specific partition using a custom Partitioner function. 
          The key can be specified as position key, expression key, and key selector function.
          <br/>
          <i>Note</i>: This method only works with a single field key.</p>
{% highlight scala %}
val in: DataSet[(Int, String)] = // [...]
val result = in
  .partitionCustom(partitioner, key).mapPartition { ... }
{% endhighlight %}
      </td>
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
      <td><strong>MinBy / MaxBy</strong></td>
      <td>
        <p>Selects a tuple from a group of tuples whose values of one or more fields are minimum (maximum). The fields which are used for comparison must be valid key fields, i.e., comparable. If multiple tuples have minimum (maximum) field values, an arbitrary tuple of these tuples is returned. MinBy (MaxBy) may be applied on a full data set or a grouped data set.</p>
{% highlight java %}
val in: DataSet[(Int, Double, String)] = // [...]
// a data set with a single tuple with minimum values for the Int and String fields.
val out: DataSet[(Int, Double, String)] = in.minBy(0, 2)
// a data set with one tuple for each group with the minimum value for the Double field.
val out2: DataSet[(Int, Double, String)] = in.groupBy(2)
                                             .minBy(1)
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

Extraction from tuples, case classes and collections via anonymous pattern matching, like the following:
{% highlight scala %}
val data: DataSet[(Int, String, Double)] = // [...]
data.map {
  case (id, name, temperature) => // [...]
}
{% endhighlight %}
is not supported by the API out-of-the-box. To use this feature, you should use a <a href="{{ site.baseurl }}/dev/scala_api_extensions.html">Scala API extension</a>.

</div>
</div>

The [parallelism]({{ site.baseurl }}/dev/parallel.html) of a transformation can be defined by `setParallelism(int)` while
`name(String)` assigns a custom name to a transformation which is helpful for debugging. The same is
possible for [Data Sources](#data-sources) and [Data Sinks](#data-sinks).

`withParameters(Configuration)` passes Configuration objects, which can be accessed from the `open()` method inside the user function.

{% top %}

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

- `readFileOfPrimitives(path, Class)` / `PrimitiveInputFormat` - Parses files of new-line (or another char sequence)
  delimited primitive data types such as `String` or `Integer`.

- `readFileOfPrimitives(path, delimiter, Class)` / `PrimitiveInputFormat` - Parses files of new-line (or another char sequence)
   delimited primitive data types such as `String` or `Integer` using the given delimiter.


Collection-based:

- `fromCollection(Collection)` - Creates a data set from a Java.util.Collection. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator, Class)` - Creates a data set from an iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(T ...)` - Creates a data set from the given sequence of objects. All objects must be
  of the same type.

- `fromParallelCollection(SplittableIterator, Class)` - Creates a data set from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the sequence of numbers in the given interval, in
  parallel.

Generic:

- `readFile(inputFormat, path)` / `FileInputFormat` - Accepts a file input format.

- `createInput(inputFormat)` / `InputFormat` - Accepts a generic input format.

**Examples**

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// read text file from local files system
DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");

// read text file from an HDFS running at nnHost:nnPort
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

// read a file from the specified path of type SequenceFileInputFormat
DataSet<Tuple2<IntWritable, Text>> tuples =
 env.createInput(HadoopInputs.readSequenceFile(IntWritable.class, Text.class, "hdfs://nnHost:nnPort/path/to/file"));

// creates a set from some given elements
DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");

// generate a number sequence
DataSet<Long> numbers = env.generateSequence(1, 10000000);

// Read data from a relational database using the JDBC input format
DataSet<Tuple2<String, Integer> dbData =
    env.createInput(
      JDBCInputFormat.buildJDBCInputFormat()
                     .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                     .setDBUrl("jdbc:derby:memory:persons")
                     .setQuery("select name, age from persons")
                     .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                     .finish()
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

- `parseQuotedStrings(char quoteChar)` enables quoted string parsing. Strings are parsed as quoted strings if the first character of the string field is the quote character (leading or tailing whitespaces are *not* trimmed). Field delimiters within quoted strings are ignored. Quoted string parsing fails if the last character of a quoted string field is not the quote character or if the quote character appears at some point which is not the start or the end of the quoted string field (unless the quote character is escaped using '\'). If quoted string parsing is enabled and the first character of the field is *not* the quoting string, the string is parsed as unquoted string. By default, quoted string parsing is disabled.

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

- `readFileOfPrimitives(path, delimiter)` / `PrimitiveInputFormat` - Parses files of new-line (or another char sequence)
  delimited primitive data types such as `String` or `Integer` using the given delimiter.

- `readSequenceFile(Key, Value, path)` / `SequenceFileInputFormat` - Creates a JobConf and reads file from the specified path with
   type SequenceFileInputFormat, Key class and Value class and returns them as Tuple2<Key, Value>.

Collection-based:

- `fromCollection(Iterable)` - Creates a data set from an Iterable. All elements
  returned by the Iterable must be of the same type.

- `fromCollection(Iterator)` - Creates a data set from an Iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(elements: _*)` - Creates a data set from the given sequence of objects. All objects
  must be of the same type.

- `fromParallelCollection(SplittableIterator)` - Creates a data set from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the sequence of numbers in the given interval, in
  parallel.

Generic:

- `readFile(inputFormat, path)` / `FileInputFormat` - Accepts a file input format.

- `createInput(inputFormat)` / `InputFormat` - Accepts a generic input format.

**Examples**

{% highlight scala %}
val env  = ExecutionEnvironment.getExecutionEnvironment

// read text file from local files system
val localLines = env.readTextFile("file:///path/to/my/textfile")

// read text file from an HDFS running at nnHost:nnPort
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
val numbers = env.generateSequence(1, 10000000)

// read a file from the specified path of type SequenceFileInputFormat
val tuples = env.createInput(HadoopInputs.readSequenceFile(classOf[IntWritable], classOf[Text],
 "hdfs://nnHost:nnPort/path/to/file"))

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

### Read Compressed Files

Flink currently supports transparent decompression of input files if these are marked with an appropriate file extension. In particular, this means that no further configuration of the input formats is necessary and any `FileInputFormat` support the compression, including custom input formats. Please notice that compressed files might not be read in parallel, thus impacting job scalability.

The following table lists the currently supported compression methods.

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Compression method</th>
      <th class="text-left">File extensions</th>
      <th class="text-left" style="width: 20%">Parallelizable</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>DEFLATE</strong></td>
      <td><code>.deflate</code></td>
      <td>no</td>
    </tr>
    <tr>
      <td><strong>GZip</strong></td>
      <td><code>.gz</code>, <code>.gzip</code></td>
      <td>no</td>
    </tr>
    <tr>
      <td><strong>Bzip2</strong></td>
      <td><code>.bz2</code></td>
      <td>no</td>
    </tr>
    <tr>
      <td><strong>XZ</strong></td>
      <td><code>.xz</code></td>
      <td>no</td>
    </tr>
  </tbody>
</table>


{% top %}

Data Sinks
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Data sinks consume DataSets and are used to store or return them. Data sink operations are described
using an
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/io/OutputFormat.java "OutputFormat" %}.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataSet:

- `writeAsText()` / `TextOutputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.
- `writeAsFormattedText()` / `TextOutputFormat` - Write elements line-wise as Strings. The Strings
  are obtained by calling a user-defined *format()* method for each element.
- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.
- `print()` / `printToErr()` / `print(String msg)` / `printToErr(String msg)` - Prints the *toString()* value
of each element on the standard out / standard error stream. Optionally, a prefix (msg) can be provided which is
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

// write DataSet to a file on an HDFS with a namenode running at nnHost:nnPort
textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS");

// write DataSet to a file and overwrite the file if it exists
textData.writeAsText("file:///my/result/on/localFS", WriteMode.OVERWRITE);

// tuples as lines with pipe as the separator "a|b|c"
DataSet<Tuple3<String, Integer, Double>> values = // [...]
values.writeAsCsv("file:///path/to/the/result/file", "\n", "|");

// this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
values.writeAsText("file:///path/to/the/result/file");

// this writes values as strings using a user-defined TextFormatter object
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

The output of a data sink can be locally sorted on specified fields in specified orders using [tuple field positions]({{ site.baseurl }}/dev/api_concepts.html#define-keys-for-tuples) or [field expressions]({{ site.baseurl }}/dev/api_concepts.html#define-keys-using-field-expressions). This works for every output format.

The following examples show how to use this feature:

{% highlight java %}

DataSet<Tuple3<Integer, String, Double>> tData = // [...]
DataSet<Tuple2<BookPojo, Double>> pData = // [...]
DataSet<String> sData = // [...]

// sort output on String field in ascending order
tData.sortPartition(1, Order.ASCENDING).print();

// sort output on Double field in descending and Integer field in ascending order
tData.sortPartition(2, Order.DESCENDING).sortPartition(0, Order.ASCENDING).print();

// sort output on the "author" field of nested BookPojo in descending order
pData.sortPartition("f0.author", Order.DESCENDING).writeAsText(...);

// sort output on the full tuple in ascending order
tData.sortPartition("*", Order.ASCENDING).writeAsCsv(...);

// sort atomic type (String) output in descending order
sData.sortPartition("*", Order.DESCENDING).writeAsText(...);

{% endhighlight %}

Globally sorted output is not supported yet.

</div>
<div data-lang="scala" markdown="1">
Data sinks consume DataSets and are used to store or return them. Data sink operations are described
using an
{% gh_link /flink-core/src/main/java/org/apache/flink/api/common/io/OutputFormat.java "OutputFormat" %}.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataSet:

- `writeAsText()` / `TextOutputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.
- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.
- `print()` / `printToErr()` - Prints the *toString()* value of each element on the
  standard out / standard error stream.
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

// write DataSet to a file on an HDFS with a namenode running at nnHost:nnPort
textData.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS")

// write DataSet to a file and overwrite the file if it exists
textData.writeAsText("file:///my/result/on/localFS", WriteMode.OVERWRITE)

// tuples as lines with pipe as the separator "a|b|c"
val values: DataSet[(String, Int, Double)] = // [...]
values.writeAsCsv("file:///path/to/the/result/file", "\n", "|")

// this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
values.writeAsText("file:///path/to/the/result/file")

// this writes values as strings using a user-defined formatting
values map { tuple => tuple._1 + " - " + tuple._2 }
  .writeAsText("file:///path/to/the/result/file")
{% endhighlight %}


#### Locally Sorted Output

The output of a data sink can be locally sorted on specified fields in specified orders using [tuple field positions]({{ site.baseurl }}/dev/api_concepts.html#define-keys-for-tuples) or [field expressions]({{ site.baseurl }}/dev/api_concepts.html#define-keys-using-field-expressions). This works for every output format.

The following examples show how to use this feature:

{% highlight scala %}

val tData: DataSet[(Int, String, Double)] = // [...]
val pData: DataSet[(BookPojo, Double)] = // [...]
val sData: DataSet[String] = // [...]

// sort output on String field in ascending order
tData.sortPartition(1, Order.ASCENDING).print()

// sort output on Double field in descending and Int field in ascending order
tData.sortPartition(2, Order.DESCENDING).sortPartition(0, Order.ASCENDING).print()

// sort output on the "author" field of nested BookPojo in descending order
pData.sortPartition("_1.author", Order.DESCENDING).writeAsText(...)

// sort output on the full tuple in ascending order
tData.sortPartition("_", Order.ASCENDING).writeAsCsv(...)

// sort atomic type (String) output in descending order
sData.sortPartition("_", Order.DESCENDING).writeAsText(...)

{% endhighlight %}

Globally sorted output is not supported yet.

</div>
</div>

{% top %}


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
{% gh_link /flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/clustering/KMeans.java "K-Means example" %},
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

env.execute("Iterative Pi Example")
{% endhighlight %}

You can also check out the
{% gh_link /flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala/clustering/KMeans.scala "K-Means example" %},
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

{% top %}

Operating on data objects in functions
--------------------------------------

Flink's runtime exchanges data with user functions in form of Java objects. Functions receive input objects from the runtime as method parameters and return output objects as result. Because these objects are accessed by user functions and runtime code, it is very important to understand and follow the rules about how the user code may access, i.e., read and modify, these objects.

User functions receive objects from Flink's runtime either as regular method parameters (like a `MapFunction`) or through an `Iterable` parameter (like a `GroupReduceFunction`). We refer to objects that the runtime passes to a user function as *input objects*. User functions can emit objects to the Flink runtime either as a method return value (like a `MapFunction`) or through a `Collector` (like a `FlatMapFunction`). We refer to objects which have been emitted by the user function to the runtime as *output objects*.

Flink's DataSet API features two modes that differ in how Flink's runtime creates or reuses input objects. This behavior affects the guarantees and constraints for how user functions may interact with input and output objects. The following sections define these rules and give coding guidelines to write safe user function code.

### Object-Reuse Disabled (DEFAULT)

By default, Flink operates in object-reuse disabled mode. This mode ensures that functions always receive new input objects within a function call. The object-reuse disabled mode gives better guarantees and is safer to use. However, it comes with a certain processing overhead and might cause higher Java garbage collection activity. The following table explains how user functions may access input and output objects in object-reuse disabled mode.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Guarantees and Restrictions</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Reading Input Objects</strong></td>
      <td>
        Within a method call it is guaranteed that the value of an input object does not change. This includes objects served by an Iterable. For example it is safe to collect input objects served by an Iterable in a List or Map. Note that objects may be modified after the method call is left. It is <strong>not safe</strong> to remember objects across function calls.
      </td>
   </tr>
   <tr>
      <td><strong>Modifying Input Objects</strong></td>
      <td>You may modify input objects.</td>
   </tr>
   <tr>
      <td><strong>Emitting Input Objects</strong></td>
      <td>
        You may emit input objects. The value of an input object may have changed after it was emitted. It is <strong>not safe</strong> to read an input object after it was emitted.
      </td>
   </tr>
   <tr>
      <td><strong>Reading Output Objects</strong></td>
      <td>
        An object that was given to a Collector or returned as method result might have changed its value. It is <strong>not safe</strong> to read an output object.
      </td>
   </tr>
   <tr>
      <td><strong>Modifying Output Objects</strong></td>
      <td>You may modify an object after it was emitted and emit it again.</td>
   </tr>
  </tbody>
</table>

**Coding guidelines for the object-reuse disabled (default) mode:**

- Do not remember and read input objects across method calls.
- Do not read objects after you emitted them.


### Object-Reuse Enabled

In object-reuse enabled mode, Flink's runtime minimizes the number of object instantiations. This can improve the performance and can reduce the Java garbage collection pressure. The object-reuse enabled mode is activated by calling `ExecutionConfig.enableObjectReuse()`. The following table explains how user functions may access input and output objects in object-reuse enabled mode.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Operation</th>
      <th class="text-center">Guarantees and Restrictions</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Reading input objects received as regular method parameters</strong></td>
      <td>
        Input objects received as regular method arguments are not modified within a function call. Objects may be modified after method call is left. It is <strong>not safe</strong> to remember objects across function calls.
      </td>
   </tr>
   <tr>
      <td><strong>Reading input objects received from an Iterable parameter</strong></td>
      <td>
        Input objects received from an Iterable are only valid until the next() method is called. An Iterable or Iterator may serve the same object instance multiple times. It is <strong>not safe</strong> to remember input objects received from an Iterable, e.g., by putting them in a List or Map.
      </td>
   </tr>
   <tr>
      <td><strong>Modifying Input Objects</strong></td>
      <td>You <strong>must not</strong> modify input objects, except for input objects of MapFunction, FlatMapFunction, MapPartitionFunction, GroupReduceFunction, GroupCombineFunction, CoGroupFunction, and InputFormat.next(reuse).</td>
   </tr>
   <tr>
      <td><strong>Emitting Input Objects</strong></td>
      <td>
        You <strong>must not</strong> emit input objects, except for input objects of MapFunction, FlatMapFunction, MapPartitionFunction, GroupReduceFunction, GroupCombineFunction, CoGroupFunction, and InputFormat.next(reuse).
      </td>
   </tr>
   <tr>
      <td><strong>Reading Output Objects</strong></td>
      <td>
        An object that was given to a Collector or returned as method result might have changed its value. It is <strong>not safe</strong> to read an output object.
      </td>
   </tr>
   <tr>
      <td><strong>Modifying Output Objects</strong></td>
      <td>You may modify an output object and emit it again.</td>
   </tr>
  </tbody>
</table>

**Coding guidelines for object-reuse enabled:**

- Do not remember input objects received from an `Iterable`.
- Do not remember and read input objects across method calls.
- Do not modify or emit input objects, except for input objects of `MapFunction`, `FlatMapFunction`, `MapPartitionFunction`, `GroupReduceFunction`, `GroupCombineFunction`, `CoGroupFunction`, and `InputFormat.next(reuse)`.
- To reduce object instantiations, you can always emit a dedicated output object which is repeatedly modified but never read.

{% top %}

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
start the LocalEnvironment from an IDE, you can set breakpoints in your code and easily debug your
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

env.execute()
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

{% top %}

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

Field forward information is specified using [field expressions]({{ site.baseurl }}/dev/api_concepts.html#define-keys-using-field-expressions).
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

Non-forwarded fields are specified as a list of [field expressions]({{ site.baseurl }}/dev/api_concepts.html#define-keys-using-field-expressions). The list can be either given as a single String with field expressions separated by semicolons or as multiple Strings.
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

Read fields are specified as a list of [field expressions]({{ site.baseurl }}/dev/api_concepts.html#define-keys-using-field-expressions). The list can be either given as a single String with field expressions separated by semicolons or as multiple Strings.
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

{% top %}


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
// 1. The DataSet to be broadcast
DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

DataSet<String> data = env.fromElements("a", "b");

data.map(new RichMapFunction<String, String>() {
    @Override
    public void open(Configuration parameters) throws Exception {
      // 3. Access the broadcast DataSet as a Collection
      Collection<Integer> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
    }


    @Override
    public String map(String value) throws Exception {
        ...
    }
}).withBroadcastSet(toBroadcast, "broadcastSetName"); // 2. Broadcast the DataSet
{% endhighlight %}

Make sure that the names (`broadcastSetName` in the previous example) match when registering and
accessing broadcast data sets. For a complete example program, have a look at
{% gh_link /flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/clustering/KMeans.java "K-Means Algorithm" %}.
</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// 1. The DataSet to be broadcast
val toBroadcast = env.fromElements(1, 2, 3)

val data = env.fromElements("a", "b")

data.map(new RichMapFunction[String, String]() {
    var broadcastSet: Traversable[String] = null

    override def open(config: Configuration): Unit = {
      // 3. Access the broadcast DataSet as a Collection
      broadcastSet = getRuntimeContext().getBroadcastVariable[String]("broadcastSetName").asScala
    }

    def map(in: String): String = {
        ...
    }
}).withBroadcastSet(toBroadcast, "broadcastSetName") // 2. Broadcast the DataSet
{% endhighlight %}

Make sure that the names (`broadcastSetName` in the previous example) match when registering and
accessing broadcast data sets. For a complete example program, have a look at
{% gh_link /flink-examples/flink-examples-batch/src/main/scala/org/apache/flink/examples/scala/clustering/KMeans.scala "KMeans Algorithm" %}.
</div>
</div>

**Note**: As the content of broadcast variables is kept in-memory on each node, it should not become
too large. For simpler things like scalar values you can simply make parameters part of the closure
of a function, or use the `withParameters(...)` method to pass in a configuration.

{% top %}

Distributed Cache
-------------------

Flink offers a distributed cache, similar to Apache Hadoop, to make files locally accessible to parallel instances of user functions. This functionality can be used to share files that contain static external data such as dictionaries or machine-learned regression models.

The cache works as follows. A program registers a file or directory of a [local or remote filesystem such as HDFS or S3]({{ site.baseurl }}/dev/batch/connectors.html#reading-from-file-systems) under a specific name in its `ExecutionEnvironment` as a cached file. When the program is executed, Flink automatically copies the file or directory to the local filesystem of all workers. A user function can look up the file or directory under the specified name and access it from the worker's local filesystem.

The distributed cache is used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Register the file or directory in the `ExecutionEnvironment`.

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// register a file from HDFS
env.registerCachedFile("hdfs:///path/to/your/file", "hdfsFile")

// register a local executable file (script, executable, ...)
env.registerCachedFile("file:///path/to/exec/file", "localExecFile", true)

// define your program and execute
...
DataSet<String> input = ...
DataSet<Integer> result = input.map(new MyMapper());
...
env.execute();
{% endhighlight %}

Access the cached file or directory in a user function (here a `MapFunction`). The function must extend a [RichFunction]({{ site.baseurl }}/dev/api_concepts.html#rich-functions) class because it needs access to the `RuntimeContext`.

{% highlight java %}

// extend a RichFunction to have access to the RuntimeContext
public final class MyMapper extends RichMapFunction<String, Integer> {

    @Override
    public void open(Configuration config) {

      // access cached file via RuntimeContext and DistributedCache
      File myFile = getRuntimeContext().getDistributedCache().getFile("hdfsFile");
      // read the file (or navigate the directory)
      ...
    }

    @Override
    public Integer map(String value) throws Exception {
      // use content of cached file
      ...
    }
}
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

Register the file or directory in the `ExecutionEnvironment`.

{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

// register a file from HDFS
env.registerCachedFile("hdfs:///path/to/your/file", "hdfsFile")

// register a local executable file (script, executable, ...)
env.registerCachedFile("file:///path/to/exec/file", "localExecFile", true)

// define your program and execute
...
val input: DataSet[String] = ...
val result: DataSet[Integer] = input.map(new MyMapper())
...
env.execute()
{% endhighlight %}

Access the cached file in a user function (here a `MapFunction`). The function must extend a [RichFunction]({{ site.baseurl }}/dev/api_concepts.html#rich-functions) class because it needs access to the `RuntimeContext`.

{% highlight scala %}

// extend a RichFunction to have access to the RuntimeContext
class MyMapper extends RichMapFunction[String, Int] {

  override def open(config: Configuration): Unit = {

    // access cached file via RuntimeContext and DistributedCache
    val myFile: File = getRuntimeContext.getDistributedCache.getFile("hdfsFile")
    // read the file (or navigate the directory)
    ...
  }

  override def map(value: String): Int = {
    // use content of cached file
    ...
  }
}
{% endhighlight %}

</div>
</div>

{% top %}

Passing Parameters to Functions
-------------------

Parameters can be passed to functions using either the constructor or the `withParameters(Configuration)` method. The parameters are serialized as part of the function object and shipped to all parallel task instances.

Check also the [best practices guide on how to pass command line arguments to functions]({{ site.baseurl }}/dev/best_practices.html#parsing-command-line-arguments-and-passing-them-around-in-your-flink-application).

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

This method takes a Configuration object as an argument, which will be passed to the [rich function]({{ site.baseurl }}/dev/api_concepts.html#rich-functions)'s `open()`
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

Objects in the global job parameters are accessible in many places in the system. All user functions implementing a `RichFunction` interface have access through the runtime context.

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

{% top %}
