---
title: 概览 
type: docs
weight: 1
aliases:
  - /zh/dev/batch/index.html
  - /zh/apis/programming_guide.html
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

# DataSet API 编程指南

DataSet programs in Flink are regular programs that implement transformations on data sets (e.g., filtering, mapping, joining, grouping). The data sets are initially created from certain sources (e.g., by reading files, or from local collections). Results are returned via sinks, which may for example write the data to (distributed) files, or to standard output (for example the command line terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs. The execution can happen in a local JVM, or on clusters of many machines.

Please refer to the DataStream API overview for an introduction to the basic concepts of the Flink API. That overview is for the DataStream API but the basic concepts of the two APIs are the same.

In order to create your own Flink DataSet program, we encourage you to start with the anatomy of a Flink Program and gradually add your own transformations. The remaining sections act as references for additional operations and advanced features.

{{< hint info >}}
Starting with Flink 1.12 the DataSet has been soft deprecated. We recommend that you use the DataStream API with `BATCH` execution mode. The linked section also outlines cases where it makes sense to use the DataSet API but those cases will become rarer as development progresses and the DataSet API will eventually be removed. Please also see FLIP-131 for background information on this decision. 
{{< /hint >}}

## Example Program

The following program is a complete, working example of WordCount.
You can copy & paste the code to run it locally.
You only have to include the correct Flink’s library into your project and specify the imports.
Then you are ready to go!

{{< tabs "basic-example" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    val counts = text
      .flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()
  }
}
```
{{< /tab >}}
{{< /tabs >}}

## DataSet Transformations

Data transformations transform one or more DataSets into a new DataSet.
Programs can combine multiple transformations into sophisticated assemblies.

#### Map

Takes one element and produces one element.

{{< tabs "mapfun" >}}
{{< tab "Java" >}}
```java
data.map(new MapFunction<String, Integer>() {
  public Integer map(String value) { return Integer.parseInt(value); }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.map { x => x.toInt }
```
{{< /tab >}}
{{< /tabs >}}

#### FlatMap

Takes one element and produces zero, one, or more elements. 

{{< tabs "flatmapfunc" >}}
{{< tab "Java" >}}
```java
data.flatMap(new FlatMapFunction<String, String>() {
  public void flatMap(String value, Collector<String> out) {
    for (String s : value.split(" ")) {
      out.collect(s);
    }
  }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.flatMap { str => str.split(" ") }
```
{{< /tab >}}
{{< /tabs >}}

#### MapPartition

Transforms a parallel partition in a single function call.
The function gets the partition as an Iterable stream and can produce an arbitrary number of result values.
The number of elements in each partition depends on the degree-of-parallelism and previous operations.

{{< tabs "mappartition" >}}
{{< tab "Java" >}}
```java
data.mapPartition(new MapPartitionFunction<String, Long>() {
  public void mapPartition(Iterable<String> values, Collector<Long> out) {
    long c = 0;
    for (String s : values) {
      c++;
    }
    out.collect(c);
  }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.mapPartition { in => in map { (_, 1) } }
```
{{< /tab >}}
{{< /tabs >}}

#### Filter

Evaluates a boolean function for each element and retains those for which the function returns true.
**IMPORTANT:** The system assumes that the function does not modify the element on which the predicate is applied. Violating this assumption can lead to incorrect results.

{{< tabs "filter" >}}
{{< tab "Java" >}}
```java
data.filter(new FilterFunction<Integer>() {
  public boolean filter(Integer value) { return value > 1000; }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.filter { _ > 1000 }
```
{{< /tab >}}
{{< /tabs >}}

#### Reduce

Combines a group of elements into a single element by repeatedly combining two elements into one.
Reduce may be applied on a full data set or on a grouped data set.

{{< tabs "reduce" >}}
{{< tab "Java" >}}
```java
data.reduce(new ReduceFunction<Integer> {
  public Integer reduce(Integer a, Integer b) { return a + b; }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.reduce { _ + _ }
```
{{< /tab >}}
{{< /tabs >}}

If the reduce was applied to a grouped data set then you can specify the way that the runtime executes the combine phase of the reduce by supplying a `CombineHint` to `setCombineHint`.
The hash-based strategy should be faster in most cases, especially if the number of different keys is small compared to the number of input elements (eg. 1/10).

#### ReduceGroup

Combines a group of elements into one or more elements.
ReduceGroup may be applied on a full data set, or on a grouped data set.

{{< tabs "reducegroup" >}}
{{< tab "Java" >}}
```java
data.reduceGroup(new GroupReduceFunction<Integer, Integer> {
  public void reduce(Iterable<Integer> values, Collector<Integer> out) {
    int prefixSum = 0;
    for (Integer i : values) {
      prefixSum += i;
      out.collect(prefixSum);
    }
  }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.reduceGroup { elements => elements.sum }
```
{{< /tab >}}
{{< /tabs >}}

#### Aggregate

Aggregates a group of values into a single value.
Aggregation functions can be thought of as built-in reduce functions.
Aggregate may be applied on a full data set, or on a grouped data set.

{{< tabs "aggregate" >}}
{{< tab "Java" >}}
```java
Dataset<Tuple3<Integer, String, Double>> input = // [...]
DataSet<Tuple3<Integer, String, Double>> output = input.aggregate(SUM, 0).and(MIN, 2);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Double)] = input.aggregate(SUM, 0).aggregate(MIN, 2)
```
{{< /tab >}}
{{< /tabs >}}

#### Distinct 

Returns the distinct elements of a data set.
It removes the duplicate entries from the input DataSet, with respect to all fields of the elements, or a subset of fields.

{{< tabs "distinct" >}}
{{< tab "Java" >}}
```java
data.distinct()
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.distinct()
```
{{< /tab >}}
{{< /tabs >}}

#### Join

Joins two data sets by creating all pairs of elements that are equal on their keys.
Optionally uses a JoinFunction to turn the pair of elements into a single element, or a FlatJoinFunction to turn the pair of elements into arbitrarily many (including none) elements.
See the keys section to learn how to define join keys. 

{{< tabs "join" >}}
{{< tab "Java" >}}
```java
result = input1.join(input2)
               .where(0)       // key of the first input (tuple field 0)
               .equalTo(1);    // key of the second input (tuple field 1)
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// In this case tuple fields are used as keys. "0" is the join field on the first tuple
// "1" is the join field on the second tuple.
val result = input1.join(input2).where(0).equalTo(1)
```
{{< /tab >}}
{{< /tabs >}}

You can specify the way that the runtime executes the join via Join Hints.
The hints describe whether the join happens through partitioning or broadcasting, and whether it uses a sort-based or a hash-based algorithm.
Please refer to the Transformations Guide for a list of possible hints and an example.
If no hint is specified, the system will try to make an estimate of the input sizes and pick the best strategy according to those estimates. 

{{< tabs "joinhint" >}}
{{< tab "Java" >}}
```java
// This executes a join by broadcasting the first data set
// using a hash table for the broadcast data
result = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST)
               .where(0).equalTo(1);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// This executes a join by broadcasting the first data set
// using a hash table for the broadcast data
val result = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST)
                   .where(0).equalTo(1)
```
{{< /tab >}}
{{< /tabs >}}

Note that the join transformation works only for equi-joins. Other join types need to be expressed using `OuterJoin` or `CoGroup`. 

#### OuterJoin

Performs a left, right, or full outer join on two data sets. Outer joins are similar to regular (inner) joins and create all pairs of elements that are equal on their keys.
In addition, records of the "outer" side (left, right, or both in case of full) are preserved if no matching key is found in the other side.
Matching pairs of elements (or one element and a null value for the other input) are given to a JoinFunction to turn the pair of elements into a single element, or to a FlatJoinFunction to turn the pair of elements into arbitrarily many (including none) elements.
See the keys section to learn how to define join keys. 

{{< tabs "outerjoin" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val joined = left.leftOuterJoin(right).where(0).equalTo(1) {
   (left, right) =>
     val a = if (left == null) "none" else left._1
     (a, right)
  }
```
{{< /tab >}}
{{< /tabs >}}

#### CoGroup
	
The two-dimensional variant of the reduce operation. Groups each input on one or more fields and then joins the groups.
The transformation function is called per pair of groups. See the keys section to learn how to define coGroup keys.

{{< tabs "cogroup" >}}
{{< tab "Java" >}}
```java
data1.coGroup(data2)
     .where(0)
     .equalTo(1)
     .with(new CoGroupFunction<String, String, String>() {
         public void coGroup(Iterable<String> in1, Iterable<String> in2, Collector<String> out) {
           out.collect(...);
         }
      });
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data1.coGroup(data2).where(0).equalTo(1)
```
{{< /tab >}}
{{< /tabs >}}

#### Cross

Builds the Cartesian product (cross product) of two inputs, creating all pairs of elements. Optionally uses a CrossFunction to turn the pair of elements into a single element

{{< tabs "cross" >}}
{{< tab "Java" >}}
```java
DataSet<Integer> data1 = // [...]
DataSet<String> data2 = // [...]
DataSet<Tuple2<Integer, String>> result = data1.cross(data2);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val data1: DataSet[Int] = // [...]
val data2: DataSet[String] = // [...]
val result: DataSet[(Int, String)] = data1.cross(data2)
```
{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
Cross is potentially a **very** compute-intensive operation which can challenge even large compute clusters! It is advised to hint the system with the `DataSet` sizes by using `crossWithTiny()` and `crossWithHuge()`.
{{< /hint >}}

#### Union

Produces the union of two data sets.

{{< tabs "union" >}}
{{< tab "Java" >}}
```java
data.union(data2)
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
data.union(data2)
```
{{< /tab >}}
{{< /tabs >}}

#### Rebalance

Evenly rebalances the parallel partitions of a data set to eliminate data skew.
Only Map-like transformations may follow a rebalance transformation.

{{< tabs "rebalance" >}}
{{< tab "Java" >}}
```java
DataSet<Int> data1 = // [...]
DataSet<Tuple2<Int, String>> result = data1.rebalance().map(...)
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val data1: DataSet[Int] = // [...]
val result: DataSet[(Int, String)] = data1.rebalance().map(...)
```
{{< /tab >}}
{{< /tabs >}}

#### Hash-Partition

Hash-partitions a data set on a given key. Keys can be specified as position keys, expression keys, and key selector functions.

{{< tabs "hashpartition" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionByHash(0)
                            .mapPartition(new PartitionMapper());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val in: DataSet[(Int, String)] = // [...]
val result = in.partitionByHash(0).mapPartition { ... }
```
{{< /tab >}}
{{< /tabs >}}

#### Range-Partition

Range-partitions a data set on a given key. Keys can be specified as position keys, expression keys, and key selector functions.

{{< tabs "rangepartition" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionByRange(0)
                            .mapPartition(new PartitionMapper());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val in: DataSet[(Int, String)] = // [...]
val result = in.partitionByRange(0).mapPartition { ... }
```
{{< /tab >}}
{{< /tabs >}}

#### Custom Partitioning

Assigns records based on a key to a specific partition using a custom Partitioner function.
The key can be specified as position key, expression key, and key selector function.
Note: This method only works with a single field key.

{{< tabs "custompartitioning" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.partitionCustom(partitioner, key)
                            .mapPartition(new PartitionMapper());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val in: DataSet[(Int, String)] = // [...]
val result = in
  .partitionCustom(partitioner, key).mapPartition { ... }
```
{{< /tab >}}
{{< /tabs >}}

#### Sort Partitioning

Locally sorts all partitions of a data set on a specified field in a specified order.
Fields can be specified as tuple positions or field expressions.
Sorting on multiple fields is done by chaining sortPartition() calls.

{{< tabs "sortpartitioning" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple2<String,Integer>> in = // [...]
DataSet<Integer> result = in.sortPartition(1, Order.ASCENDING)
                            .mapPartition(new PartitionMapper());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val in: DataSet[(Int, String)] = // [...]
val result = in.sortPartition(1, Order.ASCENDING).mapPartition { ... }
```
{{< /tab >}}
{{< /tabs >}}

#### First-N

Returns the first n (arbitrary) elements of a data set.
First-n can be applied on a regular data set, a grouped data set, or a grouped-sorted data set.
Grouping keys can be specified as key-selector functions or field position keys.

{{< tabs "firstn" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val in: DataSet[(Int, String)] = // [...]
// regular data set
val result1 = in.first(3)
// grouped data set
val result2 = in.groupBy(0).first(3)
// grouped-sorted data set
val result3 = in.groupBy(0).sortGroup(1, Order.ASCENDING).first(3)
```
{{< /tab >}}
{{< /tabs >}}

#### Project

Selects a subset of fields from tuples.

{{< tabs "project" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple3<Integer, Double, String>> in = // [...]
DataSet<Tuple2<String, Integer>> out = in.project(2,0);
```
{{< /tab >}}
{{< tab "Scala" >}}
This feature is not available in the Scala API
{{< /tab >}}
{{< /tabs >}}

#### MinBy / MaxBy

Selects a tuple from a group of tuples whose values of one or more fields are minimum (maximum).
The fields which are used for comparison must be valid key fields, i.e., comparable.
If multiple tuples have minimum (maximum) field values, an arbitrary tuple of these tuples is returned. MinBy (MaxBy) may be applied on a full data set or a grouped data set.

{{< tabs "minbymaxby" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple3<Integer, Double, String>> in = // [...]
// a DataSet with a single tuple with minimum values for the Integer and String fields.
DataSet<Tuple3<Integer, Double, String>> out = in.minBy(0, 2);
// a DataSet with one tuple for each group with the minimum value for the Double field.
DataSet<Tuple3<Integer, Double, String>> out2 = in.groupBy(2)
                                                  .minBy(1);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val in: DataSet[(Int, Double, String)] = // [...]
// a data set with a single tuple with minimum values for the Int and String fields.
val out: DataSet[(Int, Double, String)] = in.minBy(0, 2)
// a data set with one tuple for each group with the minimum value for the Double field.
val out2: DataSet[(Int, Double, String)] = in.groupBy(2)
                                             .minBy(1)
```
{{< /tab >}}
{{< /tabs >}}

## Specifying Keys

Some transformations (join, coGroup, groupBy) require that a key be defined on a collection of elements.
Other transformations (Reduce, GroupReduce, Aggregate) allow data being grouped on a key before they are applied.

A DataSet is grouped as

```java
DataSet<...> input = // [...]
DataSet<...> reduced = input
  .groupBy(/*define key here*/)
  .reduceGroup(/*do something*/);
```

The data model of Flink is not based on key-value pairs.
Therefore, you do not need to physically pack the data set types into keys and values.
Keys are “virtual”: they are defined as functions over the actual data to guide the grouping operator.

### Define keys for Tuples

The simplest case is grouping Tuples on one or more fields of the Tuple:

{{< tabs "keyfortuple" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple3<Integer,String,Long>> input = // [...]
UnsortedGrouping<Tuple3<Integer,String,Long>,Tuple> keyed = input.groupBy(0)
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataSet[(Int, String, Long)] = // [...]
val keyed = input.groupBy(0)
```
{{< /tab >}}
{{< /tabs >}}

Tuples are grouped on the first field (the one of Integer type).

{{< tabs "tuplemultigroup" >}}
{{< tab "Java" >}}
```java
DataSet<Tuple3<Integer,String,Long>> input = // [...]
UnsortedGrouping<Tuple3<Integer,String,Long>,Tuple> keyed = input.groupBy(0,1)
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataSet[(Int, String, Long)] = // [...]
val grouped = input.groupBy(0,1)
```
{{< /tab >}}
{{< /tabs >}}

Here, we group the tuples on a composite key consisting of the first and the second field.

A note on nested Tuples: If you have a DataSet with a nested tuple, such as:

```java
DataSet<Tuple3<Tuple2<Integer, Float>,String,Long>> ds;
```

Specifying `groupBy(0)` will cause the system to use the full `Tuple2` as a key (with the Integer and Float being the key).
If you want to “navigate” into the nested Tuple2, you have to use field expression keys which are explained below.

### Define keys using Field Expressions

You can use String-based field expressions to reference nested fields and define keys for grouping, sorting, joining, or coGrouping.
Field expressions make it very easy to select fields in (nested) composite types such as Tuple and POJO types.

In the example below, we have a WC `POJO` with two fields “word” and “count”.
To group by the field word, we just pass its name to the groupBy() function.

{{< tabs "pojokey" >}}
{{< tab "Java" >}}
```java
// some ordinary POJO (Plain old Java Object)
public class WC {
  public String word;
  public int count;
}
DataSet<WC> words = // [...]
DataSet<WC> wordCounts = words.groupBy("word")
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// some ordinary POJO (Plain old Java Object)
class WC(var word: String, var count: Int) {
  def this() { this("", 0L) }
}
val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy("word")

// or, as a case class, which is less typing
case class WC(word: String, count: Int)
val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy("word")
```
{{< /tab >}}
{{< /tabs >}}

#### Field Expression Syntax:

  * Select POJO fields by their field name. For example "user" refers to the “user” field of a POJO type.

  * Select Tuple fields by their 1-offset field name or 0-offset field index. For example "_1" and "5" refer to the first and sixth field of a Scala Tuple type, respectively.

  * You can select nested fields in POJOs and Tuples. For example "user.zip" refers to the “zip” field of a POJO which is stored in the “user” field of a POJO type. Arbitrary nesting and mixing of POJOs and Tuples is supported such as "_2.user.zip" or "user._4.1.zip".

  * You can select the full type using the "_" wildcard expressions. This does also work for types which are not Tuple or POJO types.

#### Field Expression Example:

{{< tabs "flatmapfunc" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

These are valid field expressions for the example code above:

  * "count": The count field in the WC class.

  * "complex": Recursively selects all fields of the field complex of POJO type ComplexNestedClass.

  * "complex.word.f2": Selects the last field of the nested Tuple3.

  * "complex.hadoopCitizen": Selects the Hadoop IntWritable type.

### Define keys using Key Selector Functions

An additional way to define keys are “key selector” functions.
A key selector function takes a single element as input and returns the key for the element. The key can be of any type and be derived from deterministic computations.

The following example shows a key selector function that simply returns the field of an object:

{{< tabs "flatmapfunc" >}}
{{< tab "Java" >}}
```java
// some ordinary POJO
public class WC {public String word; public int count;}
DataSet<WC> words = // [...]
UnsortedGrouping<WC> keyed = words
  .groupBy(new KeySelector<WC, String>() {
     public String getKey(WC wc) { return wc.word; }
   });
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// some ordinary case class
case class WC(word: String, count: Int)
val words: DataSet[WC] = // [...]
val keyed = words.groupBy( _.word )
```
{{< /tab >}}
{{< /tabs >}}

## Data Sources

Data sources create the initial data sets, such as from files or from Java collections. The general mechanism of creating data sets is abstracted behind an InputFormat. Flink comes with several built-in formats to create data sets from common file formats. Many of them have shortcut methods on the ExecutionEnvironment.

File-based:

  * readTextFile(path) / TextInputFormat - Reads files line wise and returns them as Strings.

  * readTextFileWithValue(path) / TextValueInputFormat - Reads files line wise and returns them as StringValues. StringValues are mutable strings.

  * readCsvFile(path) / CsvInputFormat - Parses files of comma (or another char) delimited fields. Returns a DataSet of tuples or POJOs. Supports the basic java types and their Value counterparts as field types.

  * readFileOfPrimitives(path, Class) / PrimitiveInputFormat - Parses files of new-line (or another char sequence) delimited primitive data types such as String or Integer.

  * readFileOfPrimitives(path, delimiter, Class) / PrimitiveInputFormat - Parses files of new-line (or another char sequence) delimited primitive data types such as String or Integer using the given delimiter.

Collection-based:

  * fromCollection(Collection) - Creates a data set from a Java.util.Collection. All elements in the collection must be of the same type.

  * fromCollection(Iterator, Class) - Creates a data set from an iterator. The class specifies the data type of the elements returned by the iterator.

  * fromElements(T ...) - Creates a data set from the given sequence of objects. All objects must be of the same type.

  * fromParallelCollection(SplittableIterator, Class) - Creates a data set from an iterator, in parallel. The class specifies the data type of the elements returned by the iterator.

  * generateSequence(from, to) - Generates the sequence of numbers in the given interval, in parallel.

Generic:

  * readFile(inputFormat, path) / FileInputFormat - Accepts a file input format.

  * createInput(inputFormat) / InputFormat - Accepts a generic input format.


{{< tabs "datasources" >}}
{{< tab "Java" >}}
```java
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
      JdbcInputFormat.buildJdbcInputFormat()
                     .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                     .setDBUrl("jdbc:derby:memory:persons")
                     .setQuery("select name, age from persons")
                     .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                     .finish()
    );

// Note: Flink's program compiler needs to infer the data types of the data items which are returned
// by an InputFormat. If this information cannot be automatically inferred, it is necessary to
// manually provide the type information as shown in the examples above.
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala


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
```
{{< /tab >}}
{{< /tabs >}}

#### Configuring CSV Parsing

Flink offers a number of configuration options for CSV parsing:

  * types(Class ... types) specifies the types of the fields to parse. It is mandatory to configure the types of the parsed fields. In case of the type class Boolean.class, “True” (case-insensitive), “False” (case-insensitive), “1” and “0” are treated as booleans.

  * lineDelimiter(String del) specifies the delimiter of individual records. The default line delimiter is the new-line character '\n'.

  * fieldDelimiter(String del) specifies the delimiter that separates fields of a record. The default field delimiter is the comma character ','.

  * includeFields(boolean ... flag), includeFields(String mask), or includeFields(long bitMask) defines which fields to read from the input file (and which to ignore). By default the first n fields (as defined by the number of types in the types() call) are parsed.

  * parseQuotedStrings(char quoteChar) enables quoted string parsing. Strings are parsed as quoted strings if the first character of the string field is the quote character (leading or tailing whitespaces are not trimmed). Field delimiters within quoted strings are ignored. Quoted string parsing fails if the last character of a quoted string field is not the quote character or if the quote character appears at some point which is not the start or the end of the quoted string field (unless the quote character is escaped using ‘'). If quoted string parsing is enabled and the first character of the field is not the quoting string, the string is parsed as unquoted string. By default, quoted string parsing is disabled.

  * ignoreComments(String commentPrefix) specifies a comment prefix. All lines that start with the specified comment prefix are not parsed and ignored. By default, no lines are ignored.

  * ignoreInvalidLines() enables lenient parsing, i.e., lines that cannot be correctly parsed are ignored. By default, lenient parsing is disabled and invalid lines raise an exception.

  * ignoreFirstLine() configures the InputFormat to ignore the first line of the input file. By default no line is ignored.

#### Recursive Traversal of the Input Path Directory

For file-based inputs, when the input path is a directory, nested files are not enumerated by default.
Instead, only the files inside the base directory are read, while nested files are ignored.
Recursive enumeration of nested files can be enabled through the recursive.file.enumeration configuration parameter, like in the following example.

{{< tabs "recursiveinput" >}}
{{< tab "Java" >}}
```java
// enable recursive enumeration of nested input files
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// create a configuration object
Configuration parameters = new Configuration();

// set the recursive enumeration parameter
parameters.setBoolean("recursive.file.enumeration", true);

// pass the configuration to the data source
DataSet<String> logs = env.readTextFile("file:///path/with.nested/files")
			  .withParameters(parameters);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// enable recursive enumeration of nested input files
val env  = ExecutionEnvironment.getExecutionEnvironment

// create a configuration object
val parameters = new Configuration

// set the recursive enumeration parameter
parameters.setBoolean("recursive.file.enumeration", true)

// pass the configuration to the data source
env.readTextFile("file:///path/with.nested/files").withParameters(parameters)
```
{{< /tab >}}
{{< /tabs >}}

### Read Compressed Files

Flink currently supports transparent decompression of input files if these are marked with an appropriate file extension. In particular, this means that no further configuration of the input formats is necessary and any `FileInputFormat` support the compression, including custom input formats. Please notice that compressed files might not be read in parallel, thus impacting job scalability.

The following table lists the currently supported compression methods.

| Compressed Method | File Extensions | Parallelizable |
|-------------------|-----------------|----------------|
| DEFLATE | .deflate | no |
| GZip | .gz, .gzip | no |
| Bzip2 | .bz2 | no |
| XZ | .xz | no |
| ZStandart | .zst | no |

## Data Sinks

Data sinks consume DataSets and are used to store or return them. Data sink operations are described using an OutputFormat. Flink comes with a variety of built-in output formats that are encapsulated behind operations on the DataSet:

  * writeAsText() / TextOutputFormat - Writes elements line-wise as Strings. The Strings are obtained by calling the toString() method of each element.
  * writeAsFormattedText() / TextOutputFormat - Write elements line-wise as Strings. The Strings are obtained by calling a user-defined format() method for each element.
  * writeAsCsv(...) / CsvOutputFormat - Writes tuples as comma-separated value files. Row and field delimiters are configurable. The value for each field comes from the toString() method of the objects.
    print() / printToErr() / print(String msg) / printToErr(String msg) - Prints the toString() value of each element on the standard out / standard error stream. Optionally, a prefix (msg) can be provided which is prepended to the output. This can help to distinguish between different calls to print. If the parallelism is greater than 1, the output will also be prepended with the identifier of the task which produced the output.
  * write() / FileOutputFormat - Method and base class for custom file outputs. Supports custom object-to-bytes conversion.
  * output()/ OutputFormat - Most generic output method, for data sinks that are not file based (such as storing the result in a database).

A DataSet can be input to multiple operations. Programs can write or print a data set and at the same time run additional transformations on them.

{{< tabs "datasinkbuiltin" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

Or with a custom output format:

```java
DataSet<Tuple3<String, Integer, Double>> myResult = [...]

// write Tuple DataSet to a relational database
myResult.output(
    // build and configure OutputFormat
    JdbcOutputFormat.buildJdbcOutputFormat()
                    .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                    .setDBUrl("jdbc:derby:memory:persons")
                    .setQuery("insert into persons (name, age, height) values (?,?,?)")
                    .finish()
    );
```

#### Locally Sorted Output

The output of a data sink can be locally sorted on specified fields in specified orders using tuple field positions or field expressions. This works for every output format.

The following examples show how to use this feature:

```java
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
```

Globally sorted output is not supported.

## Iteration Operators

Iterations implement loops in Flink programs.
The iteration operators encapsulate a part of the program and execute it repeatedly, feeding back the result of one iteration (the partial solution) into the next iteration.
There are two types of iterations in Flink: `BulkIteration` and `DeltaIteration`.

This section provides quick examples on how to use both operators. Check out the Introduction to Iterations page for a more detailed introduction.

{{< tabs "iteration" >}}
{{< tab "Java" >}}

#### Bulk Iterations

To create a BulkIteration call the iterate(int) method of the DataSet the iteration should start at. This will return an IterativeDataSet, which can be transformed with the regular operators. The single argument to the iterate call specifies the maximum number of iterations.

To specify the end of an iteration call the closeWith(DataSet) method on the IterativeDataSet to specify which transformation should be fed back to the next iteration. You can optionally specify a termination criterion with closeWith(DataSet, DataSet), which evaluates the second DataSet and terminates the iteration, if this DataSet is empty. If no termination criterion is specified, the iteration terminates after the given maximum number iterations.

The following example iteratively estimates the number Pi. The goal is to count the number of random points, which fall into the unit circle. In each iteration, a random point is picked. If this point lies inside the unit circle, we increment the count. Pi is then estimated as the resulting count divided by the number of iterations multiplied by 4.

```java
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
```

#### Delta Iterations

Delta iterations exploit the fact that certain algorithms do not change every data point of the solution in each iteration.

In addition to the partial solution that is fed back (called workset) in every iteration, delta iterations maintain state across iterations (called solution set), which can be updated through deltas. The result of the iterative computation is the state after the last iteration. Please refer to the Introduction to Iterations for an overview of the basic principle of delta iterations.

Defining a DeltaIteration is similar to defining a BulkIteration. For delta iterations, two data sets form the input to each iteration (workset and solution set), and two data sets are produced as the result (new workset, solution set delta) in each iteration.

To create a DeltaIteration call the iterateDelta(DataSet, int, int) (or iterateDelta(DataSet, int, int[]) respectively). This method is called on the initial solution set. The arguments are the initial delta set, the maximum number of iterations and the key positions. The returned DeltaIteration object gives you access to the DataSets representing the workset and solution set via the methods iteration.getWorkset() and iteration.getSolutionSet().

Below is an example for the syntax of a delta iteration

```java
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
```

{{< /tab >}}
{{< tab "Scala" >}}

#### Bulk Iterations

To create a BulkIteration call the iterate(int) method of the DataSet the iteration should start at and also specify a step function. The step function gets the input DataSet for the current iteration and must return a new DataSet. The parameter of the iterate call is the maximum number of iterations after which to stop.

There is also the iterateWithTermination(int) function that accepts a step function that returns two DataSets: The result of the iteration step and a termination criterion. The iterations are stopped once the termination criterion DataSet is empty.

The following example iteratively estimates the number Pi. The goal is to count the number of random points, which fall into the unit circle. In each iteration, a random point is picked. If this point lies inside the unit circle, we increment the count. Pi is then estimated as the resulting count divided by the number of iterations multiplied by 4.

```scala
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
```

#### Delta Iterations

Delta iterations exploit the fact that certain algorithms do not change every data point of the solution in each iteration.

In addition to the partial solution that is fed back (called workset) in every iteration, delta iterations maintain state across iterations (called solution set), which can be updated through deltas. The result of the iterative computation is the state after the last iteration. Please refer to the Introduction to Iterations for an overview of the basic principle of delta iterations.

Defining a DeltaIteration is similar to defining a BulkIteration. For delta iterations, two data sets form the input to each iteration (workset and solution set), and two data sets are produced as the result (new workset, solution set delta) in each iteration.

To create a DeltaIteration call the iterateDelta(initialWorkset, maxIterations, key) on the initial solution set. The step function takes two parameters: (solutionSet, workset), and must return two values: (solutionSetDelta, newWorkset).

Below is an example for the syntax of a delta iteration

```scala
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
```

{{< /tab >}}
{{< /tabs >}}

## Operating on Data Objects in Functions

Flink’s runtime exchanges data with user functions in form of Java objects. Functions receive input objects from the runtime as method parameters and return output objects as result. Because these objects are accessed by user functions and runtime code, it is very important to understand and follow the rules about how the user code may access, i.e., read and modify, these objects.

User functions receive objects from Flink’s runtime either as regular method parameters (like a MapFunction) or through an Iterable parameter (like a GroupReduceFunction). We refer to objects that the runtime passes to a user function as input objects. User functions can emit objects to the Flink runtime either as a method return value (like a MapFunction) or through a Collector (like a FlatMapFunction). We refer to objects which have been emitted by the user function to the runtime as output objects.

Flink’s DataSet API features two modes that differ in how Flink’s runtime creates or reuses input objects. This behavior affects the guarantees and constraints for how user functions may interact with input and output objects. The following sections define these rules and give coding guidelines to write safe user function code.

### Object-Reuse Disabled (DEFAULT)

By default, Flink operates in object-reuse disabled mode. This mode ensures that functions always receive new input objects within a function call. The object-reuse disabled mode gives better guarantees and is safer to use. However, it comes with a certain processing overhead and might cause higher Java garbage collection activity. The following table explains how user functions may access input and output objects in object-reuse disabled mode.

| Operation | Guarantees and Restrictions | 
|-----------|-----------------------------|
| Reading Input Objects |  Within a method call it is guaranteed that the value of an input object does not change. This includes objects served by an Iterable. For example it is safe to collect input objects served by an Iterable in a List or Map. Note that objects may be modified after the method call is left. It is not safe to remember objects across function calls. |
| Modifying Input Objects | You may modify input objects. |
| Emitting Input Objects | You may emit input objects. The value of an input object may have changed after it was emitted. It is **not safe** to read an input object after it was emitted. |
| Reading Output Objects |  An object that was given to a Collector or returned as method result might have changed its value. It is **not safe** to read an output object. |
| Modifying Output Objects | You may modify an object after it was emitted and emit it again. |

**Coding guidelines for the object-reuse disabled (default) mode:**

  * Do not remember the read input objects across method calls.
  * Do not read objects after you emitted them.

### Object-Reuse Enabled

In object-reuse enabled mode, Flink’s runtime minimizes the number of object instantiations. This can improve the performance and can reduce the Java garbage collection pressure. The object-reuse enabled mode is activated by calling `ExecutionConfig.enableObjectReuse()`. The following table explains how user functions may access input and output objects in object-reuse enabled mode.

| Operation | Guarantees and Restrictions |
|-----------|-----------------------------|
| Reading input objects received as regular method parameters | Input objects received as regular method arguments are not modified within a function call. Objects may be modified after method call is left. It is **not safe** to remember objects across function calls. |
| Reading input objects received from an Iterable parameter | Input objects received from an Iterable are only valid until the next() method is called. An Iterable or Iterator may serve the same object instance multiple times. It is **not safe** to remember input objects received from an Iterable, e.g., by putting them in a List or Map. |
| Modifying Input Objects | You **must not** modify input objects, except for input objects of MapFunction, FlatMapFunction, MapPartitionFunction, GroupReduceFunction, GroupCombineFunction, CoGroupFunction, and InputFormat.next(reuse). | 
| Emitting Input Objects | You **must not** emit input objects, except for input objects of MapFunction, FlatMapFunction, MapPartitionFunction, GroupReduceFunction, GroupCombineFunction, CoGroupFunction, and InputFormat.next(reuse). |
| Reading output Objects | An object that was given to a Collector or returned as method result might have changed its value. It is **not safe** to read an output object. | 
| Modifying Output Objects | You may modify an output object and emit it again. |

**Coding guidelines for object-reuse enabled:**

* Do not remember input objects received from an Iterable.
* Do not remember and read input objects across method calls.
* Do not modify or emit input objects, except for input objects of MapFunction, FlatMapFunction, MapPartitionFunction, GroupReduceFunction, GroupCombineFunction, CoGroupFunction, and InputFormat.next(reuse).
* To reduce object instantiations, you can always emit a dedicated output object which is repeatedly modified but never read.


## Debugging

Before running a data analysis program on a large data set in a distributed cluster, it is a good idea to make sure that the implemented algorithm works as desired. Hence, implementing data analysis programs is usually an incremental process of checking results, debugging, and improving.

Flink provides a few nice features to significantly ease the development process of data analysis programs by supporting local debugging from within an IDE, injection of test data, and collection of result data. This section give some hints how to ease the development of Flink programs.

### Local Execution Envronment

A LocalEnvironment starts a Flink system within the same JVM process it was created in. If you start the LocalEnvironment from an IDE, you can set breakpoints in your code and easily debug your program.

A LocalEnvironment is created and used as follows:

{{< tabs "localenv" >}}
{{< tab "Java" >}}
```java
final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

DataSet<String> lines = env.readTextFile(pathToTextFile);
// build your program

env.execute();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = ExecutionEnvironment.createLocalEnvironment()

val lines = env.readTextFile(pathToTextFile)
// build your program

env.execute()
```
{{< /tab >}}
{{< /tabs >}}

### Collection Data Sources and Sinks

Providing input for an analysis program and checking its output is cumbersome when done by creating input files and reading output files. Flink features special data sources and sinks which are backed by Java collections to ease testing. Once a program has been tested, the sources and sinks can be easily replaced by sources and sinks that read from / write to external data stores such as HDFS.

Collection data sources can be used as follows:

{{< tabs "collectionenv" >}}
{{< tab "Java" >}}
```java
final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

// Create a DataSet from a list of elements
DataSet<Integer> myInts = env.fromElements(1, 2, 3, 4, 5);

// Create a DataSet from any Java collection
List<Tuple2<String, Integer>> data = ...
DataSet<Tuple2<String, Integer>> myTuples = env.fromCollection(data);

// Create a DataSet from an Iterator
Iterator<Long> longIt = ...
DataSet<Long> myLongs = env.fromCollection(longIt, Long.class);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = ExecutionEnvironment.createLocalEnvironment()

// Create a DataSet from a list of elements
val myInts = env.fromElements(1, 2, 3, 4, 5)

// Create a DataSet from any Collection
val data: Seq[(String, Int)] = ...
val myTuples = env.fromCollection(data)

// Create a DataSet from an Iterator
val longIt: Iterator[Long] = ...
val myLongs = env.fromCollection(longIt)
```
{{< /tab >}}
{{< /tabs >}}

Note: Currently, the collection data source requires that data types and iterators implement Serializable. Furthermore, collection data sources can not be executed in parallel ( parallelism = 1).

## Broadcast Variables

Broadcast variables allow you to make a data set available to all parallel instances of an operation, in addition to the regular input of the operation. This is useful for auxiliary data sets, or data-dependent parameterization. The data set will then be accessible at the operator as a Collection.

* **Broadcast**: broadcast sets are registered by name via withBroadcastSet(DataSet, String), and
* **Access**: accessible via getRuntimeContext().getBroadcastVariable(String) at the target operator.

{{< tabs "broadcastvariable" >}}
{{< tab "Java" >}}
```java
    Java
    Scala

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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

Make sure that the names (broadcastSetName in the previous example) match when registering and accessing broadcast data sets. For a complete example program, have a look at K-Means Algorithm.

Note: As the content of broadcast variables is kept in-memory on each node, it should not become too large. For simpler things like scalar values you can simply make parameters part of the closure of a function, or use the withParameters(...) method to pass in a configuration.

## Distributed Cache

Flink offers a distributed cache, similar to Apache Hadoop, to make files locally accessible to parallel instances of user functions. This functionality can be used to share files that contain static external data such as dictionaries or machine-learned regression models.

The cache works as follows. A program registers a file or directory of a local or remote filesystem such as HDFS or S3 under a specific name in its ExecutionEnvironment as a cached file. When the program is executed, Flink automatically copies the file or directory to the local filesystem of all workers. A user function can look up the file or directory under the specified name and access it from the worker’s local filesystem.

The distributed cache is used as follows:

{{< tabs "distributedcache" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

Access the cached file in a user function (here a MapFunction).
The function must extend a RichFunction class because it needs access to the RuntimeContext.

{{< tabs "distributedcacheread" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

## Passing Parameters to Functions

Parameters can be passed to functions using either the constructor or the `withParameters(Configuration)` method.
The parameters are serialized as part of the function object and shipped to all parallel task instances.

#### Via Constructor

{{< tabs "constructorparams" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val toFilter = env.fromElements(1, 2, 3)

toFilter.filter(new MyFilter(2))

class MyFilter(limit: Int) extends FilterFunction[Int] {
  override def filter(value: Int): Boolean = {
    value > limit
  }
}
```
{{< /tab >}}
{{< /tabs >}}

#### Via `withParameters(Configuration)`

{{< tabs "withparams" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< /tabs >}}

#### Globally via the `ExecutionConfig`


Flink also allows to pass custom configuration values to the ExecutionConfig interface of the environment.
Since the execution config is accessible in all (rich) user functions, the custom configuration will be available globally in all functions.

**Setting a custom global configuration**

{{< tabs "setexecutionconfig" >}}
{{< tab "Java" >}}
```java
Configuration conf = new Configuration();
conf.setString("mykey","myvalue");
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.getConfig().setGlobalJobParameters(conf);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = ExecutionEnvironment.getExecutionEnvironment
val conf = new Configuration()
conf.setString("mykey", "myvalue")
env.getConfig.setGlobalJobParameters(conf)
```
{{< /tab >}}
{{< /tabs >}}

Please note that you can also pass a custom class extending the ExecutionConfig.GlobalJobParameters class as the global job parameters to the execution config. The interface allows to implement the `Map<String, String> toMap()` method which will in turn show the values from the configuration in the web frontend.

**Accessing values from the global configuration**

```java
public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

  private String mykey;

  @Override
  public void open(Configuration parameters) throws Exception {
      ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
      Configuration globConf = (Configuration) globalParams;
      mykey = globConf.getString("mykey", null);
  }
```
