---
title: Transformations
nav-title: Transformations
weight: 2
type: docs
aliases:
  - /dev/batch/dataset_transformations.html
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

# DataSet Transformations

This document gives a deep-dive into the available transformations on DataSets. For a general introduction to the
Flink Java API, please refer to the [Programming Guide]({{< ref "docs/dev/dataset/overview" >}}).

For zipping elements in a data set with a dense index, please refer to the [Zip Elements Guide]({{< ref "docs/dev/dataset/zip_elements_guide" >}}).

### Map

The Map transformation applies a user-defined map function on each element of a DataSet.
It implements a one-to-one mapping, that is, exactly one element must be returned by
the function.

The following code transforms a DataSet of Integer pairs into a DataSet of Integers:

{{< tabs "3a758074-c167-4b66-a787-90525d451ddb" >}}
{{< tab "Java" >}}

```java
// MapFunction that adds two integer values
public class IntAdder implements MapFunction<Tuple2<Integer, Integer>, Integer> {
  @Override
  public Integer map(Tuple2<Integer, Integer> in) {
    return in.f0 + in.f1;
  }
}

// [...]
DataSet<Tuple2<Integer, Integer>> intPairs = // [...]
DataSet<Integer> intSums = intPairs.map(new IntAdder());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val intPairs: DataSet[(Int, Int)] = // [...]
val intSums = intPairs.map { pair => pair._1 + pair._2 }
```

{{< /tab >}}
{{< /tabs >}}
### FlatMap

The FlatMap transformation applies a user-defined flat-map function on each element of a DataSet.
This variant of a map function can return arbitrary many result elements (including none) for each input element.

The following code transforms a DataSet of text lines into a DataSet of words:

{{< tabs "3f1a6873-77f0-4113-8339-7617d3b12e41" >}}
{{< tab "Java" >}}

```java
// FlatMapFunction that tokenizes a String by whitespace characters and emits all String tokens.
public class Tokenizer implements FlatMapFunction<String, String> {
  @Override
  public void flatMap(String value, Collector<String> out) {
    for (String token : value.split("\\W")) {
      out.collect(token);
    }
  }
}

// [...]
DataSet<String> textLines = // [...]
DataSet<String> words = textLines.flatMap(new Tokenizer());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val textLines: DataSet[String] = // [...]
val words = textLines.flatMap { _.split(" ") }
```

{{< /tab >}}
{{< /tabs >}}
### MapPartition

MapPartition transforms a parallel partition in a single function call. The map-partition function
gets the partition as Iterable and can produce an arbitrary number of result values. The number of elements in each partition depends on the degree-of-parallelism
and previous operations.

The following code transforms a DataSet of text lines into a DataSet of counts per partition:

{{< tabs "a2230f1a-6f85-41a0-86b9-48b9166e18df" >}}
{{< tab "Java" >}}

```java
public class PartitionCounter implements MapPartitionFunction<String, Long> {

  public void mapPartition(Iterable<String> values, Collector<Long> out) {
    long c = 0;
    for (String s : values) {
      c++;
    }
    out.collect(c);
  }
}

// [...]
DataSet<String> textLines = // [...]
DataSet<Long> counts = textLines.mapPartition(new PartitionCounter());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val textLines: DataSet[String] = // [...]
// Some is required because the return value must be a Collection.
// There is an implicit conversion from Option to a Collection.
val counts = texLines.mapPartition { in => Some(in.size) }
```

{{< /tab >}}
{{< /tabs >}}
### Filter

The Filter transformation applies a user-defined filter function on each element of a DataSet and retains only those elements for which the function returns `true`.

The following code removes all Integers smaller than zero from a DataSet:

{{< tabs "ac9444c1-31ab-4d7d-9e95-e02ba6599837" >}}
{{< tab "Java" >}}

```java
// FilterFunction that filters out all Integers smaller than zero.
public class NaturalNumberFilter implements FilterFunction<Integer> {
  @Override
  public boolean filter(Integer number) {
    return number >= 0;
  }
}

// [...]
DataSet<Integer> intNumbers = // [...]
DataSet<Integer> naturalNumbers = intNumbers.filter(new NaturalNumberFilter());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val intNumbers: DataSet[Int] = // [...]
val naturalNumbers = intNumbers.filter { _ > 0 }
```

{{< /tab >}}
{{< /tabs >}}
**IMPORTANT:** The system assumes that the function does not modify the elements on which the predicate is applied. Violating this assumption
can lead to incorrect results.

### Projection of Tuple DataSet

The Project transformation removes or moves Tuple fields of a Tuple DataSet.
The `project(int...)` method selects Tuple fields that should be retained by their index and defines their order in the output Tuple.

Projections do not require the definition of a user function.

The following code shows different ways to apply a Project transformation on a DataSet:

{{< tabs "91098608-c1e5-4579-bb1d-051d1d06566b" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple3<Integer, Double, String>> in = // [...]
// converts Tuple3<Integer, Double, String> into Tuple2<String, Integer>
DataSet<Tuple2<String, Integer>> out = in.project(2,0);
```

#### Projection with Type Hint

Note that the Java compiler cannot infer the return type of `project` operator. This can cause a problem if you call another operator on a result of `project` operator such as:

```java
DataSet<Tuple5<String,String,String,String,String>> ds = ....
DataSet<Tuple1<String>> ds2 = ds.project(0).distinct(0);
```

This problem can be overcome by hinting the return type of `project` operator like this:

```java
DataSet<Tuple1<String>> ds2 = ds.<Tuple1<String>>project(0).distinct(0);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
Not supported.
```

{{< /tab >}}
{{< /tabs >}}
### Transformations on Grouped DataSet

The reduce operations can operate on grouped data sets. Specifying the key to
be used for grouping can be done in many ways:

- key expressions
- a key-selector function
- one or more field position keys (Tuple DataSet only)
- Case Class fields (Case Classes only)

Please look at the reduce examples to see how the grouping keys are specified.

### Reduce on Grouped DataSet

A Reduce transformation that is applied on a grouped DataSet reduces each group to a single
element using a user-defined reduce function.
For each group of input elements, a reduce function successively combines pairs of elements into one
element until only a single element for each group remains.

Note that for a `ReduceFunction` the keyed fields of the returned object should match the input
values. This is because reduce is implicitly combinable and objects emitted from the combine
operator are again grouped by key when passed to the reduce operator.

#### Reduce on DataSet Grouped by Key Expression

Key expressions specify one or more fields of each element of a DataSet. Each key expression is
either the name of a public field or a getter method. A dot can be used to drill down into objects.
The key expression "*" selects all fields.
The following code shows how to group a POJO DataSet using key expressions and to reduce it
with a reduce function.

{{< tabs "1e955a6e-1aa5-4c86-a783-ad52c13d2464" >}}
{{< tab "Java" >}}

```java
// some ordinary POJO
public class WC {
  public String word;
  public int count;
  // [...]
}

// ReduceFunction that sums Integer attributes of a POJO
public class WordCounter implements ReduceFunction<WC> {
  @Override
  public WC reduce(WC in1, WC in2) {
    return new WC(in1.word, in1.count + in2.count);
  }
}

// [...]
DataSet<WC> words = // [...]
DataSet<WC> wordCounts = words
                         // DataSet grouping on field "word"
                         .groupBy("word")
                         // apply ReduceFunction on grouped DataSet
                         .reduce(new WordCounter());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// some ordinary POJO
class WC(val word: String, val count: Int) {
  def this() {
    this(null, -1)
  }
  // [...]
}

val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy("word").reduce {
  (w1, w2) => new WC(w1.word, w1.count + w2.count)
}
```

{{< /tab >}}
{{< /tabs >}}
#### Reduce on DataSet Grouped by KeySelector Function

A key-selector function extracts a key value from each element of a DataSet. The extracted key
value is used to group the DataSet.
The following code shows how to group a POJO DataSet using a key-selector function and to reduce it
with a reduce function.

{{< tabs "a502de54-4f61-488f-a2d1-6511cfbb8a20" >}}
{{< tab "Java" >}}

```java
// some ordinary POJO
public class WC {
  public String word;
  public int count;
  // [...]
}

// ReduceFunction that sums Integer attributes of a POJO
public class WordCounter implements ReduceFunction<WC> {
  @Override
  public WC reduce(WC in1, WC in2) {
    return new WC(in1.word, in1.count + in2.count);
  }
}

// [...]
DataSet<WC> words = // [...]
DataSet<WC> wordCounts = words
                         // DataSet grouping on field "word"
                         .groupBy(new SelectWord())
                         // apply ReduceFunction on grouped DataSet
                         .reduce(new WordCounter());

public class SelectWord implements KeySelector<WC, String> {
  @Override
  public String getKey(Word w) {
    return w.word;
  }
}
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// some ordinary POJO
class WC(val word: String, val count: Int) {
  def this() {
    this(null, -1)
  }
  // [...]
}

val words: DataSet[WC] = // [...]
val wordCounts = words.groupBy { _.word } reduce {
  (w1, w2) => new WC(w1.word, w1.count + w2.count)
}
```

{{< /tab >}}
{{< /tabs >}}
#### Reduce on DataSet Grouped by Field Position Keys (Tuple DataSets only)

Field position keys specify one or more fields of a Tuple DataSet that are used as grouping keys.
The following code shows how to use field position keys and apply a reduce function

{{< tabs "6195742f-896b-471b-a35d-75545a9905c3" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple3<String, Integer, Double>> tuples = // [...]
DataSet<Tuple3<String, Integer, Double>> reducedTuples = tuples
                                         // group DataSet on first and second field of Tuple
                                         .groupBy(0, 1)
                                         // apply ReduceFunction on grouped DataSet
                                         .reduce(new MyTupleReducer());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val tuples = DataSet[(String, Int, Double)] = // [...]
// group on the first and second Tuple field
val reducedTuples = tuples.groupBy(0, 1).reduce { ... }
```

{{< /tab >}}
{{< /tabs >}}
#### Reduce on DataSet grouped by Case Class Fields

When using Case Classes you can also specify the grouping key using the names of the fields:

{{< tabs "732febf3-c182-475f-a0e6-44434ea6dd62" >}}
{{< tab "Java" >}}

```java
Not supported.
```
{{< /tab >}}
{{< tab "Scala" >}}

```scala
case class MyClass(val a: String, b: Int, c: Double)
val tuples = DataSet[MyClass] = // [...]
// group on the first and second field
val reducedTuples = tuples.groupBy("a", "b").reduce { ... }
```

{{< /tab >}}
{{< /tabs >}}
### GroupReduce on Grouped DataSet

A GroupReduce transformation that is applied on a grouped DataSet calls a user-defined
group-reduce function for each group. The difference
between this and *Reduce* is that the user defined function gets the whole group at once.
The function is invoked with an Iterable over all elements of a group and can return an arbitrary
number of result elements.

#### GroupReduce on DataSet Grouped by Field Position Keys (Tuple DataSets only)

The following code shows how duplicate strings can be removed from a DataSet grouped by Integer.

{{< tabs "d7c3d396-189e-418b-a326-888172e480dd" >}}
{{< tab "Java" >}}

```java
public class DistinctReduce
         implements GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

  @Override
  public void reduce(Iterable<Tuple2<Integer, String>> in, Collector<Tuple2<Integer, String>> out) {

    Set<String> uniqStrings = new HashSet<String>();
    Integer key = null;

    // add all strings of the group to the set
    for (Tuple2<Integer, String> t : in) {
      key = t.f0;
      uniqStrings.add(t.f1);
    }

    // emit all unique strings.
    for (String s : uniqStrings) {
      out.collect(new Tuple2<Integer, String>(key, s));
    }
  }
}

// [...]
DataSet<Tuple2<Integer, String>> input = // [...]
DataSet<Tuple2<Integer, String>> output = input
                           .groupBy(0)            // group DataSet by the first tuple field
                           .reduceGroup(new DistinctReduce());  // apply GroupReduceFunction
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, String)] = // [...]
val output = input.groupBy(0).reduceGroup {
      (in, out: Collector[(Int, String)]) =>
        in.toSet foreach (out.collect)
    }
```

{{< /tab >}}
{{< /tabs >}}
#### GroupReduce on DataSet Grouped by Key Expression, KeySelector Function, or Case Class Fields

Work analogous to [key expressions](#reduce-on-dataset-grouped-by-key-expression),
[key-selector functions](#reduce-on-dataset-grouped-by-keyselector-function),
and [case class fields](#reduce-on-dataset-grouped-by-case-class-fields) in *Reduce* transformations.


#### GroupReduce on sorted groups

A group-reduce function accesses the elements of a group using an Iterable. Optionally, the Iterable can hand out the elements of a group in a specified order. In many cases this can help to reduce the complexity of a user-defined
group-reduce function and improve its efficiency.

The following code shows another example how to remove duplicate Strings in a DataSet grouped by an Integer and sorted by String.

{{< tabs "94d3e73d-03e1-4f1b-a5b7-4ebfad7f9409" >}}
{{< tab "Java" >}}

```java
// GroupReduceFunction that removes consecutive identical elements
public class DistinctReduce
         implements GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

  @Override
  public void reduce(Iterable<Tuple2<Integer, String>> in, Collector<Tuple2<Integer, String>> out) {
    Integer key = null;
    String comp = null;

    for (Tuple2<Integer, String> t : in) {
      key = t.f0;
      String next = t.f1;

      // check if strings are different
      if (comp == null || !next.equals(comp)) {
        out.collect(new Tuple2<Integer, String>(key, next));
        comp = next;
      }
    }
  }
}

// [...]
DataSet<Tuple2<Integer, String>> input = // [...]
DataSet<Double> output = input
                         .groupBy(0)                         // group DataSet by first field
                         .sortGroup(1, Order.ASCENDING)      // sort groups on second tuple field
                         .reduceGroup(new DistinctReduce());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, String)] = // [...]
val output = input.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup {
      (in, out: Collector[(Int, String)]) =>
        var prev: (Int, String) = null
        for (t <- in) {
          if (prev == null || prev != t)
            out.collect(t)
            prev = t
        }
    }

```

{{< /tab >}}
{{< /tabs >}}
**Note:** A GroupSort often comes for free if the grouping is established using a sort-based execution strategy of an operator before the reduce operation.

#### Combinable GroupReduceFunctions

In contrast to a reduce function, a group-reduce function is not
implicitly combinable. In order to make a group-reduce function
combinable it must implement the `GroupCombineFunction` interface.

**Important**: The generic input and output types of
the `GroupCombineFunction` interface must be equal to the generic input type
of the `GroupReduceFunction` as shown in the following example:

{{< tabs "f6680a99-adcb-433f-87d7-390468a7a240" >}}
{{< tab "Java" >}}

```java
// Combinable GroupReduceFunction that computes a sum.
public class MyCombinableGroupReducer implements
  GroupReduceFunction<Tuple2<String, Integer>, String>,
  GroupCombineFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
{
  @Override
  public void reduce(Iterable<Tuple2<String, Integer>> in,
                     Collector<String> out) {

    String key = null;
    int sum = 0;

    for (Tuple2<String, Integer> curr : in) {
      key = curr.f0;
      sum += curr.f1;
    }
    // concat key and sum and emit
    out.collect(key + "-" + sum);
  }

  @Override
  public void combine(Iterable<Tuple2<String, Integer>> in,
                      Collector<Tuple2<String, Integer>> out) {
    String key = null;
    int sum = 0;

    for (Tuple2<String, Integer> curr : in) {
      key = curr.f0;
      sum += curr.f1;
    }
    // emit tuple with key and sum
    out.collect(new Tuple2<>(key, sum));
  }
}
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala

// Combinable GroupReduceFunction that computes two sums.
class MyCombinableGroupReducer
  extends GroupReduceFunction[(String, Int), String]
  with GroupCombineFunction[(String, Int), (String, Int)]
{
  override def reduce(
    in: java.lang.Iterable[(String, Int)],
    out: Collector[String]): Unit =
  {
    val r: (String, Int) =
      in.iterator.asScala.reduce( (a,b) => (a._1, a._2 + b._2) )
    // concat key and sum and emit
    out.collect (r._1 + "-" + r._2)
  }

  override def combine(
    in: java.lang.Iterable[(String, Int)],
    out: Collector[(String, Int)]): Unit =
  {
    val r: (String, Int) =
      in.iterator.asScala.reduce( (a,b) => (a._1, a._2 + b._2) )
    // emit tuple with key and sum
    out.collect(r)
  }
}
```

{{< /tab >}}
{{< /tabs >}}
### GroupCombine on a Grouped DataSet

The GroupCombine transformation is the generalized form of the combine step in
the combinable GroupReduceFunction. It is generalized in the sense that it
allows combining of input type `I` to an arbitrary output type `O`. In contrast,
the combine step in the GroupReduce only allows combining from input type `I` to
output type `I`. This is because the reduce step in the GroupReduceFunction
expects input type `I`.

In some applications, it is desirable to combine a DataSet into an intermediate
format before performing additional transformations (e.g. to reduce data
size). This can be achieved with a CombineGroup transformation with very little
costs.

**Note:** The GroupCombine on a Grouped DataSet is performed in memory with a
  greedy strategy which may not process all data at once but in multiple
  steps. It is also performed on the individual partitions without a data
  exchange like in a GroupReduce transformation. This may lead to partial
  results.

The following example demonstrates the use of a CombineGroup transformation for
an alternative WordCount implementation.

{{< tabs "e03a56af-1a35-44c1-b677-40bf2e07674f" >}}
{{< tab "Java" >}}

```java
DataSet<String> input = [..] // The words received as input

DataSet<Tuple2<String, Integer>> combinedWords = input
  .groupBy(0) // group identical words
  .combineGroup(new GroupCombineFunction<String, Tuple2<String, Integer>() {

    public void combine(Iterable<String> words, Collector<Tuple2<String, Integer>>) { // combine
        String key = null;
        int count = 0;

        for (String word : words) {
            key = word;
            count++;
        }
        // emit tuple with word and count
        out.collect(new Tuple2(key, count));
    }
});

DataSet<Tuple2<String, Integer>> output = combinedWords
  .groupBy(0)                              // group by words again
  .reduceGroup(new GroupReduceFunction() { // group reduce with full data exchange

    public void reduce(Iterable<Tuple2<String, Integer>>, Collector<Tuple2<String, Integer>>) {
        String key = null;
        int count = 0;

        for (Tuple2<String, Integer> word : words) {
            key = word;
            count++;
        }
        // emit tuple with word and count
        out.collect(new Tuple2(key, count));
    }
});
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[String] = [..] // The words received as input

val combinedWords: DataSet[(String, Int)] = input
  .groupBy(0)
  .combineGroup {
    (words, out: Collector[(String, Int)]) =>
        var key: String = null
        var count = 0

        for (word <- words) {
            key = word
            count += 1
        }
        out.collect((key, count))
}

val output: DataSet[(String, Int)] = combinedWords
  .groupBy(0)
  .reduceGroup {
    (words, out: Collector[(String, Int)]) =>
        var key: String = null
        var sum = 0

        for ((word, sum) <- words) {
            key = word
            sum += count
        }
        out.collect((key, sum))
}

```

{{< /tab >}}
{{< /tabs >}}
The above alternative WordCount implementation demonstrates how the GroupCombine
combines words before performing the GroupReduce transformation. The above
example is just a proof of concept. Note, how the combine step changes the type
of the DataSet which would normally require an additional Map transformation
before executing the GroupReduce.

### Aggregate on Grouped Tuple DataSet

There are some common aggregation operations that are frequently used. The Aggregate transformation provides the following build-in aggregation functions:

- Sum,
- Min, and
- Max.

The Aggregate transformation can only be applied on a Tuple DataSet and supports only field position keys for grouping.

The following code shows how to apply an Aggregation transformation on a DataSet grouped by field position keys:

{{< tabs "9d8f27c8-217b-4bda-b56d-0606d5ee77a4" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple3<Integer, String, Double>> input = // [...]
DataSet<Tuple3<Integer, String, Double>> output = input
                                   .groupBy(1)        // group DataSet on second field
                                   .aggregate(SUM, 0) // compute sum of the first field
                                   .and(MIN, 2);      // compute minimum of the third field
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output = input.groupBy(1).aggregate(SUM, 0).and(MIN, 2)
```

{{< /tab >}}
{{< /tabs >}}
To apply multiple aggregations on a DataSet it is necessary to use the `.and()` function after the first aggregate, that means `.aggregate(SUM, 0).and(MIN, 2)` produces the sum of field 0 and the minimum of field 2 of the original DataSet.
In contrast to that `.aggregate(SUM, 0).aggregate(MIN, 2)` will apply an aggregation on an aggregation. In the given example it would produce the minimum of field 2 after calculating the sum of field 0 grouped by field 1.

**Note:** The set of aggregation functions will be extended in the future.

### MinBy / MaxBy on Grouped Tuple DataSet

The MinBy (MaxBy) transformation selects a single tuple for each group of tuples. The selected tuple is the tuple whose values of one or more specified fields are minimum (maximum). The fields which are used for comparison must be valid key fields, i.e., comparable. If multiple tuples have minimum (maximum) fields values, an arbitrary tuple of these tuples is returned.

The following code shows how to select the tuple with the minimum values for the `Integer` and `Double` fields for each group of tuples with the same `String` value from a `DataSet<Tuple3<Integer, String, Double>>`:

{{< tabs "d40f26af-4669-4c36-8193-aca1cf547cdb" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple3<Integer, String, Double>> input = // [...]
DataSet<Tuple3<Integer, String, Double>> output = input
                                   .groupBy(1)   // group DataSet on second field
                                   .minBy(0, 2); // select tuple with minimum values for first and third field.
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Double)] = input
                                   .groupBy(1)  // group DataSet on second field
                                   .minBy(0, 2) // select tuple with minimum values for first and third field.
```

{{< /tab >}}
{{< /tabs >}}
### Reduce on full DataSet

The Reduce transformation applies a user-defined reduce function to all elements of a DataSet.
The reduce function subsequently combines pairs of elements into one element until only a single element remains.

The following code shows how to sum all elements of an Integer DataSet:

{{< tabs "c11498cd-b758-4fd6-ad0b-223458b238e2" >}}
{{< tab "Java" >}}

```java
// ReduceFunction that sums Integers
public class IntSummer implements ReduceFunction<Integer> {
  @Override
  public Integer reduce(Integer num1, Integer num2) {
    return num1 + num2;
  }
}

// [...]
DataSet<Integer> intNumbers = // [...]
DataSet<Integer> sum = intNumbers.reduce(new IntSummer());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val intNumbers = env.fromElements(1,2,3)
val sum = intNumbers.reduce (_ + _)
```

{{< /tab >}}
{{< /tabs >}}
Reducing a full DataSet using the Reduce transformation implies that the final Reduce operation cannot be done in parallel. However, a reduce function is automatically combinable such that a Reduce transformation does not limit scalability for most use cases.

### GroupReduce on full DataSet

The GroupReduce transformation applies a user-defined group-reduce function on all elements of a DataSet.
A group-reduce can iterate over all elements of DataSet and return an arbitrary number of result elements.

The following example shows how to apply a GroupReduce transformation on a full DataSet:

{{< tabs "9f1bf18b-5a2e-473b-bfc1-331421b52ef2" >}}
{{< tab "Java" >}}

```java
DataSet<Integer> input = // [...]
// apply a (preferably combinable) GroupReduceFunction to a DataSet
DataSet<Double> output = input.reduceGroup(new MyGroupReducer());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[Int] = // [...]
val output = input.reduceGroup(new MyGroupReducer())
```

{{< /tab >}}
{{< /tabs >}}
**Note:** A GroupReduce transformation on a full DataSet cannot be done in parallel if the
group-reduce function is not combinable. Therefore, this can be a very compute intensive operation.
See the paragraph on "Combinable GroupReduceFunctions" above to learn how to implement a
combinable group-reduce function.

### GroupCombine on a full DataSet

The GroupCombine on a full DataSet works similar to the GroupCombine on a
grouped DataSet. The data is partitioned on all nodes and then combined in a
greedy fashion (i.e. only data fitting into memory is combined at once).

### Aggregate on full Tuple DataSet

There are some common aggregation operations that are frequently used. The Aggregate transformation
provides the following build-in aggregation functions:

- Sum,
- Min, and
- Max.

The Aggregate transformation can only be applied on a Tuple DataSet.

The following code shows how to apply an Aggregation transformation on a full DataSet:

{{< tabs "03da5cf6-dec9-4a70-9c6a-b89b0a139c41" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<Integer, Double>> input = // [...]
DataSet<Tuple2<Integer, Double>> output = input
                                     .aggregate(SUM, 0)    // compute sum of the first field
                                     .and(MIN, 1);    // compute minimum of the second field
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output = input.aggregate(SUM, 0).and(MIN, 2)

```

{{< /tab >}}
{{< /tabs >}}
**Note:** Extending the set of supported aggregation functions is on our roadmap.

### MinBy / MaxBy on full Tuple DataSet

The MinBy (MaxBy) transformation selects a single tuple from a DataSet of tuples. The selected tuple is the tuple whose values of one or more specified fields are minimum (maximum). The fields which are used for comparison must be valid key fields, i.e., comparable. If multiple tuples have minimum (maximum) fields values, an arbitrary tuple of these tuples is returned.

The following code shows how to select the tuple with the maximum values for the `Integer` and `Double` fields from a `DataSet<Tuple3<Integer, String, Double>>`:

{{< tabs "1b5a62f0-0463-415e-92d9-05aa10df2393" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple3<Integer, String, Double>> input = // [...]
DataSet<Tuple3<Integer, String, Double>> output = input
                                   .maxBy(0, 2); // select tuple with maximum values for first and third field.
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output: DataSet[(Int, String, Double)] = input                          
                                   .maxBy(0, 2) // select tuple with maximum values for first and third field.
```

{{< /tab >}}
{{< /tabs >}}
### Distinct

The Distinct transformation computes the DataSet of the distinct elements of the source DataSet.
The following code removes all duplicate elements from the DataSet:

{{< tabs "47904f04-4af7-4c9d-ae85-e245f13ce3bc" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<Integer, Double>> input = // [...]
DataSet<Tuple2<Integer, Double>> output = input.distinct();

```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, String, Double)] = // [...]
val output = input.distinct()

```

{{< /tab >}}
{{< /tabs >}}
It is also possible to change how the distinction of the elements in the DataSet is decided, using:

- one or more field position keys (Tuple DataSets only),
- a key-selector function, or
- a key expression.

#### Distinct with field position keys

{{< tabs "51da20ca-143d-48a6-bef7-fd74df136083" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<Integer, Double, String>> input = // [...]
DataSet<Tuple2<Integer, Double, String>> output = input.distinct(0,2);

```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[(Int, Double, String)] = // [...]
val output = input.distinct(0,2)

```

{{< /tab >}}
{{< /tabs >}}
#### Distinct with KeySelector function

{{< tabs "3f5974bb-f5db-43cb-81f5-fcb653e81902" >}}
{{< tab "Java" >}}

```java
private static class AbsSelector implements KeySelector<Integer, Integer> {
private static final long serialVersionUID = 1L;
	@Override
	public Integer getKey(Integer t) {
    	return Math.abs(t);
	}
}
DataSet<Integer> input = // [...]
DataSet<Integer> output = input.distinct(new AbsSelector());

```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input: DataSet[Int] = // [...]
val output = input.distinct {x => Math.abs(x)}

```

{{< /tab >}}
{{< /tabs >}}
#### Distinct with key expression

{{< tabs "bd8e5712-84ab-4851-9ad1-6aa79cacac37" >}}
{{< tab "Java" >}}

```java
// some ordinary POJO
public class CustomType {
  public String aName;
  public int aNumber;
  // [...]
}

DataSet<CustomType> input = // [...]
DataSet<CustomType> output = input.distinct("aName", "aNumber");

```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// some ordinary POJO
case class CustomType(aName : String, aNumber : Int) { }

val input: DataSet[CustomType] = // [...]
val output = input.distinct("aName", "aNumber")

```

{{< /tab >}}
{{< /tabs >}}
It is also possible to indicate to use all the fields by the wildcard character:

{{< tabs "2219a467-df97-4f7f-bd6b-783ff95f03d7" >}}
{{< tab "Java" >}}

```java
DataSet<CustomType> input = // [...]
DataSet<CustomType> output = input.distinct("*");

```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// some ordinary POJO
val input: DataSet[CustomType] = // [...]
val output = input.distinct("_")

```

{{< /tab >}}
{{< /tabs >}}
### Join

The Join transformation joins two DataSets into one DataSet. The elements of both DataSets are joined on one or more keys which can be specified using

- a key expression
- a key-selector function
- one or more field position keys (Tuple DataSet only).
- Case Class Fields

There are a few different ways to perform a Join transformation which are shown in the following.

#### Default Join (Join into Tuple2)

The default Join transformation produces a new Tuple DataSet with two fields. Each tuple holds a joined element of the first input DataSet in the first tuple field and a matching element of the second input DataSet in the second field.

The following code shows a default Join transformation using field position keys:

{{< tabs "e44b7d2f-474f-4b47-bd5b-873b9230fd90" >}}
{{< tab "Java" >}}

```java
public static class User { public String name; public int zip; }
public static class Store { public Manager mgr; public int zip; }
DataSet<User> input1 = // [...]
DataSet<Store> input2 = // [...]
// result dataset is typed as Tuple2
DataSet<Tuple2<User, Store>>
            result = input1.join(input2)
                           .where("zip")       // key of the first input (users)
                           .equalTo("zip");    // key of the second input (stores)
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input1: DataSet[(Int, String)] = // [...]
val input2: DataSet[(Double, Int)] = // [...]
val result = input1.join(input2).where(0).equalTo(1)
```

{{< /tab >}}
{{< /tabs >}}
#### Join with Join Function

A Join transformation can also call a user-defined join function to process joining tuples.
A join function receives one element of the first input DataSet and one element of the second input DataSet and returns exactly one element.

The following code performs a join of DataSet with custom java objects and a Tuple DataSet using key-selector functions and shows how to use a user-defined join function:

{{< tabs "260fd4ce-1c12-44cd-a22a-dbb7de50c1b9" >}}
{{< tab "Java" >}}

```java
// some POJO
public class Rating {
  public String name;
  public String category;
  public int points;
}

// Join function that joins a custom POJO with a Tuple
public class PointWeighter
         implements JoinFunction<Rating, Tuple2<String, Double>, Tuple2<String, Double>> {

  @Override
  public Tuple2<String, Double> join(Rating rating, Tuple2<String, Double> weight) {
    // multiply the points and rating and construct a new output tuple
    return new Tuple2<String, Double>(rating.name, rating.points * weight.f1);
  }
}

DataSet<Rating> ratings = // [...]
DataSet<Tuple2<String, Double>> weights = // [...]
DataSet<Tuple2<String, Double>>
            weightedRatings =
            ratings.join(weights)

                   // key of the first input
                   .where("category")

                   // key of the second input
                   .equalTo("f0")

                   // applying the JoinFunction on joining pairs
                   .with(new PointWeighter());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
case class Rating(name: String, category: String, points: Int)

val ratings: DataSet[Ratings] = // [...]
val weights: DataSet[(String, Double)] = // [...]

val weightedRatings = ratings.join(weights).where("category").equalTo(0) {
  (rating, weight) => (rating.name, rating.points * weight._2)
}
```

{{< /tab >}}
{{< /tabs >}}
#### Join with Flat-Join Function

Analogous to Map and FlatMap, a FlatJoin behaves in the same
way as a Join, but instead of returning one element, it can
return (collect), zero, one, or more elements.

{{< tabs "1dcde17c-e76e-490d-affc-efd891138e6b" >}}
{{< tab "Java" >}}

```java
public class PointWeighter
         implements FlatJoinFunction<Rating, Tuple2<String, Double>, Tuple2<String, Double>> {
  @Override
  public void join(Rating rating, Tuple2<String, Double> weight,
	  Collector<Tuple2<String, Double>> out) {
	if (weight.f1 > 0.1) {
		out.collect(new Tuple2<String, Double>(rating.name, rating.points * weight.f1));
	}
  }
}

DataSet<Tuple2<String, Double>>
            weightedRatings =
            ratings.join(weights) // [...]
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
case class Rating(name: String, category: String, points: Int)

val ratings: DataSet[Ratings] = // [...]
val weights: DataSet[(String, Double)] = // [...]

val weightedRatings = ratings.join(weights).where("category").equalTo(0) {
  (rating, weight, out: Collector[(String, Double)]) =>
    if (weight._2 > 0.1) out.collect(rating.name, rating.points * weight._2)
}

```

{{< /tab >}}
{{< /tabs >}}
#### Join with Projection (Java Only)

A Join transformation can construct result tuples using a projection as shown here:

{{< tabs "1b86db6d-cf3d-484a-b478-110af7482e64" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple3<Integer, Byte, String>> input1 = // [...]
DataSet<Tuple2<Integer, Double>> input2 = // [...]
DataSet<Tuple4<Integer, String, Double, Byte>>
            result =
            input1.join(input2)
                  // key definition on first DataSet using a field position key
                  .where(0)
                  // key definition of second DataSet using a field position key
                  .equalTo(0)
                  // select and reorder fields of matching tuples
                  .projectFirst(0,2).projectSecond(1).projectFirst(1);
```

`projectFirst(int...)` and `projectSecond(int...)` select the fields of the first and second joined input that should be assembled into an output Tuple. The order of indexes defines the order of fields in the output tuple.
The join projection works also for non-Tuple DataSets. In this case, `projectFirst()` or `projectSecond()` must be called without arguments to add a joined element to the output Tuple.

{{< /tab >}}
{{< tab "Scala" >}}

```scala
Not supported.
```

{{< /tab >}}
{{< /tabs >}}

#### Join with DataSet Size Hint

In order to guide the optimizer to pick the right execution strategy, you can hint the size of a DataSet to join as shown here:

{{< tabs "65a746da-b8a7-4be4-a39a-54fac41d8cb6" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<Integer, String>> input1 = // [...]
DataSet<Tuple2<Integer, String>> input2 = // [...]

DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>>
            result1 =
            // hint that the second DataSet is very small
            input1.joinWithTiny(input2)
                  .where(0)
                  .equalTo(0);

DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>>
            result2 =
            // hint that the second DataSet is very large
            input1.joinWithHuge(input2)
                  .where(0)
                  .equalTo(0);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input1: DataSet[(Int, String)] = // [...]
val input2: DataSet[(Int, String)] = // [...]

// hint that the second DataSet is very small
val result1 = input1.joinWithTiny(input2).where(0).equalTo(0)

// hint that the second DataSet is very large
val result1 = input1.joinWithHuge(input2).where(0).equalTo(0)

```
{{< /tab >}}
{{< /tabs >}}

#### Join Algorithm Hints

The Flink runtime can execute joins in various ways. Each possible way outperforms the others under
different circumstances. The system tries to pick a reasonable way automatically, but allows you
to manually pick a strategy, in case you want to enforce a specific way of executing the join.

{{< tabs "4de39b7c-3f5c-4002-8b0d-65027fd3dbcb" >}}
{{< tab "Java" >}}

```java
DataSet<SomeType> input1 = // [...]
DataSet<AnotherType> input2 = // [...]

DataSet<Tuple2<SomeType, AnotherType> result =
      input1.join(input2, JoinHint.BROADCAST_HASH_FIRST)
            .where("id").equalTo("key");
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input1: DataSet[SomeType] = // [...]
val input2: DataSet[AnotherType] = // [...]

// hint that the second DataSet is very small
val result1 = input1.join(input2, JoinHint.BROADCAST_HASH_FIRST).where("id").equalTo("key")

```

{{< /tab >}}
{{< /tabs >}}

The following hints are available:

* `OPTIMIZER_CHOOSES`: Equivalent to not giving a hint at all, leaves the choice to the system.

* `BROADCAST_HASH_FIRST`: Broadcasts the first input and builds a hash table from it, which is
  probed by the second input. A good strategy if the first input is very small.

* `BROADCAST_HASH_SECOND`: Broadcasts the second input and builds a hash table from it, which is
  probed by the first input. A good strategy if the second input is very small.

* `REPARTITION_HASH_FIRST`: The system partitions (shuffles) each input (unless the input is already
  partitioned) and builds a hash table from the first input. This strategy is good if the first
  input is smaller than the second, but both inputs are still large.
  *Note:* This is the default fallback strategy that the system uses if no size estimates can be made
  and no pre-existing partitions and sort-orders can be re-used.

* `REPARTITION_HASH_SECOND`: The system partitions (shuffles) each input (unless the input is already
  partitioned) and builds a hash table from the second input. This strategy is good if the second
  input is smaller than the first, but both inputs are still large.

* `REPARTITION_SORT_MERGE`: The system partitions (shuffles) each input (unless the input is already
  partitioned) and sorts each input (unless it is already sorted). The inputs are joined by
  a streamed merge of the sorted inputs. This strategy is good if one or both of the inputs are
  already sorted.


### OuterJoin

The OuterJoin transformation performs a left, right, or full outer join on two data sets. Outer joins are similar to regular (inner) joins and create all pairs of elements that are equal on their keys. In addition, records of the "outer" side (left, right, or both in case of full) are preserved if no matching key is found in the other side. Matching pair of elements (or one element and a `null` value for the other input) are given to a `JoinFunction` to turn the pair of elements into a single element, or to a `FlatJoinFunction` to turn the pair of elements into arbitrarily many (including none) elements.

The elements of both DataSets are joined on one or more keys which can be specified using

- a key expression
- a key-selector function
- one or more field position keys (Tuple DataSet only).
- Case Class Fields

**OuterJoins are only supported for the Java and Scala DataSet API.**

#### OuterJoin with Join Function

A OuterJoin transformation calls a user-defined join function to process joining tuples.
A join function receives one element of the first input DataSet and one element of the second input DataSet and returns exactly one element. Depending on the type of the outer join (left, right, full) one of both input elements of the join function can be `null`.

The following code performs a left outer join of DataSet with custom java objects and a Tuple DataSet using key-selector functions and shows how to use a user-defined join function:

{{< tabs "769adbfd-3f74-40d5-b762-cdf76ca42e13" >}}
{{< tab "Java" >}}

```java
// some POJO
public class Rating {
  public String name;
  public String category;
  public int points;
}

// Join function that joins a custom POJO with a Tuple
public class PointAssigner
         implements JoinFunction<Tuple2<String, String>, Rating, Tuple2<String, Integer>> {

  @Override
  public Tuple2<String, Integer> join(Tuple2<String, String> movie, Rating rating) {
    // Assigns the rating points to the movie.
    // NOTE: rating might be null
    return new Tuple2<String, Double>(movie.f0, rating == null ? -1 : rating.points;
  }
}

DataSet<Tuple2<String, String>> movies = // [...]
DataSet<Rating> ratings = // [...]
DataSet<Tuple2<String, Integer>>
            moviesWithPoints =
            movies.leftOuterJoin(ratings)

                   // key of the first input
                   .where("f0")

                   // key of the second input
                   .equalTo("name")

                   // applying the JoinFunction on joining pairs
                   .with(new PointAssigner());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
case class Rating(name: String, category: String, points: Int)

val movies: DataSet[(String, String)] = // [...]
val ratings: DataSet[Ratings] = // [...]

val moviesWithPoints = movies.leftOuterJoin(ratings).where(0).equalTo("name") {
  (movie, rating) => (movie._1, if (rating == null) -1 else rating.points)
}
```

{{< /tab >}}
{{< /tabs >}}

#### OuterJoin with Flat-Join Function

Analogous to Map and FlatMap, an OuterJoin with flat-join function behaves in the same
way as an OuterJoin with join function, but instead of returning one element, it can
return (collect), zero, one, or more elements.

{{< tabs "a00c96d4-2fa2-4531-927c-18001fd99002" >}}
{{< tab "Java" >}}

```java
public class PointAssigner
         implements FlatJoinFunction<Tuple2<String, String>, Rating, Tuple2<String, Integer>> {
  @Override
  public void join(Tuple2<String, String> movie, Rating rating,
    Collector<Tuple2<String, Integer>> out) {
  if (rating == null ) {
    out.collect(new Tuple2<String, Integer>(movie.f0, -1));
  } else if (rating.points < 10) {
    out.collect(new Tuple2<String, Integer>(movie.f0, rating.points));
  } else {
    // do not emit
  }
}

DataSet<Tuple2<String, Integer>>
            moviesWithPoints =
            movies.leftOuterJoin(ratings) // [...]
```

{{< /tab >}}
{{< tab "Scala" >}}
Not supported.
{{< /tab >}}
{{< /tabs >}}

#### Join Algorithm Hints

The Flink runtime can execute outer joins in various ways. Each possible way outperforms the others under
different circumstances. The system tries to pick a reasonable way automatically, but allows you
to manually pick a strategy, in case you want to enforce a specific way of executing the outer join.

{{< tabs "fbfb3a6e-a27f-4578-b838-df2180c8e19d" >}}
{{< tab "Java" >}}

```java
DataSet<SomeType> input1 = // [...]
DataSet<AnotherType> input2 = // [...]

DataSet<Tuple2<SomeType, AnotherType> result1 =
      input1.leftOuterJoin(input2, JoinHint.REPARTITION_SORT_MERGE)
            .where("id").equalTo("key");

DataSet<Tuple2<SomeType, AnotherType> result2 =
      input1.rightOuterJoin(input2, JoinHint.BROADCAST_HASH_FIRST)
            .where("id").equalTo("key");
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input1: DataSet[SomeType] = // [...]
val input2: DataSet[AnotherType] = // [...]

// hint that the second DataSet is very small
val result1 = input1.leftOuterJoin(input2, JoinHint.REPARTITION_SORT_MERGE).where("id").equalTo("key")

val result2 = input1.rightOuterJoin(input2, JoinHint.BROADCAST_HASH_FIRST).where("id").equalTo("key")

```

{{< /tab >}}
{{< /tabs >}}
The following hints are available.

* `OPTIMIZER_CHOOSES`: Equivalent to not giving a hint at all, leaves the choice to the system.

* `BROADCAST_HASH_FIRST`: Broadcasts the first input and builds a hash table from it, which is
  probed by the second input. A good strategy if the first input is very small.

* `BROADCAST_HASH_SECOND`: Broadcasts the second input and builds a hash table from it, which is
  probed by the first input. A good strategy if the second input is very small.

* `REPARTITION_HASH_FIRST`: The system partitions (shuffles) each input (unless the input is already
  partitioned) and builds a hash table from the first input. This strategy is good if the first
  input is smaller than the second, but both inputs are still large.

* `REPARTITION_HASH_SECOND`: The system partitions (shuffles) each input (unless the input is already
  partitioned) and builds a hash table from the second input. This strategy is good if the second
  input is smaller than the first, but both inputs are still large.

* `REPARTITION_SORT_MERGE`: The system partitions (shuffles) each input (unless the input is already
  partitioned) and sorts each input (unless it is already sorted). The inputs are joined by
  a streamed merge of the sorted inputs. This strategy is good if one or both of the inputs are
  already sorted.

**NOTE:** Not all execution strategies are supported by every outer join type, yet.

* `LeftOuterJoin` supports:
  * `OPTIMIZER_CHOOSES`
  * `BROADCAST_HASH_SECOND`
  * `REPARTITION_HASH_SECOND`
  * `REPARTITION_SORT_MERGE`

* `RightOuterJoin` supports:
  * `OPTIMIZER_CHOOSES`
  * `BROADCAST_HASH_FIRST`
  * `REPARTITION_HASH_FIRST`
  * `REPARTITION_SORT_MERGE`

* `FullOuterJoin` supports:
  * `OPTIMIZER_CHOOSES`
  * `REPARTITION_SORT_MERGE`


### Cross

The Cross transformation combines two DataSets into one DataSet. It builds all pairwise combinations of the elements of both input DataSets, i.e., it builds a Cartesian product.
The Cross transformation either calls a user-defined cross function on each pair of elements or outputs a Tuple2. Both modes are shown in the following.

**Note:** Cross is potentially a *very* compute-intensive operation which can challenge even large compute clusters!

#### Cross with User-Defined Function

A Cross transformation can call a user-defined cross function. A cross function receives one element of the first input and one element of the second input and returns exactly one result element.

The following code shows how to apply a Cross transformation on two DataSets using a cross function:

{{< tabs "ad2606a0-151e-4cc5-be3c-d3998439d8c0" >}}
{{< tab "Java" >}}

```java
public class Coord {
  public int id;
  public int x;
  public int y;
}

// CrossFunction computes the Euclidean distance between two Coord objects.
public class EuclideanDistComputer
         implements CrossFunction<Coord, Coord, Tuple3<Integer, Integer, Double>> {

  @Override
  public Tuple3<Integer, Integer, Double> cross(Coord c1, Coord c2) {
    // compute Euclidean distance of coordinates
    double dist = sqrt(pow(c1.x - c2.x, 2) + pow(c1.y - c2.y, 2));
    return new Tuple3<Integer, Integer, Double>(c1.id, c2.id, dist);
  }
}

DataSet<Coord> coords1 = // [...]
DataSet<Coord> coords2 = // [...]
DataSet<Tuple3<Integer, Integer, Double>>
            distances =
            coords1.cross(coords2)
                   // apply CrossFunction
                   .with(new EuclideanDistComputer());
```

#### Cross with Projection

A Cross transformation can also construct result tuples using a projection as shown here:

```java
DataSet<Tuple3<Integer, Byte, String>> input1 = // [...]
DataSet<Tuple2<Integer, Double>> input2 = // [...]
DataSet<Tuple4<Integer, Byte, Integer, Double>
            result =
            input1.cross(input2)
                  // select and reorder fields of matching tuples
                  .projectSecond(0).projectFirst(1,0).projectSecond(1);
```

The field selection in a Cross projection works the same way as in the projection of Join results.

{{< /tab >}}
{{< tab "Scala" >}}

```scala
case class Coord(id: Int, x: Int, y: Int)

val coords1: DataSet[Coord] = // [...]
val coords2: DataSet[Coord] = // [...]

val distances = coords1.cross(coords2) {
  (c1, c2) =>
    val dist = sqrt(pow(c1.x - c2.x, 2) + pow(c1.y - c2.y, 2))
    (c1.id, c2.id, dist)
}
```


{{< /tab >}}
{{< /tabs >}}
#### Cross with DataSet Size Hint

In order to guide the optimizer to pick the right execution strategy, you can hint the size of a DataSet to cross as shown here:

{{< tabs "073c8892-cbfe-4fe6-b3bd-bf406565e48b" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<Integer, String>> input1 = // [...]
DataSet<Tuple2<Integer, String>> input2 = // [...]

DataSet<Tuple4<Integer, String, Integer, String>>
            udfResult =
                  // hint that the second DataSet is very small
            input1.crossWithTiny(input2)
                  // apply any Cross function (or projection)
                  .with(new MyCrosser());

DataSet<Tuple3<Integer, Integer, String>>
            projectResult =
                  // hint that the second DataSet is very large
            input1.crossWithHuge(input2)
                  // apply a projection (or any Cross function)
                  .projectFirst(0,1).projectSecond(1);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val input1: DataSet[(Int, String)] = // [...]
val input2: DataSet[(Int, String)] = // [...]

// hint that the second DataSet is very small
val result1 = input1.crossWithTiny(input2)

// hint that the second DataSet is very large
val result1 = input1.crossWithHuge(input2)

```

{{< /tab >}}
{{< /tabs >}}
### CoGroup

The CoGroup transformation jointly processes groups of two DataSets. Both DataSets are grouped on a defined key and groups of both DataSets that share the same key are handed together to a user-defined co-group function. If for a specific key only one DataSet has a group, the co-group function is called with this group and an empty group.
A co-group function can separately iterate over the elements of both groups and return an arbitrary number of result elements.

Similar to Reduce, GroupReduce, and Join, keys can be defined using the different key-selection methods.

#### CoGroup on DataSets

{{< tabs "48677022-ad18-4a24-b5b8-b51171eba128" >}}
{{< tab "Java" >}}

The example shows how to group by Field Position Keys (Tuple DataSets only). You can do the same with Pojo-types and key expressions.

```java
// Some CoGroupFunction definition
class MyCoGrouper
         implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Double>, Double> {

  @Override
  public void coGroup(Iterable<Tuple2<String, Integer>> iVals,
                      Iterable<Tuple2<String, Double>> dVals,
                      Collector<Double> out) {

    Set<Integer> ints = new HashSet<Integer>();

    // add all Integer values in group to set
    for (Tuple2<String, Integer>> val : iVals) {
      ints.add(val.f1);
    }

    // multiply each Double value with each unique Integer values of group
    for (Tuple2<String, Double> val : dVals) {
      for (Integer i : ints) {
        out.collect(val.f1 * i);
      }
    }
  }
}

// [...]
DataSet<Tuple2<String, Integer>> iVals = // [...]
DataSet<Tuple2<String, Double>> dVals = // [...]
DataSet<Double> output = iVals.coGroup(dVals)
                         // group first DataSet on first tuple field
                         .where(0)
                         // group second DataSet on first tuple field
                         .equalTo(0)
                         // apply CoGroup function on each pair of groups
                         .with(new MyCoGrouper());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val iVals: DataSet[(String, Int)] = // [...]
val dVals: DataSet[(String, Double)] = // [...]

val output = iVals.coGroup(dVals).where(0).equalTo(0) {
  (iVals, dVals, out: Collector[Double]) =>
    val ints = iVals map { _._2 } toSet

    for (dVal <- dVals) {
      for (i <- ints) {
        out.collect(dVal._2 * i)
      }
    }
}
```

{{< /tab >}}
{{< /tabs >}}

### Union

Produces the union of two DataSets, which have to be of the same type. A union of more than two DataSets can be implemented with multiple union calls, as shown here:

{{< tabs "8e81e50d-0055-45fc-9c32-971149b2665e" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<String, Integer>> vals1 = // [...]
DataSet<Tuple2<String, Integer>> vals2 = // [...]
DataSet<Tuple2<String, Integer>> vals3 = // [...]
DataSet<Tuple2<String, Integer>> unioned = vals1.union(vals2).union(vals3);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val vals1: DataSet[(String, Int)] = // [...]
val vals2: DataSet[(String, Int)] = // [...]
val vals3: DataSet[(String, Int)] = // [...]

val unioned = vals1.union(vals2).union(vals3)
```

{{< /tab >}}
{{< /tabs >}}
### Rebalance
Evenly rebalances the parallel partitions of a DataSet to eliminate data skew.

{{< tabs "e3255bfc-4d57-435c-ae39-a15aae75691b" >}}
{{< tab "Java" >}}

```java
DataSet<String> in = // [...]
// rebalance DataSet and apply a Map transformation.
DataSet<Tuple2<String, String>> out = in.rebalance()
                                        .map(new Mapper());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val in: DataSet[String] = // [...]
// rebalance DataSet and apply a Map transformation.
val out = in.rebalance().map { ... }
```

{{< /tab >}}
{{< /tabs >}}

### Hash-Partition

Hash-partitions a DataSet on a given key.
Keys can be specified as position keys, expression keys, and key selector functions (see [Reduce examples](#reduce-on-grouped-dataset) for how to specify keys).

{{< tabs "b71f7167-56c1-4e1b-acc2-1f47c1fca0a0" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<String, Integer>> in = // [...]
// hash-partition DataSet by String value and apply a MapPartition transformation.
DataSet<Tuple2<String, String>> out = in.partitionByHash(0)
                                        .mapPartition(new PartitionMapper());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val in: DataSet[(String, Int)] = // [...]
// hash-partition DataSet by String value and apply a MapPartition transformation.
val out = in.partitionByHash(0).mapPartition { ... }
```

{{< /tab >}}
{{< /tabs >}}
### Range-Partition

Range-partitions a DataSet on a given key.
Keys can be specified as position keys, expression keys, and key selector functions (see [Reduce examples](#reduce-on-grouped-dataset) for how to specify keys).

{{< tabs "2d618ef2-4286-4536-b485-c3a8e3f49b86" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<String, Integer>> in = // [...]
// range-partition DataSet by String value and apply a MapPartition transformation.
DataSet<Tuple2<String, String>> out = in.partitionByRange(0)
                                        .mapPartition(new PartitionMapper());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val in: DataSet[(String, Int)] = // [...]
// range-partition DataSet by String value and apply a MapPartition transformation.
val out = in.partitionByRange(0).mapPartition { ... }
```

{{< /tab >}}
{{< /tabs >}}

### Sort Partition

Locally sorts all partitions of a DataSet on a specified field in a specified order.
Fields can be specified as field expressions or field positions (see [Reduce examples](#reduce-on-grouped-dataset) for how to specify keys).
Partitions can be sorted on multiple fields by chaining `sortPartition()` calls.

{{< tabs "6d24783a-75e1-44c9-a7e7-633ee75c9b47" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<String, Integer>> in = // [...]
// Locally sort partitions in ascending order on the second String field and
// in descending order on the first String field.
// Apply a MapPartition transformation on the sorted partitions.
DataSet<Tuple2<String, String>> out = in.sortPartition(1, Order.ASCENDING)
                                        .sortPartition(0, Order.DESCENDING)
                                        .mapPartition(new PartitionMapper());
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val in: DataSet[(String, Int)] = // [...]
// Locally sort partitions in ascending order on the second String field and
// in descending order on the first String field.
// Apply a MapPartition transformation on the sorted partitions.
val out = in.sortPartition(1, Order.ASCENDING)
            .sortPartition(0, Order.DESCENDING)
            .mapPartition { ... }
```

{{< /tab >}}
{{< /tabs >}}
### First-n

Returns the first n (arbitrary) elements of a DataSet. First-n can be applied on a regular DataSet, a grouped DataSet, or a grouped-sorted DataSet. Grouping keys can be specified as key-selector functions or field position keys (see [Reduce examples](#reduce-on-grouped-dataset) for how to specify keys).

{{< tabs "250a1c92-b5b0-4b34-a307-6d50e93a0508" >}}
{{< tab "Java" >}}

```java
DataSet<Tuple2<String, Integer>> in = // [...]
// Return the first five (arbitrary) elements of the DataSet
DataSet<Tuple2<String, Integer>> out1 = in.first(5);

// Return the first two (arbitrary) elements of each String group
DataSet<Tuple2<String, Integer>> out2 = in.groupBy(0)
                                          .first(2);

// Return the first three elements of each String group ordered by the Integer field
DataSet<Tuple2<String, Integer>> out3 = in.groupBy(0)
                                          .sortGroup(1, Order.ASCENDING)
                                          .first(3);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val in: DataSet[(String, Int)] = // [...]
// Return the first five (arbitrary) elements of the DataSet
val out1 = in.first(5)

// Return the first two (arbitrary) elements of each String group
val out2 = in.groupBy(0).first(2)

// Return the first three elements of each String group ordered by the Integer field
val out3 = in.groupBy(0).sortGroup(1, Order.ASCENDING).first(3)
```

{{< /tab >}}
{{< /tabs >}}

