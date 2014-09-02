---
title: "Java API Transformations"
---

<section id="top">
DataSet Transformations
-----------------------

This document gives a deep-dive into the available transformations on DataSets. For a general introduction to the
Flink Java API, please refer to the [API guide](java_api_guide.html)


### Map

The Map transformation applies a user-defined `MapFunction` on each element of a DataSet.
It implements a one-to-one mapping, that is, exactly one element must be returned by
the function.

The following code transforms a `DataSet` of Integer pairs into a `DataSet` of Integers:

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

### FlatMap

The FlatMap transformation applies a user-defined `FlatMapFunction` on each element of a `DataSet`.
This variant of a map function can return arbitrary many result elements (including none) for each input element.

The following code transforms a `DataSet` of text lines into a `DataSet` of words:

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

### MapPartition

The MapPartition function transforms a parallel partition in a single function call. The function get the partition as an `Iterable` stream and
can produce an arbitrary number of result values. The number of elements in each partition depends on the degree-of-parallelism
and previous operations.

The following code transforms a `DataSet` of text lines into a `DataSet` of counts per partition:

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

### Filter

The Filter transformation applies a user-defined `FilterFunction` on each element of a `DataSet` and retains only those elements for which the function returns `true`.

The following code removes all Integers smaller than zero from a `DataSet`:

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

### Project (Tuple DataSets only)

The Project transformation removes or moves `Tuple` fields of a `Tuple` `DataSet`.
The `project(int...)` method selects `Tuple` fields that should be retained by their index and defines their order in the output `Tuple`.
The `types(Class<?> ...)`method must give the types of the output `Tuple` fields.

Projections do not require the definition of a user function.

The following code shows different ways to apply a Project transformation on a `DataSet`:

```java
DataSet<Tuple3<Integer, Double, String>> in = // [...]
// converts Tuple3<Integer, Double, String> into Tuple2<String, Integer>
DataSet<Tuple2<String, Integer>> out = in.project(2,0).types(String.class, Integer.class);
```

### Transformations on grouped DataSet

The reduce operations can operate on grouped data sets. Specifying the key to
be used for grouping can be done in two ways:

- a `KeySelector` function or
- one or more field position keys (`Tuple` `DataSet` only).

Please look at the reduce examples to see how the grouping keys are specified.

### Reduce on grouped DataSet

A Reduce transformation that is applied on a grouped `DataSet` reduces each group to a single element using a user-defined `ReduceFunction`.
For each group of input elements, a `ReduceFunction` successively combines pairs of elements into one element until only a single element for each group remains.

#### Reduce on DataSet grouped by KeySelector Function

A `KeySelector` function extracts a key value from each element of a `DataSet`. The extracted key value is used to group the `DataSet`.
The following code shows how to group a POJO `DataSet` using a `KeySelector` function and to reduce it with a `ReduceFunction`.

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
                         // DataSet grouping with inline-defined KeySelector function
                         .groupBy(
                           new KeySelector<WC, String>() {
                             public String getKey(WC wc) { return wc.word; }
                           })
                         // apply ReduceFunction on grouped DataSet
                         .reduce(new WordCounter());
```

#### Reduce on DataSet grouped by Field Position Keys (Tuple DataSets only)

Field position keys specify one or more fields of a `Tuple` `DataSet` that are used as grouping keys.
The following code shows how to use field position keys and apply a `ReduceFunction`.

```java
DataSet<Tuple3<String, Integer, Double>> tuples = // [...]
DataSet<Tuple3<String, Integer, Double>> reducedTuples =
                                         tuples
                                         // group DataSet on first and second field of Tuple
                                         .groupBy(0,1)
                                         // apply ReduceFunction on grouped DataSet
                                         .reduce(new MyTupleReducer());
```

### GroupReduce on grouped DataSet

A GroupReduce transformation that is applied on a grouped `DataSet` calls a user-defined `GroupReduceFunction` for each group. The difference
between this and `Reduce` is that the user defined function gets the whole group at once.
The function is invoked with an Iterable over all elements of a group and can return an arbitrary number of result elements using the collector.

#### GroupReduce on DataSet grouped by Field Position Keys (Tuple DataSets only)

The following code shows how duplicate strings can be removed from a `DataSet` grouped by Integer.

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

#### GroupReduce on DataSet grouped by KeySelector Function

Works analogous to `KeySelector` functions in Reduce transformations.

#### GroupReduce on sorted groups (Tuple DataSets only)

A `GroupReduceFunction` accesses the elements of a group using an Iterable. Optionally, the Iterable can hand out the elements of a group in a specified order. In many cases this can help to reduce the complexity of a user-defined `GroupReduceFunction` and improve its efficiency.
Right now, this feature is only available for DataSets of Tuples.

The following code shows another example how to remove duplicate Strings in a `DataSet` grouped by an Integer and sorted by String.

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
      if (com == null || !next.equals(comp)) {
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

**Note:** A GroupSort often comes for free if the grouping is established using a sort-based execution strategy of an operator before the reduce operation.

#### Combinable GroupReduceFunctions

In contrast to a `ReduceFunction`, a `GroupReduceFunction` is not
necessarily combinable. In order to make a `GroupReduceFunction`
combinable, you need to use the `RichGroupReduceFunction` variant,
implement (override) the `combine()` method, and annotate the
`GroupReduceFunction` with the `@Combinable` annotation as shown here:

```java
// Combinable GroupReduceFunction that computes two sums.
// Note that we use the RichGroupReduceFunction because it defines the combine method
@Combinable
public class MyCombinableGroupReducer
         extends RichGroupReduceFunction<Tuple3<String, Integer, Double>,
                                     Tuple3<String, Integer, Double>> {
  @Override
  public void reduce(Iterable<Tuple3<String, Integer, Double>> in,
                     Collector<Tuple3<String, Integer, Double>> out) {

    String key = null
    int intSum = 0;
    double doubleSum = 0.0;

    for (Tuple3<String, Integer, Double> curr : in) {
      key = curr.f0;
      intSum += curr.f1;
      doubleSum += curr.f2;
    }
    // emit a tuple with both sums
    out.collect(new Tuple3<String, Integer, Double>(key, intSum, doubleSum));
  }

  @Override
  public void combine(Iterable<Tuple3<String, Integer, Double>> in,
                      Collector<Tuple3<String, Integer, Double>> out)) {
    // in some cases combine() calls can simply be forwarded to reduce().
    this.reduce(in, out);
  }
}
```

### Aggregate on grouped Tuple DataSet

There are some common aggregation operations that are frequently used. The Aggregate transformation provides the following build-in aggregation functions:

- Sum,
- Min, and
- Max.

The Aggregate transformation can only be applied on a `Tuple` `DataSet` and supports only field positions keys for grouping.

The following code shows how to apply an Aggregation transformation on a `DataSet` grouped by field position keys:

```java
DataSet<Tuple3<Integer, String, Double>> input = // [...]
DataSet<Tuple3<Integer, String, Double>> output = input
                                   .groupBy(1)        // group DataSet on second field
                                   .aggregate(SUM, 0) // compute sum of the first field
                                   .and(MIN, 2);      // compute minimum of the third field
```

To apply multiple aggregations on a DataSet it is necessary to use the `.and()` function after the first aggregate, that means `.aggregate(SUM, 0).and(MIN, 2)` produces the sum of field 0 and the minimum of field 2 of the original DataSet. 
In contrast to that `.aggregate(SUM, 0).aggregate(MIN, 2)` will apply an aggregation on an aggregation. In the given example it would produce the minimum of field 2 after calculating the sum of field 0 grouped by field 1.

**Note:** The set of aggregation functions will be extended in the future.

### Reduce on full DataSet

The Reduce transformation applies a user-defined `ReduceFunction` to all elements of a `DataSet`.
The `ReduceFunction` subsequently combines pairs of elements into one element until only a single element remains.

The following code shows how to sum all elements of an Integer `DataSet`:

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

Reducing a full `DataSet` using the Reduce transformation implies that the final Reduce operation cannot be done in parallel. However, a `ReduceFunction` is automatically combinable such that a Reduce transformation does not limit scalability for most use cases.

### GroupReduce on full DataSet

The GroupReduce transformation applies a user-defined `GroupReduceFunction` on all elements of a `DataSet`.
A `GroupReduceFunction` can iterate over all elements of `DataSet` and return an arbitrary number of result elements.

The following example shows how to apply a GroupReduce transformation on a full `DataSet`:

```java
DataSet<Integer> input = // [...]
// apply a (preferably combinable) GroupReduceFunction to a DataSet
DataSet<Double> output = input.reduceGroup(new MyGroupReducer());
```

**Note:** A GroupReduce transformation on a full `DataSet` cannot be done in parallel if the `GroupReduceFunction` is not combinable. Therefore, this can be a very compute intensive operation. See the paragraph on "Combineable `GroupReduceFunction`s" above to learn how to implement a combinable `GroupReduceFunction`.

### Aggregate on full Tuple DataSet

There are some common aggregation operations that are frequently used. The Aggregate transformation provides the following build-in aggregation functions:

- Sum,
- Min, and
- Max.

The Aggregate transformation can only be applied on a `Tuple` `DataSet`.

The following code shows how to apply an Aggregation transformation on a full `DataSet`:

```java
DataSet<Tuple2<Integer, Double>> input = // [...]
DataSet<Tuple2<Integer, Double>> output = input
                                     .aggregate(SUM, 0)    // compute sum of the first field
                                     .and(MIN, 1);    // compute minimum of the second field
```

**Note:** Extending the set of supported aggregation functions is on our roadmap.

### Join

The Join transformation joins two `DataSet`s into one `DataSet`. The elements of both `DataSet`s are joined on one or more keys which can be specified using

- a `KeySelector` function or
- one or more field position keys (`Tuple` `DataSet` only).

There are a few different ways to perform a Join transformation which are shown in the following.

#### Default Join (Join into Tuple2)

The default Join transformation produces a new `Tuple``DataSet` with two fields. Each tuple holds a joined element of the first input `DataSet` in the first tuple field and a matching element of the second input `DataSet` in the second field.

The following code shows a default Join transformation using field position keys:

```java
DataSet<Tuple2<Integer, String>> input1 = // [...]
DataSet<Tuple2<Double, Integer>> input2 = // [...]
// result dataset is typed as Tuple2
DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Double, Integer>>>
            result = input1.join(input2)
                           .where(0)       // key of the first input
                           .equalTo(1);    // key of the second input
```

#### Join with JoinFunction

A Join transformation can also call a user-defined `JoinFunction` to process joining tuples.
A `JoinFunction` receives one element of the first input `DataSet` and one element of the second input `DataSet` and returns exactly one element.

The following code performs a join of `DataSet` with custom java objects and a `Tuple` `DataSet` using `KeySelector` functions and shows how to call a user-defined `JoinFunction`:

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
                   .where(new KeySelection<Rating, String>() {
                            public String getKey(Rating r) { return r.category; }
                          })

                   // key of the second input
                   .equalTo(new KeySelection<Tuple2<String, Double>, String>() {
                              public String getKey(Tuple2<String, Double> t) { return t.f0; }
                            })

                   // applying the JoinFunction on joining pairs
                   .with(new PointWeighter());
```

#### Join with FlatJoinFunction

Analogous to Map and FlatMap, a FlatJoin function behaves in the same
way as a JoinFunction, but instead of returning one element, it can
return (collect), zero, one, or more elements.
{% highlight java %}
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
{% endhighlight %}

#### Join with Projection

A Join transformation can construct result tuples using a projection as shown here:

```java
DataSet<Tuple3<Integer, Byte, String>> input1 = // [...]
DataSet<Tuple2<Integer, Double>> input2 = // [...]
DataSet<Tuple4<Integer, String, Double, Byte>
            result =
            input1.join(input2)
                  // key definition on first DataSet using a field position key
                  .where(0)
                  // key definition of second DataSet using a field position key
                  .equalTo(0)
                  // select and reorder fields of matching tuples
                  .projectFirst(0,2).projectSecond(1).projectFirst(1)
                  .types(Integer.class, String.class, Double.class, Byte.class);
```

`projectFirst(int...)` and `projectSecond(int...)` select the fields of the first and second joined input that should be assembled into an output `Tuple`. The order of indexes defines the order of fields in the output tuple.
The join projection works also for non-`Tuple` `DataSet`s. In this case, `projectFirst()` or `projectSecond()` must be called without arguments to add a joined element to the output `Tuple`.

#### Join with DataSet Size Hint

In order to guide the optimizer to pick the right execution strategy, you can hint the size of a `DataSet` to join as shown here:

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

### Cross

The Cross transformation combines two `DataSet`s into one `DataSet`. It builds all pairwise combinations of the elements of both input `DataSet`s, i.e., it builds a Cartesian product.
The Cross transformation either calls a user-defined `CrossFunction` on each pair of elements or applies a projection. Both modes are shown in the following.

**Note:** Cross is potentially a *very* compute-intensive operation which can challenge even large compute clusters!

#### Cross with User-Defined Function

A Cross transformation can call a user-defined `CrossFunction`. A `CrossFunction` receives one element of the first input and one element of the second input and returns exactly one result element.

The following code shows how to apply a Cross transformation on two `DataSet`s using a `CrossFunction`:

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
                  .projectSecond(0).projectFirst(1,0).projectSecond(1)
                  .types(Integer.class, Byte.class, Integer.class, Double.class);
```

The field selection in a Cross projection works the same way as in the projection of Join results.

#### Cross with DataSet Size Hint

In order to guide the optimizer to pick the right execution strategy, you can hint the size of a `DataSet` to cross as shown here:

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
                  .projectFirst(0,1).projectSecond(1).types(Integer.class, String.class, String.class)
```

### CoGroup

The CoGroup transformation jointly processes groups of two `DataSet`s. Both `DataSet`s are grouped on a defined key and groups of both `DataSet`s that share the same key are handed together to a user-defined `CoGroupFunction`. If for a specific key only one `DataSet` has a group, the `CoGroupFunction` is called with this group and an empty group.
A `CoGroupFunction` can separately iterate over the elements of both groups and return an arbitrary number of result elements.

Similar to Reduce, GroupReduce, and Join, keys can be defined using

- a `KeySelector` function or
- one or more field position keys (`Tuple` `DataSet` only).

#### CoGroup on DataSets grouped by Field Position Keys (Tuple DataSets only)

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
    for (Tuple2<String, Integer>> val : iVale) {
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

#### CoGroup on DataSets grouped by Key Selector Function

Works analogous to key selector functions in Join transformations.

### Union

Produces the union of two `DataSet`s, which have to be of the same type. A union of more than two `DataSet`s can be implemented with multiple union calls, as shown here:

```java
DataSet<Tuple2<String, Integer>> vals1 = // [...]
DataSet<Tuple2<String, Integer>> vals2 = // [...]
DataSet<Tuple2<String, Integer>> vals3 = // [...]
DataSet<Tuple2<String, Integer>> unioned = vals1.union(vals2)
                    .union(vals3);
```


[Back to top](#top)
