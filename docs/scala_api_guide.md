---
title: "Scala API Programming Guide"
---

<section id="top">
Scala Programming Guide
=======================

This guide explains how to develop Stratosphere programs with the Scala
programming interface. It assumes you are familiar with the general concepts of
Stratosphere's [Programming Model](pmodel.html "Programming Model"). We
recommend to learn about the basic concepts first, before continuing with the
[Java](java.html "Java Programming Guide") or this Scala programming guide.

Here we will look at the general structure of a Scala job. You will learn how to
write data sources, data sinks, and operators to create data flows that can be
executed using the Stratosphere system.

Writing Scala jobs requires an understanding of Scala, there is excellent
documentation available [here](http://scala-lang.org/documentation/). Most
of the examples can be understood by someone with a good understanding
of programming in general, though.

[Back to top](#top)

<section id="intro-example">
Word Count Example
------------------

To start, let's look at a Word Count job implemented in Scala. This program is
very simple but it will give you a basic idea of what a Scala job looks like.

```scala
import eu.stratosphere.client.LocalExecutor

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

object WordCount {
  def main(args: Array[String]) {
    val input = TextFile(textInput)

    val words = input.flatMap { _.split(" ") map { (_, 1) } }

    val counts = words.groupBy { case (word, _) => word }
      .reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    val output = counts.write(wordsOutput, CsvOutputFormat())
    val plan = new ScalaPlan(Seq(output))

    LocalExecutor.execute(plan)
  }
}
``` 

Same as any Stratosphere job a Scala job consists of one or several data
sources, one or several data sinks and operators in between these that transform
data. Together these parts are referred to as the data flow graph. It dictates
the way data is passed when a job is executed.

When using Scala in Stratosphere an important concept to grasp is that of the
`DataSet`. `DataSet` is an abstract concept that represents actual data sets at
runtime and which has operations that transform data to create a new transformed
data set. In this example the `TextFile("/some/input")` call creates a
`DataSet[String]` that represents the lines of text from the input. The
`flatMap` operation that looks like a regular Scala flatMap is in fact an
operation on `DataSet` that passes (at runtime) the data items through the
provided anonymous function to transform them. The result of the `flatMap`
operation is a new `DataSet` that represents the transformed data. On this other
operations be performed. Another such operation are `groupBy` and `reduce`, but
we will go into details of those later in this guide.

The `write` operation of `DataSet` is used to create a data sink. You provide it
with a path where the data is to be written to and an output format. This is
enough for now but we will discuss data formats (for sources and sinks) later.

To execute a data flow graph one or several sinks have to wrapped in a `Plan`
which can then be executed on a cluster using `RemoteExecutor`. Here, the
`LocalExecutor` is used to run the flow on the local computer. This is useful
for debugging your job before running it on an actual cluster.

[Back to top](#top)

<section id="intro-example">
Project Setup
-------------

We will only cover maven here but the concepts should work equivalently with
other build systems such as Gradle or sbt. When wanting to develop a Scala job
all that is needed as dependency is is `stratosphere-scala` (and `stratosphere-clients`, if
you want to execute your jobs). So all that needs to be done is to add the
following lines to your POM.


```xml
<dependencies>
  <dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>stratosphere-scala</artifactId>
    <version>{{site.current_stable}}</version>
  </dependency>
  <dependency>
    <groupId>eu.stratosphere</groupId>
    <artifactId>stratosphere-clients</artifactId>
    <version>{{site.current_stable}}</version>
  </dependency>
</dependencies>
```

To quickly get started you can use the Stratosphere Scala quickstart available
[here]({{site.baseurl}}/quickstart/scala.html). This will give you a
completeMaven project with some working example code that you can use to explore
the system or as basis for your own projects.

These imports are normally enough for any project:

```scala
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.client.RemoteExecutor
```

The first two imports contain things like `DataSet`, `Plan`, data sources, data
sinks, and the operations. The last two imports are required if you want to run
a data flow on your local machine, respectively cluster.

[Back to top](#top)

<section id="dataset">
The DataSet Abstraction
-----------------------

As already alluded to in the introductory example you write Scala jobs by using
operations on a `DataSet` to create new transformed `DataSet`. This concept is
the core of the Stratosphere Scala API so it merits some more explanation. A
`DataSet` can look and behave like a regular Scala collection in your code but
it does not contain any actual data but only represents data. For example: when
you use `TextFile()` you get back a `DataSource[String]` that represents each
line of text in the input as a `String`. No data is actually loaded or available
at this point. The set is only used to apply further operations which themselves
are not executed until the data flow is executed. An operation on `DataSet`
creates a new `DataSet` that represents the transformation and has a pointer to
the `DataSet` that represents the data to be transformed. In this way a tree of
data sets is created that contains both the specification of the flow of data as
well as all the transformations. This graph can be wrapped in a `Plan` and
executed.

Working with the system is like working with lazy collections, where execution
is postponed until the user submits the job.

`DataSet` has a generic parameter, this is the type of each data item or record
that would be processed by further transformations. This is similar to how
`List[A]` in Scala would behave. For example in:

```scala
val input: DataSet[(String, Int)] = ...
val mapped = input map { a => (a._1, a._2 + 1)}
```

The anonymous function would retrieve in `a` tuples of type `(String, Int)`.

[Back to top](#top)

<section id="datatypes">
Data Types
----------

There are some restrictions regarding the data types that can be used in Scala
jobs (basically the generic parameter of `DataSet`). The usable types are
the primitive Scala types, case classes (which includes tuples), and custom
data types.

Custom data types must implement the interface
[Value](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-core/src/main/java/eu/stratosphere/types/Value.java).
For custom data types that should also be used as a grouping key or join key
the [Key](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-core/src/main/java/eu/stratosphere/types/Key.java)
interface must be implemented.

[Back to top](#top)

<section id="data-sources">
Creating Data Sources
---------------------

To get an initial `DataSet` on which to perform operations to build a data flow
graph the following construct is used:

```scala
val input = DataSource("<file-path>", <input-format>)
```

The value `input` is now a `DataSet` with the generic type depending on the
input format.

The file path can be on of either `file:///some/file` to acces files on the
local machine or `hdfs://some/path` to read files from HDFS. The input
format can be one of our builtin formats or a custom input format. The builtin
formats are:

* [TextInputFormat](#text-input-format)
* [CsvInputFormat](#csv-input-format)
* [DelimitedInputFormat](#delimited-input-format)
* [BinaryInputFormat](#binary-input-format)
* [BinarySerializedInputFormat](#binary-serialized-input-format)
* [FixedLengthInputFormat](#fixed-length-input-format)

We will now have a look at each of them and show how they are employed and in
which situations.

[Back to top](#top)

<section id="text-input-format">
#### TextInputFormat

This input format simply reads a text file line wise and creates a `String`
for each line. It is used as:

```scala
TextInputFormat()
```

As you have already seen in the Word Count Example there is a shortcut for this.
Instead of using a `DataSource` with `TextInputFormat` you can simply write:

```scala
val input = TextFile("<file-path>")
```

The `input` would then be a `DataSet[String]`.

[Back to top](#top)

<section id="csv-input-format">
#### CsvInputFormat

This input format is mainly used to read Csv-Files, as the name suggests. Input
files must be text files. You can specify the `String` that should be used
as the separator between individual records (this would often be newline) and
also the separator between fields of a record (this would often be a comma).
The `CsvInputFormat` will automatically read the records and create
Scala tuples or custom case class objects for you. The format can be used
in one of the following ways:

```scala
CsvInputFormat[Out]()
CsvInputFormat[Out](recordDelim: String)
CsvInputFormat[Out](recordDelim: String, fieldDelim: Char)

CsvInputFormat[Out](fieldIndices: Seq[Int])
CsvInputFormat[Out](fieldIndices: Seq[Int], recordDelim: String)
CsvInputFormat[Out](fieldIndices: Seq[Int], recordDelim: String, fieldDelim: Char)
```

The default record delimiter is a newline, the default field delimiter is a
comma. The type parameter `Out` must be a case class type, which also includes
tuple types since they are internally case classes.

Normally, all the fields of a record are read. If you want to explicitly
specify which fields of the record should be read you can use one of the
tree variants with a `fieldIndices` parameter. Here you give a list
of the fields that should be read. Field indices start from zero.

An example usage could look as follows:

```scala
val input = DataSource("file:///some/file", CsvInputFormat[(Int, Int, String)](Seq(1, 17, 42), "\n", ','))
```

Here only the specified fields would be read and 3-tuples created for you.
The type of input would be `DataSet[(Int, Int, String)]`.

[Back to top](#top)

<section id="delimited-input-format">
#### DelimitedInputFormat

This input format is meant for textual records that are separated by
some delimiter. The delimiter could be a newline, for example. It is used like
this:

```scala
DelimitedInputFormat[Out](parseFunction: String => Out, delim: String = "\n")
```

The input files will be split on the supplied delimiter (or the default newline)
and the supplied parse function must parse the textual representation in the
`String` and return an object. The type of this object will then also be the
type of the `DataSet` created by the `DataSource` operation.

Just as with `BinaryInputFormat` the function can be an anonymous function, so
you could have:

```scala
val input = DataSource("file:///some/file", BinaryInputFormat( { line =>
  line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }
}))
```

In this example EdgeInputPattern is some regular expression used for parsing
a line of text and `Path` is a custom case class that is used to represent
the data. The type of input would in this case be `DataSet[Path]`.

[Back to top](#top)

<section id="binary-input-format">
#### BinaryInputFormat

This input format is best used when you have a custom binary format that
you store the data in. It is created using one of the following:

```scala
BinaryInputFormat[Out](readFunction: DataInput => Out)
BinaryInputFormat[Out](readFunction: DataInput => Out, blocksize: Long)
```

So you have to provide a function that gets a
[java.io.DataInput](http://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html)
and returns the object that
contains the data. The type of this object will then also be the type of the
`DataSet` created by the `DataSource` operation.

The provided function can also be an anonymous function, so you could
have something like this:

```scala
val input = DataSource("file:///some/file", BinaryInputFormat( { input =>
  val one = input.readInt
  val two = input.readDouble
  (one, two)  
}))
```

Here `input` would be of type `DataSet[(Int, Double)]`.

[Back to top](#top)

<section id="binary-serialized-input-format">
#### BinarySerializedInputFormat

This input format is only meant to be used in conjunction with
`BinarySerializedOutputFormat`. You can use these to write elements to files using a
Stratosphere-internal format that can efficiently be read again. You should only
use this when output is only meant to be consumed by other Stratosphere jobs.
The format can be used on one of two ways:

```scala
BinarySerializedInputFormat[Out]()
BinarySerializedInputFormat[Out](blocksize: Long)
```

So if input files contain elements of type `(String, Int)` (a tuple type) you
could use:

```scala
val input = DataSource("file:///some/file", BinarySerializedInputFormat[(String, Int)]())
```
[Back to top](#top)

<section id="fixed-length-input-format">
#### FixedLengthInputFormat

This input format is for cases where you want to read binary blocks
of a fixed size. The size of a block must be specified and you must
provide code that reads elements from a byte array.

The format is used like this:

```scala
FixedLengthInputFormat[Out](readFunction: (Array[Byte], Int) => Out, recordLength: Int)
```

The specified function gets an array and a position at which it must start
reading the array and returns the element read from the binary data.

[Back to top](#top)

<section id="operations">
Operations on DataSet
---------------------

As explained in [Programming Model](pmodel.html#operators),
a Stratosphere job is a graph of operators that process data coming from
sources that is finally written to sinks. When you use the Scala front end
these operators as well as the graph is created behind the scenes. For example,
when you write code like this:

```scala
val input = TextFile("file:///some/file")

val words = input.map { x => (x, 1) }

val output = counts.write(words, CsvOutputFormat()))

val plan = new ScalaPlan(Seq(output))
```

What you get is a graph that has a data source, a map operator (that contains
the code written inside the anonymous function block), and a data sink. You 
do not have to know about this to be able to use the Scala front end but
it helps to remember, that when you are using Scala you are building
a data flow graph that processes data only when executed.

There are operations on `DataSet` that correspond to all the types of operators
that the Stratosphere system supports. We will shortly go trough all of them with
some examples.

[Back to top](#top)

<section id="operator-templates">
#### Basic Operator Templates

Most of the operations have three similar versions and we will
explain them here for all of the operators together. The three versions are `map`,
`flatMap`, and `filter`. All of them accept an anonymous function that
defines what the operation does but the semantics are different.

The `map` version is a simple one to one mapping. Take a look at the following
code:

```scala
val input: DataSet[(String, Int)]

val mapped = input.map { x => (x._1, x._2 + 3) }
```

This defines a map operator that operates on tuples of String and Int and just
adds three to the Int (the second fields of the tuple). So, if the input set had
the tuples (a, 1), (b, 2), and (c, 3) the result after the operator would be
(a, 4), (b, 5), and (c, 6).

The `flatMap` version works a bit differently,
here you return something iterable from the anonymous function. The iterable
could be a list or an array. The elements in this iterable are unnested.
So for every element in the input data you get a list of elements. The
concatenation of those is the result of the operator. If you had
the following code:

```scala
val input: DataSet[(String, Int)]

val mapped = input.flatMap { x => List( (x._1, x._2), (x._1, x._2 + 1) ) }
```

and as input the tuples (a, 1) and (b, 1) you would get (a, 1), (a, 2), (b, 1),
and (b, 2) as result. It is one flat list, and not the individual lists returned
from the anonymous function.

The third template is `filter`. Here you give an anonymous function that
returns a Boolean. The elements for which this Boolean is true are part of the
result of the operation, the others are culled. An example for a filter is this
code:


```scala
val input: DataSet[(String, Int)]

val mapped = input.filter { x => x._2 >= 3 }
```
[Back to top](#top)

<section id="key-selectors">
#### Field/Key Selectors

For some operations (group, join, and cogroup) it is necessary to specify which
parts of a data type are to be considered the key. This key is used for grouping
elements together for reduce and for joining in case of a join or cogroup.
In Scala the key is specified using a special anonymous function called
a key selector. The key selector has as input an element of the type of
the `DataSet` and must return a single value or a tuple of values that should
be considered the key. This will become clear with some examples: (Note that
we use the reduce operation here as an example, we will have a look at
that further down):

```scala
val input: DataSet[(String, Int)]
val reduced = input groupBy { x => (x._1) } reduce { ... }
val reduced2 = input groupBy { case (w, c) => w } reduce { ... }

case class Test(a: String, b: Int, c: Int)
val input2: DataSet[Test]
val reduced3 = input2 groupBy { x => (x.a, x.b) } reduce { ... }
val reduced4 = input2 groupBy { case Test(x,y,z) => (x,y) } reduce { ... }
```

The anonymous function block passed to `groupBy` is the key selector. The first
two examples both specify the `String` field of the tuple as key. In the second
set of examples we see a custom case class and here we select the first two
fields as a compound key.

It is worth noting that the key selector function is not actually executed 
at runtime but it is parsed at job creation time where the key information is
extracted and stored for efficient computation at runtime.

#### Map Operation

Map is an operation that gets one element at a time and can output one or
several elements. The operations that result in a `MapOperator` in the graph are exactly
those mentioned in the previous section. For completeness' sake we will mention
their signatures here (in this and the following such lists `In` is the
type of the input data set, `DataSet[In]`):

```scala
def map[Out](fun: In => Out): DataSet[Out]
def flatMap[Out](fun: In => Iterator[Out]): DataSet[Out]
def filter(fun: In => Boolean): DataSet[Out]
```

#### Reduce Operation

As explained [here](pmodel.html#operators) Reduce is an operation that looks
at groups of elements at a time and can, for one group, output one or several
elements. To specify how elements should be grouped you need to give
a key selection function, as explained [above](#key-selectors).

The basic template of the reduce operation is:

```scala
input groupBy { <key selector> } reduce { <reduce function> }
```

The signature of the reduce function depends on the variety of reduce operation
selected. There are right now three different versions:

```scala
def reduce(fun: (In, In) => In): DataSet[In]

def reduceGroup[Out](fun: Iterator[In] => Out): DataSet[Out]
def combinableReduceGroup(fun: Iterator[In] => In): DataSet[In]
```

The `reduce` variant is like a `reduceLeft` on a Scala collection with
the limitation that the output data type must be the same as the input data
type. You specify how to elements of the selection should be combined,
this is then used to reduce the elements in one group (of the same key)
down to one element. This can be used to implement aggregation operators,
for example:

```scala
val words: DataSet[(String, Int)]
val counts = words.groupBy { case (word, count) => word}
  .reduce { (w1, w1) => (w1._1, w1._2 + w2._2) }
```

This would add up the Int fields of those tuples that have the same String
in the first fields. As is for example required in Word Count.

The `reduceGroup` variant can be used when more control is required. Here
your reduce function gets an `Iterator` that can be used to iterate over
all the elements in a group. With this type or reduce operation the
output data type can be different from the input data type. An example
of this kind of operation is this:

```scala
val words: DataSet[(String, Int)]
val minCounts = words.groupBy { case (word, count) => word}
  .reduceGroup { words => words.minBy { _._2 } }
```

Here we use the minBy function of Scala collections to determine the
element with the minimum count in a group.

The `combinableGroupReduce` works like the `groupReduce` with the difference
that the reduce operation is combinable. This is an optimization one can use,
please have a look at [Programming Model](pmodel.html "Programming Model") for
the details.

#### Join Operation

The join operation is similar to a database equi-join. It is a two input
iteration where you have to specify a key selector for each of the inputs
and then the anonymous function is called for every pair of matching
elements from the two input sides.

The basic template is:

```scala
input1 join input2 where { <key selector 1> } isEqualTo { <key selector 2>} map { <join function> }
```

or, because lines will get to long fast:
```scala
input1.join(input2)
  .where { <key selector 1> }
  .isEqualTo { <key selector 2>}
  .map { <join function> }
```

(Scala can sometimes be quite finicky about where you can omit dots and
parentheses, so it's best to use dots in multi-line code like this.)

As mentioned in [here](#operator-templates) there are three versions of
this operator, so you can use one of these in the last position:

```scala
def map[Out](fun: (LeftIn, RightIn) => Out): DataSet[Out]
def flatMap[Out](fun: (LeftIn, RightIn) => Iterator[Out]): DataSet[Out]
def filter(fun: (LeftIn, RightIn) => Boolean): DataSet[(LeftIn, RightIn)]
```

One example where this can be used is database-style joining with projection:

```scala
input1.join(input2)
  .where { case (a, b, c) => (a, b) }
  .isEqualTo { case (a, b, c, d) => (c, d) }
  .map { (left, right) => (left._3, right._1) }
```

Here the join key for the left input is a compound of the first two tuple fields
while the key for the second input is a compound of the last two fields. We then
pick one field each from both sides as the result of the operation.

#### CoGroup Operation

The cogroup operation is a cross between join and reduce. It has two inputs
and you have to specify a key selector for each of them. This is where the
similarities with join stop. Instead of having one invocation of your user
code per pair of matching elements all elements from the left and from the right
are grouped together for one single invocation. In your function you get
an `Iterator` for the elements from the left input and another `Iterator`
for the elements from the right input.

The basic template is:

```scala
input1 cogroup input2 where { <key selector 1> } isEqualTo { <key selector 2>} map { <cogroup function> }
```

or, because lines will get to long fast:
```scala
input1.cogroup(input2)
  .where { <key selector 1> }
  .isEqualTo { <key selector 2>}
  .map { <cogroup function> }
```

There are to variants you can use, with the semantics explained
[here](#operator-templates).

```scala
def map[Out](fun: (Iterator[LeftIn], Iterator[RightIn]) => Out): DataSet[Out]
def flatMap[Out](fun: (Iterator[LeftIn], Iterator[RightIn]) => Iterator[Out]): DataSet[Out]
```

#### Cross Operation

The cross operation is used to form the Cartesian product of the elements
from two inputs. The basic template is:

```scala
input1 cross input2 map { <cogroup function> }
```

Again there are three variants, with the semantics explained
[here](#operator-templates).

```scala
def map[Out](fun: (LeftIn, RightIn) => Out): DataSet[Out]
def flatMap[Out](fun: (LeftIn, RightIn) => Iterator[Out]): DataSet[Out]
def filter(fun: (LeftIn, RightIn) => Boolean): DataSet[(LeftIn, RightIn)]
```

#### Union

When you want to have the combination of several data sets as the input of
an operation you can use a union to combine them. It is used like this

```scala
val input1: DataSet[String]
val input2: DataSet[String]
val unioned = input1.union(input2)
```

The signature of union is:

```scala
def union(secondInput: DataSet[A])
```

Where `A` is the generic type of the `DataSet` on which you execute the `union`.

[Back to top](#top)

<section id="iterations">
Iterations
----------

Iterations allow you to implement *loops* in Stratosphere programs.
[This page](iterations.html) gives a
general introduction to iterations. This section here provides quick examples
of how to use the concepts using the Scala API.
The iteration operators encapsulate a part of the program and execute it
repeatedly, feeding back the result of one iteration (the partial solution) into
the next iteration. Stratosphere has two different types of iterations,
*Bulk Iteration* and *Delta Iteration*.

For both types of iterations you provide the iteration body as a function
that has data sets as input and returns a new data set. The difference is
that bulk iterations map from one data set two one new data set while
delta iterations map two data sets to two new data sets.

#### Bulk Iteration

The signature of the bulk iterate method is this:

```scala
def iterate(n: Int, stepFunction: DataSet[A] => DataSet[A])
```

where `A` is the type of the `DataSet` on which `iterate` is called. The number
of steps is given in `n`. This is how you use it in practice:

```scala
val dataPoints = DataSource(dataPointInput, DelimitedInputFormat(parseInput))
val clusterPoints = DataSource(clusterInput, DelimitedInputFormat(parseInput))

def kMeansStep(centers: DataSet[(Int, Point)]) = {

  val distances = dataPoints cross centers map computeDistance
  val nearestCenters = distances.groupBy { case (pid, _) => pid }
    .reduceGroup { ds => ds.minBy(_._2.distance) } map asPointSum.tupled
  val newCenters = nearestCenters.groupBy { case (cid, _) => cid }
    .reduceGroup sumPointSums map { case (cid, pSum) => cid -> pSum.toPoint() }

  newCenters
}

val finalCenters = clusterPoints.iterate(numIterations, kMeansStep)

val output = finalCenters.write(clusterOutput, DelimitedOutputFormat(formatOutput.tupled))
```

Not that we use some functions here which we don't show. If you want, you
can check out the complete code in our KMeans example.

#### Delta Iteration

The signature of the delta iterate method is this:

```scala
def iterateWithDelta(workset: DataSet[W], solutionSetKey: A => K, stepFunction: (DataSet[A], DataSet[W]) => (DataSet[A], DataSet[W]), maxIterations: Int)
```

where `A` is the type of the `DataSet` on which `iterateWithDelta` is called,
`W` is the type of the `DataSet` that represents the workset and `K` is the
key type. The maximum number of iterations must always be given.

For information on how delta iterations in general work on our system, please
refer to [iterations](iterations.html). A working example job is
available here:
[Scala Connected Components Example](examples_scala.html#connected_components) 

[Back to top](#top)

<section id="data-sinks">
Creating Data Sinks
-------------------

The creation of data sinks is analog to the creation of data sources. `DataSet`
has a `write` method that is used to create a sink that writes the output
of the operation to a file in the local file system or HDFS. The general pattern
is this:

```scala
val sink = out.write("<file-path>", <output-format>)
```

Where `out` is some `DataSet`. Just as for data sources, the file path can be
on of either `file:///some/file` to acces files on the local machine or
`hdfs://some/path` to read files from HDFS. The output format can be one of our
builtin formats or a custom output format. The builtin formats are:

* [DelimitedOutputFormat](#delimited-output-format)
* [CsvOutputFormat](#csv-output-format)
* [RawOutputFormat](#raw-output-format)
* [BinaryOutputFormat](#binary-output-format)
* [BinarySerializedOutputFormat](#binary-serialized-output-format)

We will now have a look at each of them and show how they are employed and in
which situations.

[Back to top](#top)

<section id="delimited-output-format">
#### DelimitedOutputFormat

This output format is meant for writing textual records that are separated by
some delimiter. The delimiter could be a newline, for example. It is used like
this:

```scala
DelimitedOutputFormat[In](formatFunction: In => String, delim: String = "\n")
```

For every element in the `DataSet` the formatting function is called and
the result of that is appended to the output file. In between the elements
the `delim` string is inserted.

An example would be:

```scala
val out: DataSet[(String, Int)]
val sink = out.write("file:///some/file", DelimitedOutputFormat( { elem =>
  "%s|%d".format(elem._1, elem._2)
}))
```

Here we use Scala String formatting to write the two fields of the tuple
separated by a pipe character. The default newline delimiter will be inserted
between the elements in the output files.

[Back to top](#top)

<section id="csv-output-format">
#### CsvOutputFormat

This output format can be used to automatically write fields of tuple
elements or case classes to CSV files. You can specify what separator should
be used between fields of an element and also the separator between elements.

```scala
CsvOutputFormat[In]()
CsvOutputFormat[In](recordDelim: String)
CsvOutputFormat[In](recordDelim: String, fieldDelim: Char)
```

The default record delimiter is a newline, the default field delimiter is a
comma. 

An example usage could look as follows:

```scala
val out: DataSet[(String, Int)]
val sink = out.write("file:///some/file", CsvOutputFormat())
```

Notice how we don't need to specify the generic type here, it is inferred.

[Back to top](#top)

<section id="raw-output-format">
#### RawOutputFormat

This input format can be used when you want to have complete control over
what gets written. You get an
[OutputStream](http://docs.oracle.com/javase/7/docs/api/java/io/OutputStream.html)
and can write the elements of the `DataSet` exactly as you see fit.

A `RawOutputFormat` is created like this:

```scala
RawOutputFormat[In](writeFunction: (In, OutputStream) => Unit)
```

The function you pass in gets one element from the `DataSet` and must
write it to the given `OutputStream`. An example would be the following:

```scala
val out: DataSet[(String, Int)]
val sink = out.write("file:///some/file", RawOutputFormat( { (elem, output) =>
  /* write elem._1 and elem._2 to output */ 
}))
```

<section id="binary-output-format">
#### BinaryOutputFormat

This format is very similar to the RawOutputFormat. The difference is that
instead of an [OutputStream](http://docs.oracle.com/javase/7/docs/api/java/io/OutputStream.html)
you get a [DataOutput](http://docs.oracle.com/javase/7/docs/api/java/io/DataOutput.html)
to which you can write binary data. You can also specify the block size for
the binary output file. When you don't specify a block size some default
is used.

A `BinaryOutputFormat` is created like this:

```scala
BinaryOutputFormat[In](writeFunction: (In, DataOutput) => Unit)
BinaryOutputFormat[In](writeFunction: (In, DataOutput) => Unit, blockSize: Long)
```
[Back to top](#top)

<section id="binary-serialized-output-format">
#### BinarySerializedOutputFormat

This output format is only meant to be used in conjunction with
`BinarySerializedInputFormat`. You can use these to write elements to files using a
Stratosphere-internal format that can efficiently be read again. You should only
use this when output is only meant to be consumed by other Stratosphere jobs.
The output format can be used on one of two ways:

```scala
BinarySerializedOutputFormat[In]()
BinarySerializedOutputFormat[In](blocksize: Long)
```

So to write elements of some `DataSet[A]` to a binary file you could use:

```scala
val out: DataSet[(String, Int)]
val sink = out.write("file:///some/file", BinarySerializedInputFormat())
```

As you can see the type of the elements need not be specified, it is inferred
by Scala.

[Back to top](#top)

<section id="execution">
Executing Jobs
--------------

To execute a data flow graph the sinks need to be wrapped in a
[ScalaPlan](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-scala/src/main/scala/eu/stratosphere/api/scala/ScalaPlan.scala)
object like this:

```scala
val out: DataSet[(String, Int)]
val sink = out.write("file:///some/file", CsvOutputFormat())

val plan = new ScalaPlan(Seq(sink))
```

You can put several sinks into the `Seq` that is passed to the constructor.

There are two ways one can execute a data flow plan: local execution and
remote/cluster execution. When using local execution the plan is executed on
the local computer. This is handy while developing jobs because you can
easily debug your code and iterate quickly. When a job is ready to be
used on bigger data sets it can be executed on a cluster. We will
now give an example for each of the two execution modes.

First up is local execution:

```scala
import eu.stratosphere.client.LocalExecutor

...

val plan: ScalaPlan = ...
LocalExecutor.execute(plan)
```

This is all there is to it.

Remote (or cluster) execution is a bit more complicated because you have
to package your code in a jar file so that it can be distributed on the cluster.
Have a look at the [scala quickstart](/quickstart/scala.html) to see how you
can set up a maven project that does the packaging. Remote execution is done
using the [RemoteExecutor](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-clients/src/main/java/eu/stratosphere/client/RemoteExecutor.java), like this:

```scala
import eu.stratosphere.client.RemoteExecutor

...

val plan: ScalaPlan = ...
val ex = new RemoteExecutor("<job manager ip address>", <job manager port>, "your.jar");
ex.executePlan(plan);
```

The IP address and the port of the Stratosphere job manager depend on your
setup. Have a look at [cluster quickstart](/quickstart/setup.html) for a quick
guide about how to set up a cluster. The default cluster port is 6123, so
if you run a job manger on your local computer you can give this and "localhost"
as the first to parameters to the `RemoteExecutor` constructor.

[Back to top](#top)

<section id="rich-functions">
Rich Functions
--------------

Sometimes having a single function that is passed to an operation is not enough.
Using Rich Functions it is possible to have state inside your operations and
have code executed before the first element is processed and after the last
element is processed. For example, instead of a simple function as in this
example:

```scala
val mapped = input map { x => x + 1 }
```

you can have a rich function like this:

```scala
val mapped = input map( new MapFunction[(String, Int), (String, Int)] {
  val someState: SomeType = ...
  override def open(config: Configuration) = {
    // one-time initialization code
  }

  override def close() = {
    // one-time clean-up code
  }

  override def apply(in: (String, Int)) = {
    // do complex stuff
    val result = ...
    result
  }
})
```

You could also create a custom class that derives from `MapFunction`
instead of the anonymous class we used here.

There are rich functions for all the various operator types. The basic
template is the some, though. The common interface that they implement 
is [Function](https://github.com/stratosphere/stratosphere/blob/release-{{site.current_stable}}/stratosphere-core/src/main/java/eu/stratosphere/api/common/functions/Function.java). The `open` and `close` methods can be overridden to run set-up
and tear-down code. The other methods can be used in a rich function to
work with the runtime context which gives information about the context
of the operator. Your operation code must now reside in an `apply` method
that has the same signature as the anonymous function you would normally
supply.

The rich functions reside in the package `eu.stratosphere.api.scala.functions`.
This is a list of all the rich functions can can be used instead of
simple functions in the respective operations:

```scala
abstract class MapFunction[In, Out] 
abstract class FlatMapFunction[In, Out] 
abstract class FilterFunction[In, Out] 

abstract class ReduceFunction[In]
abstract class GroupReduceFunction[In, Out]
abstract class CombinableGroupReduceFunction[In, Out]

abstract class JoinFunction[LeftIn, RightIn, Out]
abstract class FlatJoinFunction[LeftIn, RightIn, Out]

abstract class CoGroupFunction[LeftIn, RightIn, Out]
abstract class FlatCoGroupFunction[LeftIn, RightIn, Out]

abstract class CrossFunction[LeftIn, RightIn, Out]
abstract class FlatCrossFunction[LeftIn, RightIn, Out]
```

Note that for all the rich stubs, you need to specify the generic type of
the input (or inputs) and the output type.

[Back to top](#top)