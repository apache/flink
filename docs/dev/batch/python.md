---
title: "Python Programming Guide"
is_beta: true
nav-title: Python API
nav-parent_id: batch
nav-pos: 4
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
to run it locally.

{% highlight python %}
from flink.plan.Environment import get_environment
from flink.functions.GroupReduceFunction import GroupReduceFunction

class Adder(GroupReduceFunction):
  def reduce(self, iterator, collector):
    count, word = iterator.next()
    count += sum([x[0] for x in iterator])
    collector.collect((count, word))

env = get_environment()
data = env.from_elements("Who's there?",
 "I think I hear them. Stand, ho! Who's there?")

data \
  .flat_map(lambda x, c: [(1, word) for word in x.lower().split()]) \
  .group_by(1) \
  .reduce_group(Adder(), combinable=True) \
  .output()

env.execute(local=True)
{% endhighlight %}

{% top %}

Program Skeleton
----------------

As we already saw in the example, Flink programs look like regular python programs.
Each program consists of the same basic parts:

1. Obtain an `Environment`,
2. Load/create the initial data,
3. Specify transformations on this data,
4. Specify where to put the results of your computations, and
5. Execute your program.

We will now give an overview of each of those steps but please refer to the respective sections for
more details.


The `Environment` is the basis for all Flink programs. You can
obtain one using these static methods on class `Environment`:

{% highlight python %}
get_environment()
{% endhighlight %}

For specifying data sources the execution environment has several methods
to read from files. To just read a text file as a sequence of lines, you can use:

{% highlight python %}
env = get_environment()
text = env.read_text("file:///path/to/file")
{% endhighlight %}

This will give you a DataSet on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataSet you can apply transformations to create a new
DataSet which you can then write to a file, transform again, or
combine with other DataSets. You apply transformations by calling
methods on DataSet with your own custom transformation function. For example,
a map transformation looks like this:

{% highlight python %}
data.map(lambda x: x*2)
{% endhighlight %}

This will create a new DataSet by doubling every value in the original DataSet.
For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataSet that needs to be written to disk you can call one
of these methods on DataSet:

{% highlight python %}
data.write_text("<file-path>", WriteMode=Constants.NO_OVERWRITE)
write_csv("<file-path>", line_delimiter='\n', field_delimiter=',', write_mode=Constants.NO_OVERWRITE)
output()
{% endhighlight %}

The last method is only useful for developing/debugging on a local machine,
it will output the contents of the DataSet to standard output. (Note that in
a cluster, the result goes to the standard out stream of the cluster nodes and ends
up in the *.out* files of the workers).
The first two do as the name suggests.
Please refer to [Data Sinks](#data-sinks) for more information on writing to files.

Once you specified the complete program you need to call `execute` on
the `Environment`. This will submit your program for execution on a cluster.
{% top %}

Project setup
---------------

Apart from setting up Flink, no additional work is required. The python package can be found in the /resource folder of your Flink distribution. The flink package, along with the plan and optional packages are automatically distributed among the cluster via HDFS when running a job.

The Python API was tested on Linux/Windows systems that have Python 2.7 or 3.4 installed.

By default Flink will start python processes by calling "python". By setting the "python.binary.path" key in the flink-conf.yaml you can modify this behaviour to use a binary of your choice.

{% top %}

Lazy Evaluation
---------------

All Flink programs are executed lazily: When the program's main method is executed, the data loading
and transformations do not happen directly. Rather, each operation is created and added to the
program's plan. The operations are actually executed when one of the `execute()` methods is invoked
on the Environment object.

The lazy evaluation lets you construct sophisticated programs that Flink executes as one
holistically planned unit.

{% top %}


Transformations
---------------

Data transformations transform one or more DataSets into a new DataSet. Programs can combine
multiple transformations into sophisticated assemblies.

This section gives a brief overview of the available transformations. The [transformations
documentation](dataset_transformations.html) has a full description of all transformations with
examples.

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
{% highlight python %}
data.map(lambda x: x * 2)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FlatMap</strong></td>
      <td>
        <p>Takes one element and produces zero, one, or more elements. </p>
{% highlight python %}
data.flat_map(
  lambda x,c: [(1,word) for word in line.lower().split() for line in x])
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>MapPartition</strong></td>
      <td>
        <p>Transforms a parallel partition in a single function call. The function get the partition
        as an `Iterator` and can produce an arbitrary number of result values. The number of
        elements in each partition depends on the parallelism and previous operations.</p>
{% highlight python %}
data.map_partition(lambda x,c: [value * 2 for value in x])
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong></td>
      <td>
        <p>Evaluates a boolean function for each element and retains those for which the function
        returns true.</p>
{% highlight python %}
data.filter(lambda x: x > 1000)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Reduce</strong></td>
      <td>
        <p>Combines a group of elements into a single element by repeatedly combining two elements
        into one. Reduce may be applied on a full data set, or on a grouped data set.</p>
{% highlight python %}
data.reduce(lambda x,y : x + y)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>ReduceGroup</strong></td>
      <td>
        <p>Combines a group of elements into one or more elements. ReduceGroup may be applied on a
        full data set, or on a grouped data set.</p>
{% highlight python %}
class Adder(GroupReduceFunction):
  def reduce(self, iterator, collector):
    count, word = iterator.next()
    count += sum([x[0] for x in iterator)      
    collector.collect((count, word))

data.reduce_group(Adder())
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Aggregate</strong></td>
      <td>
        <p>Performs a built-in operation (sum, min, max) on one field of all the Tuples in a
        data set or in each group of a data set. Aggregation can be applied on a full dataset
        or on a grouped data set.</p>
{% highlight python %}
# This code finds the sum of all of the values in the first field and the maximum of all of the values in the second field
data.aggregate(Aggregation.Sum, 0).and_agg(Aggregation.Max, 1)

# min(), max(), and sum() syntactic sugar functions are also available
data.sum(0).and_agg(Aggregation.Max, 1)
{% endhighlight %}
      </td>
    </tr>

    </tr>
      <td><strong>Join</strong></td>
      <td>
        Joins two data sets by creating all pairs of elements that are equal on their keys.
        Optionally uses a JoinFunction to turn the pair of elements into a single element.
        See <a href="#specifying-keys">keys</a> on how to define join keys.
{% highlight python %}
# In this case tuple fields are used as keys.
# "0" is the join field on the first tuple
# "1" is the join field on the second tuple.
result = input1.join(input2).where(0).equal_to(1)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>CoGroup</strong></td>
      <td>
        <p>The two-dimensional variant of the reduce operation. Groups each input on one or more
        fields and then joins the groups. The transformation function is called per pair of groups.
        See <a href="#specifying-keys">keys</a> on how to define coGroup keys.</p>
{% highlight python %}
data1.co_group(data2).where(0).equal_to(1)
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Cross</strong></td>
      <td>
        <p>Builds the Cartesian product (cross product) of two inputs, creating all pairs of
        elements. Optionally uses a CrossFunction to turn the pair of elements into a single
        element.</p>
{% highlight python %}
result = data1.cross(data2)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>Union</strong></td>
      <td>
        <p>Produces the union of two data sets.</p>
{% highlight python %}
data.union(data2)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>ZipWithIndex</strong></td>
      <td>
        <p>Assigns consecutive indexes to each element. For more information, please refer to
        the [Zip Elements Guide](zip_elements_guide.html#zip-with-a-dense-index).</p>
{% highlight python %}
data.zip_with_index()
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

{% top %}


Specifying Keys
-------------

Some transformations (like Join or CoGroup) require that a key is defined on
its argument DataSets, and other transformations (Reduce, GroupReduce) allow that the DataSet is grouped on a key before they are
applied.

A DataSet is grouped as
{% highlight python %}
reduced = data \
  .group_by(<define key here>) \
  .reduce_group(<do something>)
{% endhighlight %}

The data model of Flink is not based on key-value pairs. Therefore,
you do not need to physically pack the data set types into keys and
values. Keys are "virtual": they are defined as functions over the
actual data to guide the grouping operator.

### Define keys for Tuples
{:.no_toc}

The simplest case is grouping a data set of Tuples on one or more
fields of the Tuple:
{% highlight python %}
reduced = data \
  .group_by(0) \
  .reduce_group(<do something>)
{% endhighlight %}

The data set is grouped on the first field of the tuples.
The group-reduce function will thus receive groups of tuples with
the same value in the first field.

{% highlight python %}
grouped = data \
  .group_by(0,1) \
  .reduce(/*do something*/)
{% endhighlight %}

The data set is grouped on the composite key consisting of the first and the
second fields, therefore the reduce function will receive groups
with the same value for both fields.

A note on nested Tuples: If you have a DataSet with a nested tuple
specifying `group_by(<index of tuple>)` will cause the system to use the full tuple as a key.

{% top %}


Passing Functions to Flink
--------------------------

Certain operations require user-defined functions, whereas all of them accept lambda functions and rich functions as arguments.

{% highlight python %}
data.filter(lambda x: x > 5)
{% endhighlight %}

{% highlight python %}
class Filter(FilterFunction):
    def filter(self, value):
        return value > 5

data.filter(Filter())
{% endhighlight %}

Rich functions allow the use of imported functions, provide access to broadcast-variables,
can be parameterized using __init__(), and are the go-to-option for complex functions.
They are also the only way to define an optional `combine` function for a reduce operation.

Lambda functions allow the easy insertion of one-liners. Note that a lambda function has to return
an iterable, if the operation can return multiple values. (All functions receiving a collector argument)

{% top %}

Data Types
----------

Flink's Python API currently only offers native support for primitive python types (int, float, bool, string) and byte arrays.

The type support can be extended by passing a serializer, deserializer and type class to the environment.
{% highlight python %}
class MyObj(object):
    def __init__(self, i):
        self.value = i


class MySerializer(object):
    def serialize(self, value):
        return struct.pack(">i", value.value)


class MyDeserializer(object):
    def _deserialize(self, read):
        i = struct.unpack(">i", read(4))[0]
        return MyObj(i)


env.register_custom_type(MyObj, MySerializer(), MyDeserializer())
{% endhighlight %}

#### Tuples/Lists

You can use the tuples (or lists) for composite types. Python tuples are mapped to the Flink Tuple type, that contain
a fix number of fields of various types (up to 25). Every field of a tuple can be a primitive type - including further tuples, resulting in nested tuples.

{% highlight python %}
word_counts = env.from_elements(("hello", 1), ("world",2))

counts = word_counts.map(lambda x: x[1])
{% endhighlight %}

When working with operators that require a Key for grouping or matching records,
Tuples let you simply specify the positions of the fields to be used as key. You can specify more
than one position to use composite keys (see [Section Data Transformations](#transformations)).

{% highlight python %}
wordCounts \
    .group_by(0) \
    .reduce(MyReduceFunction())
{% endhighlight %}

{% top %}

Data Sources
------------

Data sources create the initial data sets, such as from files or from collections.

File-based:

- `read_text(path)` - Reads files line wise and returns them as Strings.
- `read_csv(path, type)` - Parses files of comma (or another char) delimited fields.
  Returns a DataSet of tuples. Supports the basic java types and their Value counterparts as field
  types.

Collection-based:

- `from_elements(*args)` - Creates a data set from a Seq. All elements
- `generate_sequence(from, to)` - Generates the sequence of numbers in the given interval, in parallel.

**Examples**

{% highlight python %}
env  = get_environment

\# read text file from local files system
localLiens = env.read_text("file:#/path/to/my/textfile")

\# read text file from a HDFS running at nnHost:nnPort
hdfsLines = env.read_text("hdfs://nnHost:nnPort/path/to/my/textfile")

\# read a CSV file with three fields, schema defined using constants defined in flink.plan.Constants
csvInput = env.read_csv("hdfs:///the/CSV/file", (INT, STRING, DOUBLE))

\# create a set from some given elements
values = env.from_elements("Foo", "bar", "foobar", "fubar")

\# generate a number sequence
numbers = env.generate_sequence(1, 10000000)
{% endhighlight %}

{% top %}

Data Sinks
----------

Data sinks consume DataSets and are used to store or return them:

- `write_text()` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *str()* method of each element.
- `write_csv(...)` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *str()* method of the objects.
- `output()` - Prints the *str()* value of each element on the
  standard out.

A DataSet can be input to multiple operations. Programs can write or print a data set and at the
same time run additional transformations on them.

**Examples**

Standard data sink methods:

{% highlight scala %}
 write DataSet to a file on the local file system
textData.write_text("file:///my/result/on/localFS")

 write DataSet to a file on a HDFS with a namenode running at nnHost:nnPort
textData.write_text("hdfs://nnHost:nnPort/my/result/on/localFS")

 write DataSet to a file and overwrite the file if it exists
textData.write_text("file:///my/result/on/localFS", WriteMode.OVERWRITE)

 tuples as lines with pipe as the separator "a|b|c"
values.write_csv("file:///path/to/the/result/file", line_delimiter="\n", field_delimiter="|")

 this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
values.write_text("file:///path/to/the/result/file")
{% endhighlight %}

{% top %}

Broadcast Variables
-------------------

Broadcast variables allow you to make a data set available to all parallel instances of an
operation, in addition to the regular input of the operation. This is useful for auxiliary data
sets, or data-dependent parameterization. The data set will then be accessible at the operator as a
Collection.

- **Broadcast**: broadcast sets are registered by name via `with_broadcast_set(DataSet, String)`
- **Access**: accessible via `self.context.get_broadcast_variable(String)` at the target operator

{% highlight python %}
class MapperBcv(MapFunction):
    def map(self, value):
        factor = self.context.get_broadcast_variable("bcv")[0][0]
        return value * factor

# 1. The DataSet to be broadcast
toBroadcast = env.from_elements(1, 2, 3)
data = env.from_elements("a", "b")

# 2. Broadcast the DataSet
data.map(MapperBcv()).with_broadcast_set("bcv", toBroadcast)
{% endhighlight %}

Make sure that the names (`bcv` in the previous example) match when registering and
accessing broadcast data sets.

**Note**: As the content of broadcast variables is kept in-memory on each node, it should not become
too large. For simpler things like scalar values you can simply parameterize the rich function.

{% top %}

Parallel Execution
------------------

This section describes how the parallel execution of programs can be configured in Flink. A Flink
program consists of multiple tasks (operators, data sources, and sinks). A task is split into
several parallel instances for execution and each parallel instance processes a subset of the task's
input data. The number of parallel instances of a task is called its *parallelism* or *degree of
parallelism (DOP)*.

The degree of parallelism of a task can be specified in Flink on different levels.

### Execution Environment Level

Flink programs are executed in the context of an [execution environment](#program-skeleton). An
execution environment defines a default parallelism for all operators, data sources, and data sinks
it executes. Execution environment parallelism can be overwritten by explicitly configuring the
parallelism of an operator.

The default parallelism of an execution environment can be specified by calling the
`set_parallelism()` method. To execute all operators, data sources, and data sinks of the
[WordCount](#example-program) example program with a parallelism of `3`, set the default parallelism of the
execution environment as follows:

{% highlight python %}
env = get_environment()
env.set_parallelism(3)

text.flat_map(lambda x,c: x.lower().split()) \
    .group_by(1) \
    .reduce_group(Adder(), combinable=True) \
    .output()

env.execute()
{% endhighlight %}

### System Level

A system-wide default parallelism for all execution environments can be defined by setting the
`parallelism.default` property in `./conf/flink-conf.yaml`. See the
[Configuration]({{ site.baseurl }}/ops/config.html) documentation for details.

{% top %}

Executing Plans
---------------

To run the plan with Flink, go to your Flink distribution, and run the pyflink.sh script from the /bin folder.
The script containing the plan has to be passed as the first argument, followed by a number of additional python
packages, and finally, separated by - additional arguments that will be fed to the script.

{% highlight python %}
./bin/pyflink.sh <Script>[ <pathToPackage1>[ <pathToPackageX]][ - <param1>[ <paramX>]]
{% endhighlight %}

{% top %}
