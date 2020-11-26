---
title: "Scala API Extensions"
nav-parent_id: streaming
nav-pos: 200
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

In order to keep a fair amount of consistency between the Scala and Java APIs, some
of the features that allow a high-level of expressiveness in Scala have been left
out from the standard APIs for both batch and streaming.

If you want to _enjoy the full Scala experience_ you can choose to opt-in to
extensions that enhance the Scala API via implicit conversions.

To use all the available extensions, you can just add a simple `import` for the
DataSet API

{% highlight scala %}
import org.apache.flink.api.scala.extensions._
{% endhighlight %}

or the DataStream API

{% highlight scala %}
import org.apache.flink.streaming.api.scala.extensions._
{% endhighlight %}

Alternatively, you can import individual extensions _a-là-carte_ to only use those
you prefer.

## Accept partial functions

Normally, both the DataSet and DataStream APIs don't accept anonymous pattern
matching functions to deconstruct tuples, case classes or collections, like the
following:

{% highlight scala %}
val data: DataSet[(Int, String, Double)] = // [...]
data.map {
  case (id, name, temperature) => // [...]
  // The previous line causes the following compilation error:
  // "The argument types of an anonymous function must be fully known. (SLS 8.5)"
}
{% endhighlight %}

This extension introduces new methods in both the DataSet and DataStream Scala API
that have a one-to-one correspondence in the extended API. These delegating methods
do support anonymous pattern matching functions.

#### DataSet API

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left" style="width: 20%">Original</th>
      <th class="text-center">Example</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>mapWith</strong></td>
      <td><strong>map (DataSet)</strong></td>
      <td>
{% highlight scala %}
data.mapWith {
  case (_, value) => value.toString
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>mapPartitionWith</strong></td>
      <td><strong>mapPartition (DataSet)</strong></td>
      <td>
{% highlight scala %}
data.mapPartitionWith {
  case head #:: _ => head
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>flatMapWith</strong></td>
      <td><strong>flatMap (DataSet)</strong></td>
      <td>
{% highlight scala %}
data.flatMapWith {
  case (_, name, visitTimes) => visitTimes.map(name -> _)
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>filterWith</strong></td>
      <td><strong>filter (DataSet)</strong></td>
      <td>
{% highlight scala %}
data.filterWith {
  case Train(_, isOnTime) => isOnTime
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>reduceWith</strong></td>
      <td><strong>reduce (DataSet, GroupedDataSet)</strong></td>
      <td>
{% highlight scala %}
data.reduceWith {
  case ((_, amount1), (_, amount2)) => amount1 + amount2
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>reduceGroupWith</strong></td>
      <td><strong>reduceGroup (GroupedDataSet)</strong></td>
      <td>
{% highlight scala %}
data.reduceGroupWith {
  case id #:: value #:: _ => id -> value
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>groupingBy</strong></td>
      <td><strong>groupBy (DataSet)</strong></td>
      <td>
{% highlight scala %}
data.groupingBy {
  case (id, _, _) => id
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>sortGroupWith</strong></td>
      <td><strong>sortGroup (GroupedDataSet)</strong></td>
      <td>
{% highlight scala %}
grouped.sortGroupWith(Order.ASCENDING) {
  case House(_, value) => value
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>combineGroupWith</strong></td>
      <td><strong>combineGroup (GroupedDataSet)</strong></td>
      <td>
{% highlight scala %}
grouped.combineGroupWith {
  case header #:: amounts => amounts.sum
}
{% endhighlight %}
      </td>
    <tr>
      <td><strong>projecting</strong></td>
      <td><strong>apply (JoinDataSet, CrossDataSet)</strong></td>
      <td>
{% highlight scala %}
data1.join(data2).
  whereClause(case (pk, _) => pk).
  isEqualTo(case (_, fk) => fk).
  projecting {
    case ((pk, tx), (products, fk)) => tx -> products
  }

data1.cross(data2).projecting {
  case ((a, _), (_, b) => a -> b
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>projecting</strong></td>
      <td><strong>apply (CoGroupDataSet)</strong></td>
      <td>
{% highlight scala %}
data1.coGroup(data2).
  whereClause(case (pk, _) => pk).
  isEqualTo(case (_, fk) => fk).
  projecting {
    case (head1 #:: _, head2 #:: _) => head1 -> head2
  }
}
{% endhighlight %}
      </td>
    </tr>
    </tr>
  </tbody>
</table>

#### DataStream API

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Method</th>
      <th class="text-left" style="width: 20%">Original</th>
      <th class="text-center">Example</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>mapWith</strong></td>
      <td><strong>map (DataStream)</strong></td>
      <td>
{% highlight scala %}
data.mapWith {
  case (_, value) => value.toString
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>flatMapWith</strong></td>
      <td><strong>flatMap (DataStream)</strong></td>
      <td>
{% highlight scala %}
data.flatMapWith {
  case (_, name, visits) => visits.map(name -> _)
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>filterWith</strong></td>
      <td><strong>filter (DataStream)</strong></td>
      <td>
{% highlight scala %}
data.filterWith {
  case Train(_, isOnTime) => isOnTime
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>keyingBy</strong></td>
      <td><strong>keyBy (DataStream)</strong></td>
      <td>
{% highlight scala %}
data.keyingBy {
  case (id, _, _) => id
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>mapWith</strong></td>
      <td><strong>map (ConnectedDataStream)</strong></td>
      <td>
{% highlight scala %}
data.mapWith(
  map1 = case (_, value) => value.toString,
  map2 = case (_, _, value, _) => value + 1
)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>flatMapWith</strong></td>
      <td><strong>flatMap (ConnectedDataStream)</strong></td>
      <td>
{% highlight scala %}
data.flatMapWith(
  flatMap1 = case (_, json) => parse(json),
  flatMap2 = case (_, _, json, _) => parse(json)
)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>keyingBy</strong></td>
      <td><strong>keyBy (ConnectedDataStream)</strong></td>
      <td>
{% highlight scala %}
data.keyingBy(
  key1 = case (_, timestamp) => timestamp,
  key2 = case (id, _, _) => id
)
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>reduceWith</strong></td>
      <td><strong>reduce (KeyedStream, WindowedStream)</strong></td>
      <td>
{% highlight scala %}
data.reduceWith {
  case ((_, sum1), (_, sum2) => sum1 + sum2
}
{% endhighlight %}
      </td>
    </tr>
    <tr>
      <td><strong>projecting</strong></td>
      <td><strong>apply (JoinedStream)</strong></td>
      <td>
{% highlight scala %}
data1.join(data2).
  whereClause(case (pk, _) => pk).
  isEqualTo(case (_, fk) => fk).
  projecting {
    case ((pk, tx), (products, fk)) => tx -> products
  }
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>



For more information on the semantics of each method, please refer to the
[DataSet]({% link dev/batch/index.zh.md %}) and [DataStream]({% link dev/datastream_api.zh.md %}) API documentation.

To use this extension exclusively, you can add the following `import`:

{% highlight scala %}
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
{% endhighlight %}

for the DataSet extensions and

{% highlight scala %}
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
{% endhighlight %}

The following snippet shows a minimal example of how to use these extension
methods together (with the DataSet API):

{% highlight scala %}
object Main {
  import org.apache.flink.api.scala.extensions._
  case class Point(x: Double, y: Double)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    ds.filterWith {
      case Point(x, _) => x > 1
    }.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.groupingBy {
      case (id, value) => id
    }
  }
}
{% endhighlight %}

{% top %}
