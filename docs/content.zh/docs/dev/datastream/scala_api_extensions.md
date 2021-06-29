---
title: "Scala API扩展"
weight: 201
type: docs
aliases:
  - /zh/dev/scala_api_extensions.html
  - /zh/apis/scala_api_extensions.html
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

# Scala API扩展

为了在Scala和Java api之间保持一定的一致性，在批处理和流处理的标准api中省略了一些允许Scala高级表达的特性。

如果你想体验完整的Scala，可以选择通过隐式转换增强Scala API的扩展。

要使用所有可用的扩展，你只需为DataStream API添加一个简单的引入

{{< highlight scala >}}
import org.apache.flink.streaming.api.scala.extensions._
{{< /highlight >}}

或者，您可以引入单个扩展a-là-carte来使用您喜欢的扩展。

## 接受部分功能

通常，DataStream API不接受匿名模式匹配函数来解构元组、case类或集合，如下所示：

{{< highlight scala >}}
val data: DataStream[(Int, String, Double)] = // [...]
data.map {
  case (id, name, temperature) => // [...]
  // The previous line causes the following compilation error:
  // "The argument types of an anonymous function must be fully known. (SLS 8.5)"
}
{{< /highlight >}}

这个扩展在DataStream Scala API中引入了新的方法，这些方法在扩展API中具有一对一的对应关系。这些委托方法支持匿名模式匹配函数。

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
{{< highlight scala >}}
data.mapWith {
  case (_, value) => value.toString
}
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>flatMapWith</strong></td>
      <td><strong>flatMap (DataStream)</strong></td>
      <td>
{{< highlight scala >}}
data.flatMapWith {
  case (_, name, visits) => visits.map(name -> _)
}
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>filterWith</strong></td>
      <td><strong>filter (DataStream)</strong></td>
      <td>
{{< highlight scala >}}
data.filterWith {
  case Train(_, isOnTime) => isOnTime
}
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>keyingBy</strong></td>
      <td><strong>keyBy (DataStream)</strong></td>
      <td>
{{< highlight scala >}}
data.keyingBy {
  case (id, _, _) => id
}
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>mapWith</strong></td>
      <td><strong>map (ConnectedDataStream)</strong></td>
      <td>
{{< highlight scala >}}
data.mapWith(
  map1 = case (_, value) => value.toString,
  map2 = case (_, _, value, _) => value + 1
)
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>flatMapWith</strong></td>
      <td><strong>flatMap (ConnectedDataStream)</strong></td>
      <td>
{{< highlight scala >}}
data.flatMapWith(
  flatMap1 = case (_, json) => parse(json),
  flatMap2 = case (_, _, json, _) => parse(json)
)
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>keyingBy</strong></td>
      <td><strong>keyBy (ConnectedDataStream)</strong></td>
      <td>
{{< highlight scala >}}
data.keyingBy(
  key1 = case (_, timestamp) => timestamp,
  key2 = case (id, _, _) => id
)
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>reduceWith</strong></td>
      <td><strong>reduce (KeyedStream, WindowedStream)</strong></td>
      <td>
{{< highlight scala >}}
data.reduceWith {
  case ((_, sum1), (_, sum2) => sum1 + sum2
}
{{< /highlight >}}
      </td>
    </tr>
    <tr>
      <td><strong>projecting</strong></td>
      <td><strong>apply (JoinedStream)</strong></td>
      <td>
{{< highlight scala >}}
data1.join(data2).
  whereClause(case (pk, _) => pk).
  isEqualTo(case (_, fk) => fk).
  projecting {
    case ((pk, tx), (products, fk)) => tx -> products
  }
{{< /highlight >}}
      </td>
    </tr>
  </tbody>
</table>



有关每个方法语义的更多信息, 请参考 DataStream API 文档。

要单独使用此扩展，您可以添加以下引入：

{{< highlight scala >}}
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
{{< /highlight >}}

用于DataSet扩展

{{< highlight scala >}}
import org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions
{{< /highlight >}}

下面的代码片段展示了如何一起使用这些扩展方法(以及DataSet API)的最小示例:

{{< highlight scala >}}
object Main {
  import org.apache.flink.streaming.api.scala.extensions._

  case class Point(x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    
    ds.filterWith {
      case Point(x, _) => x > 1
    }.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.keyingBy {
      case (id, value) => id
    }
  }
}
{{< /highlight >}}

{{< top >}}
