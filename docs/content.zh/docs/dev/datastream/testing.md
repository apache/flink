---
title: "测试"
weight: 100
type: docs
aliases:
  - /zh/dev/stream/testing.html
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

# 测试

测试是每个软件开发过程中不可或缺的一部分， Apache Flink 同样提供了在测试金字塔的多个级别上测试应用程序代码的工具。



## 测试用户自定义函数

通常，我们可以假设 Flink 在用户自定义函数之外产生了正确的结果。因此，建议尽可能多的用单元测试来测试那些包含主要业务逻辑的类。

### 单元测试无状态、无时间限制的 UDF


例如，让我们以以下无状态的 `MapFunction` 为例。

{{< tabs "e79be56f-6ce8-4699-b150-9b03e735927a" >}}
{{< tab "Java" >}}
```java
public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class IncrementMapFunction extends MapFunction[Long, Long] {

    override def map(record: Long): Long = {
        record + 1
    }
}
```
{{< /tab >}}
{{< /tabs >}}

通过传递合适地参数并验证输出，你可以很容易的使用你喜欢的测试框架对这样的函数进行单元测试。

{{< tabs "5f0840c1-8d81-459c-9109-e763c51aa96a" >}}
{{< tab "Java" >}}
```java
public class IncrementMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();

        // call the methods that you have implemented
        assertEquals(3L, incrementer.map(2L));
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class IncrementMapFunctionTest extends FlatSpec with Matchers {

    "IncrementMapFunction" should "increment values" in {
        // instantiate your function
        val incrementer: IncrementMapFunction = new IncrementMapFunction()

        // call the methods that you have implemented
        incremeter.map(2) should be (3)
    }
}
```
{{< /tab >}}
{{< /tabs >}}

类似地，对于使用 `org.apache.flink.util.Collector` 的用户自定义函数（例如`FlatMapFunction` 或者 `ProcessFunction`），可以通过提供模拟对象而不是真正的 collector 来轻松测试。具有与 `IncrementMapFunction` 相同功能的 `FlatMapFunction` 可以按照以下方式进行单元测试。

{{< tabs "00a09832-217e-4346-bb0a-75d08cbdd477" >}}
{{< tab "Java" >}}
```java
public class IncrementFlatMapFunctionTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();

        Collector<Integer> collector = mock(Collector.class);

        // call the methods that you have implemented
        incrementer.flatMap(2L, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(3L);
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class IncrementFlatMapFunctionTest extends FlatSpec with MockFactory {

    "IncrementFlatMapFunction" should "increment values" in {
       // instantiate your function
      val incrementer : IncrementFlatMapFunction = new IncrementFlatMapFunction()

      val collector = mock[Collector[Integer]]

      //verify collector was called with the right output
      (collector.collect _).expects(3)

      // call the methods that you have implemented
      flattenFunction.flatMap(2, collector)
  }
}
```
{{< /tab >}}
{{< /tabs >}}

### 对有状态或及时 UDF 和自定义算子进行单元测试

对使用管理状态或定时器的用户自定义函数的功能测试会更加困难，因为它涉及到测试用户代码和 Flink 运行时的交互。
为此，Flink 提供了一组所谓的测试工具，可用于测试用户自定义函数和自定义算子：

* `OneInputStreamOperatorTestHarness` (适用于 `DataStream` 上的算子)
* `KeyedOneInputStreamOperatorTestHarness` (适用于 `KeyedStream` 上的算子)
* `TwoInputStreamOperatorTestHarness` (f适用于两个 `DataStream` 的 `ConnectedStreams` 算子)
* `KeyedTwoInputStreamOperatorTestHarness` (适用于两个 `KeyedStream` 上的 `ConnectedStreams` 算子)

要使用测试工具，还需要一组其他的依赖项（测试范围）。

{{< artifact flink-test-utils withScalaVersion withTestScope >}}
{{< artifact flink-runtime withTestScope >}}
{{< artifact flink-streaming-java withScalaVersion withTestScope withTestClassifier >}}

现在，可以使用测试工具将记录和 watermark 推送到用户自定义函数或自定义算子中，控制处理时间，最后对算子的输出（包括旁路输出）进行校验。

{{< tabs "b5579690-29cc-4549-8d0f-b34c33a19937" >}}
{{< tab "Java" >}}
```java

public class StatefulFlatMapTest {
    private OneInputStreamOperatorTestHarness<Long, Long> testHarness;
    private StatefulFlatMap statefulFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulFlatMapFunction = new StatefulFlatMapFunction();

        // wrap user defined function into a the corresponding operator
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulFlatMapFunction() throws Exception {

        //push (timestamped) elements into the operator (and hence user defined function)
        testHarness.processElement(2L, 100L);

        //trigger event time timers by advancing the event time of the operator with a watermark
        testHarness.processWatermark(100L);

        //trigger processing time timers by advancing the processing time of the operator directly
        testHarness.setProcessingTime(100L);

        //retrieve list of emitted records for assertions
        assertThat(testHarness.getOutput(), containsInExactlyThisOrder(3L));

        //retrieve list of records emitted to a specific side output for assertions (ProcessFunction only)
        //assertThat(testHarness.getSideOutput(new OutputTag<>("invalidRecords")), hasSize(0))
    }
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class StatefulFlatMapFunctionTest extends FlatSpec with Matchers with BeforeAndAfter {

  private var testHarness: OneInputStreamOperatorTestHarness[Long, Long] = null
  private var statefulFlatMap: StatefulFlatMapFunction = null

  before {
    //instantiate user-defined function
    statefulFlatMap = new StatefulFlatMap

    // wrap user defined function into a the corresponding operator
    testHarness = new OneInputStreamOperatorTestHarness[Long, Long](new StreamFlatMap(statefulFlatMap))

    // optionally configured the execution environment
    testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

    // open the test harness (will also call open() on RichFunctions)
    testHarness.open();
  }

  "StatefulFlatMap" should "do some fancy stuff with timers and state" in {


    //push (timestamped) elements into the operator (and hence user defined function)
    testHarness.processElement(2, 100);

    //trigger event time timers by advancing the event time of the operator with a watermark
    testHarness.processWatermark(100);

    //trigger proccesign time timers by advancing the processing time of the operator directly
    testHarness.setProcessingTime(100);

    //retrieve list of emitted records for assertions
    testHarness.getOutput should contain (3)

    //retrieve list of records emitted to a specific side output for assertions (ProcessFunction only)
    //testHarness.getSideOutput(new OutputTag[Int]("invalidRecords")) should have size 0
  }
}
```
{{< /tab >}}
{{< /tabs >}}

`KeyedOneInputStreamOperatorTestHarness` 和 `KeyedTwoInputStreamOperatorTestHarness` 可以通过为键的类另外提供一个包含 `TypeInformation` 的 `KeySelector` 来实例化。

{{< tabs "2d844043-ef31-4469-a3f7-20c968f3dddb" >}}
{{< tab "Java" >}}
```java

public class StatefulFlatMapFunctionTest {
    private OneInputStreamOperatorTestHarness<String, Long, Long> testHarness;
    private StatefulFlatMap statefulFlatMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulFlatMapFunction = new StatefulFlatMapFunction();

        // wrap user defined function into a the corresponding operator
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction), new MyStringKeySelector(), Types.STRING);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    //tests

}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class StatefulFlatMapTest extends FlatSpec with Matchers with BeforeAndAfter {

  private var testHarness: OneInputStreamOperatorTestHarness[String, Long, Long] = null
  private var statefulFlatMapFunction: FlattenFunction = null

  before {
    //instantiate user-defined function
    statefulFlatMapFunction = new StateFulFlatMap

    // wrap user defined function into a the corresponding operator
    testHarness = new KeyedOneInputStreamOperatorTestHarness(new StreamFlatMap(statefulFlatMapFunction),new MyStringKeySelector(), Types.STRING())

    // open the test harness (will also call open() on RichFunctions)
    testHarness.open();
  }

  //tests

}
```
{{< /tab >}}
{{< /tabs >}}

在 Flink 代码库里可以找到更多使用这些测试工具的示例，例如：

* `org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest` 是测试算子和用户自定义函数（取决于处理时间和事件时间）的一个很好的例子。
* `org.apache.flink.streaming.api.functions.sink.filesystem.LocalStreamingFileSinkTest` 展示了如何使用 `AbstractStreamOperatorTestHarness` 测试自定义 sink。具体来说，它使用 `AbstractStreamOperatorTestHarness.snapshot` 和 `AbstractStreamOperatorTestHarness.initializeState` 来测试它与 Flink checkpoint 机制的交互。

<span class="label label-info">注意</span> `AbstractStreamOperatorTestHarness` 及其派生类目前不属于公共 API，可以进行更改。

#### 单元测试 Process Function

考虑到它的重要性，除了之前可以直接用于测试 `ProcessFunction` 的测试工具之外，Flink 还提供了一个名为 `ProcessFunctionTestHarnesses` 的测试工具工厂类，可以简化测试工具的实例化。考虑以下示例：

<span class="label label-info">注意</span> 要使用此测试工具，还需要引入上一节中介绍的依赖项。

{{< tabs "fd27bf4c-b916-43cd-bd87-8882d52cda1f" >}}
{{< tab "Java" >}}
```java
public static class PassThroughProcessFunction extends ProcessFunction<Integer, Integer> {

	@Override
	public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
        out.collect(value);
	}
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class PassThroughProcessFunction extends ProcessFunction[Integer, Integer] {

    @throws[Exception]
    override def processElement(value: Integer, ctx: ProcessFunction[Integer, Integer]#Context, out: Collector[Integer]): Unit = {
      out.collect(value)
    }
}
```
{{< /tab >}}
{{< /tabs >}}

通过传递合适的参数并验证输出，对使用 `ProcessFunctionTestHarnesses` 是很容易进行单元测试并验证输出。

{{< tabs "80cb26f7-957a-47d0-9115-14aeb59af5c1" >}}
{{< tab "Java" >}}
```java
public class PassThroughProcessFunctionTest {

    @Test
    public void testPassThrough() throws Exception {

        //instantiate user-defined function
        PassThroughProcessFunction processFunction = new PassThroughProcessFunction();

        // wrap user defined function into a the corresponding operator
        OneInputStreamOperatorTestHarness<Integer, Integer> harness = ProcessFunctionTestHarnesses
        	.forProcessFunction(processFunction);

        //push (timestamped) elements into the operator (and hence user defined function)
        harness.processElement(1, 10);

        //retrieve list of emitted records for assertions
        assertEquals(harness.extractOutputValues(), Collections.singletonList(1));
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class PassThroughProcessFunctionTest extends FlatSpec with Matchers {

  "PassThroughProcessFunction" should "forward values" in {

    //instantiate user-defined function
    val processFunction = new PassThroughProcessFunction

    // wrap user defined function into a the corresponding operator
    val harness = ProcessFunctionTestHarnesses.forProcessFunction(processFunction)

    //push (timestamped) elements into the operator (and hence user defined function)
    harness.processElement(1, 10)

    //retrieve list of emitted records for assertions
    harness.extractOutputValues() should contain (1)
  }
}
```
{{< /tab >}}
{{< /tabs >}}

有关如何使用 `ProcessFunctionTestHarnesses` 来测试 `ProcessFunction` 不同风格的更多示例，, 例如 `KeyedProcessFunction`，`KeyedCoProcessFunction`，`BroadcastProcessFunction`等，鼓励用户自行查看 `ProcessFunctionTestHarnessesTest`。

## 测试 Flink 作业

### JUnit 规则 `MiniClusterWithClientResource`

Apache Flink 提供了一个名为 `MiniClusterWithClientResource` 的 Junit 规则，用于针对本地嵌入式小型集群测试完整的作业。
叫做 `MiniClusterWithClientResource`.

要使用 `MiniClusterWithClientResource`，需要添加一个额外的依赖项（测试范围）。

{{< artifact flink-test-utils withScalaVersion withTestScope >}}

让我们采用与前面几节相同的简单 `MapFunction`来做示例。

{{< tabs "43ccf9d3-0e08-43b7-ba36-cce2f093d3a5" >}}
{{< tab "Java" >}}
```java
public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class IncrementMapFunction extends MapFunction[Long, Long] {

    override def map(record: Long): Long = {
        record + 1
    }
}
```
{{< /tab >}}
{{< /tabs >}}

现在，可以在本地 Flink 集群使用这个 `MapFunction` 的简单 pipeline，如下所示。

{{< tabs "835fbb7e-e7e0-4019-8db0-4b357fd02aa4" >}}
{{< tab "Java" >}}
```java
public class ExampleIntegrationTest {

     @ClassRule
     public static MiniClusterWithClientResource flinkCluster =
         new MiniClusterWithClientResource(
             new MiniClusterResourceConfiguration.Builder()
                 .setNumberSlotsPerTaskManager(2)
                 .setNumberTaskManagers(1)
                 .build());

    @Test
    public void testIncrementPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements(1L, 21L, 22L)
                .map(new IncrementMapFunction())
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSink.values.containsAll(2L, 22L, 23L));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Long value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class StreamingJobIntegrationTest extends FlatSpec with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(2)
    .setNumberTaskManagers(1)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }


  "IncrementFlatMapFunction pipeline" should "incrementValues" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(2)

    // values are collected in a static variable
    CollectSink.values.clear()

    // create a stream of custom elements and apply transformations
    env.fromElements(1L, 21L, 22L)
       .map(new IncrementMapFunction())
       .addSink(new CollectSink())

    // execute
    env.execute()

    // verify your results
    CollectSink.values should contain allOf (2, 22, 23)
    }
}

// create a testing sink
class CollectSink extends SinkFunction[Long] {

  override def invoke(value: Long, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
    // must be static
    val values: util.List[Long] = Collections.synchronizedList(new util.ArrayList())
}
```
{{< /tab >}}
{{< /tabs >}}

关于使用 `MiniClusterWithClientResource` 进行集成测试的几点备注：

* 为了不将整个 pipeline 代码从生产复制到测试，请将你的 source 和 sink 在生产代码中设置成可插拔的，并在测试中注入特殊的测试 source 和测试 sink。

* 这里使用 `CollectSink` 中的静态变量，是因为Flink 在将所有算子分布到整个集群之前先对其进行了序列化。
解决此问题的一种方法是与本地 Flink 小型集群通过实例化算子的静态变量进行通信。
或者，你可以使用测试的 sink 将数据写入临时目录的文件中。

* 如果你的作业使用事件时间计时器，则可以实现自定义的 *并行* 源函数来发出 watermark。

* 建议始终以 parallelism > 1 的方式在本地测试 pipeline，以识别只有在并行执行 pipeline 时才会出现的 bug。

* 优先使用 `@ClassRule` 而不是 `@Rule`，这样多个测试可以共享同一个 Flink 集群。这样做可以节省大量的时间，因为 Flink 集群的启动和关闭通常会占用实际测试的执行时间。

* 如果你的 pipeline 包含自定义状态处理，则可以通过启用 checkpoint 并在小型集群中重新启动作业来测试其正确性。为此，你需要在 pipeline 中（仅测试）抛出用户自定义函数的异常来触发失败。

{{< top >}}
