---
title: "Testing"
nav-parent_id: dev
nav-id: testing
nav-pos: 99
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

This page briefly discusses how to test Flink application in the local environment.

* This will be replaced by the TOC
{:toc}

## Unit testing

It is encouraged to test your classes with unit tests as much as possible. For example if one implement following `ReduceFunction`:

~~~java
public class SumReduce implements ReduceFunction<Long> {
    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
        return value1 + value2;
    }
}
~~~

it is very easy to unit test it with your favorite framework:

~~~java
public class SumReduceTest {
    @Test
    public void testSum() throws Exception {
        SumReduce sumReduce = new SumReduce();

        assertEquals(42L, sumReduce.reduce(40L, 2L));
    }
}
~~~

Or in scala:

~~~scala
class SumReduce extends ReduceFunction[Long] {
    override def reduce(value1: java.lang.Long,
                        value2: java.lang.Long): java.lang.Long = value1 + value2
}
~~~

~~~scala
class SumReduceTest extends FlatSpec with Matchers {
    "SumReduce" should "add values" in {
        val sumReduce: SumReduce = new SumReduce()
        sumReduce.reduce(40L, 2L) should be (42L)
    }
}
~~~

## Integration testing

You also can write integration tests that are executed against local Flink mini cluster.
In order to do so add a test dependency `flink-test-utils`.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-test-utils{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

For example if you want to test the following `MapFunction`:

~~~java
public class MultiplyByTwo implements MapFunction<Long, Long> {
    @Override
    public Long map(Long value) throws Exception {
        return value * 2;
    }
}
~~~

You could write following integration test:

~~~java
public class ExampleIntegrationTest extends StreamingMultipleProgramsTestBase {
    @Test
    public void testMultiply() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // values are collected on a static variable
        CollectSink.values.clear();

        env.fromElements(1L, 21L, 22L)
                .map(new MultiplyByTwo())
                .addSink(new CollectSink());
        env.execute();

        assertEquals(Lists.newArrayList(2L, 42L, 44L), CollectSink.values);
    }

    private static class CollectSink implements SinkFunction<Long> {
        // must be static
        public static final List<Long> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Long value) throws Exception {
            values.add(value);
        }
    }
}
~~~

or in Scala:

~~~scala
class MultiplyByTwo extends MapFunction[Long, Long] {
  override def map(value: java.lang.Long): java.lang.Long = value * 2
}
~~~

~~~scala
class ExampleIntegrationTest extends FlatSpec with Matchers {
    "MultiplyByTwo" should "multiply it input by two" in {
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // values are collected on a static variable
        CollectSink.values.clear()
        env
            .fromElements(1L, 21L, 22L)
            .map(new MultiplyByTwo())
            .addSink(new CollectSink())
            env.execute()
        CollectSink.values should be (Lists.newArrayList(2L, 42L, 44L))
    }
}

object CollectSink {
    // must be static
    val values: List[Long] = new ArrayList()
}

class CollectSink extends SinkFunction[Long] {
    override def invoke(value: java.lang.Long): Unit = {
        synchronized {
            values.add(value)
        }
    }
}
~~~

Static variable in `CollectSink` is used here because Flink serializes all operators before distributing them across a cluster.
Communicating with operators instantiated by a local flink mini cluster via static variables is one way around this issue.
Alternatively in your test sink you could for example write the data to files in a temporary directory.
Of course you could use your own custom sources and sinks, which can emit watermarks.

## Testing checkpointing and state handling

One way to test state handling is to enable checkpointing in integration tests. You can do that by
configuring `environment` in the test:
~~~java
env.enableCheckpointing(500);
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
~~~
and for example adding to your Flink application an identity mapper operator that will throw an exception
once every `1000ms`. However writing such test could be tricky because of time dependencies between the actions.

Another approach is to write a unit test using `AbstractStreamOperatorTestHarness` from `flink-streaming-java` module.
For example how to do that please look at the `org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest`
also in the `flink-streaming-java`. Be aware that `AbstractStreamOperatorTestHarness` is not currently a part of public API
and can be subject to change.
