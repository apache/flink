/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.scala

import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.Assert.assertTrue
import org.junit.Test

import scala.collection.mutable.ListBuffer

class IntervalJoinITCase extends AbstractTestBase {

  @Test
  def testInclusiveBounds(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream1 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val dataStream2 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val sink = new ResultSink()

    val join = dataStream1.intervalJoin(dataStream2)
      .between(Time.milliseconds(0), Time.milliseconds(2))
      .process(new CombineJoinFunction())

    assertTrue(join.dataType.isInstanceOf[CaseClassTypeInfo[_]])

    join.addSink(sink)

    env.execute()

    sink.expectInAnyOrder(
      "(key:key,0)",
      "(key:key,1)",
      "(key:key,2)",

      "(key:key,2)",
      "(key:key,3)",

      "(key:key,4)"
    )
  }

  @Test
  def testExclusiveBounds(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream1 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val dataStream2 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val sink = new ResultSink()

    val join = dataStream1.intervalJoin(dataStream2)
      .between(Time.milliseconds(0), Time.milliseconds(2))
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(new CombineJoinFunction())

    assertTrue(join.dataType.isInstanceOf[CaseClassTypeInfo[_]])

    join.addSink(sink)

    env.execute()

    sink.expectInAnyOrder(
      "(key:key,1)",
      "(key:key,3)"
    )
  }
}

object Companion {
  val results: ListBuffer[String] = new ListBuffer()
}

class ResultSink extends SinkFunction[(String, Long)] {

  override def invoke(value: (String, Long), context: SinkFunction.Context): Unit = {
    Companion.results.append(value.toString())
  }

  def expectInAnyOrder(expected: String*): Unit = {
    assertTrue(expected.toSet.equals(Companion.results.toSet))
  }
}

class TimestampExtractor extends AscendingTimestampExtractor[(String, Long)] {
  override def extractAscendingTimestamp(element: (String, Long)): Long = element._2
}

class CombineJoinFunction
  extends ProcessJoinFunction[(String, Long), (String, Long), (String, Long)] {

  override def processElement(
      left: (String, Long),
      right: (String, Long),
      ctx: ProcessJoinFunction[(String, Long), (String, Long), (String, Long)]#Context,
      out: Collector[(String, Long)]): Unit = {
    out.collect((left._1 + ":" + right._1, left._2 + right._2))
  }
}
