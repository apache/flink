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

package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.ExtendWith

import java.time.{Duration, Instant, LocalDateTime, ZoneId, ZoneOffset}

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class TimeAttributesITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @TestTemplate
  def testMissingTimeAttributeInLegacySourceThrowsCorrectException(): Unit = {
    //TODO: this test can be removed when SourceFunction gets removed (FLIP-27 sources set TimestampAssigner.NO_TIMESTAMP value as event time when no timestamp is provided. See SourceOutputWithWatermarks#collect())
    val stream = env
      .addSource(new SourceFunction[(Long, String)]() {
        def run(ctx: SourceFunction.SourceContext[(Long, String)]) {
          ctx.collect(1L -> "hello")
          ctx.collect(2L -> "world")
        }

        def cancel() {}
      })
    tEnv.createTemporaryView("test", stream, Schema.newBuilder()
      .columnByMetadata("event_time", DataTypes.TIMESTAMP(3), "rowtime", true)
      .build())
    val result = tEnv.sqlQuery("SELECT * FROM test")

    val sink = new TestingAppendSink()
    tEnv.toDataStream(result).addSink(sink)

    assertThatThrownBy(() => env.execute())
      .hasMessageNotContaining("Rowtime timestamp is not defined. Please make sure that a " +
        "proper TimestampAssigner is defined and the stream environment uses the EventTime " +
        "time characteristic.")
  }

  @TestTemplate
  def testTimestampAttributesWithWatermarkStrategy(): Unit = {
    val data = List(Instant.now().toEpochMilli -> "hello", Instant.now().toEpochMilli -> "world")
    val stream = env.fromCollection[(Long, String)](data).assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[(Long, String)](Duration.ofMinutes(5))
        .withTimestampAssigner {
          new SerializableTimestampAssigner[(Long, String)] {
            override def extractTimestamp(element: (Long, String), recordTimestamp: Long): Long =
              element._1
          }
        }
    )

    tEnv.createTemporaryView("test", stream, Schema.newBuilder()
      .columnByMetadata("event_time", DataTypes.TIMESTAMP(3), "rowtime", true)
      .build())
    val result = tEnv.sqlQuery("SELECT * FROM test")

    val sink = new TestingAppendSink()
    tEnv.toDataStream(result).addSink(sink)
    env.execute()

    val formattedData = data.map {
      case (timestamp, data) =>
        val formattedTimestamp =
          LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()).toString
        s"$timestamp,$data,$formattedTimestamp"
    }
    assertThat(formattedData.sorted).isEqualTo(sink.getAppendResults.sorted)
  }
}
