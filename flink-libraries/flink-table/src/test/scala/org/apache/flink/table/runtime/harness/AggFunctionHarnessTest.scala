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

package org.apache.flink.table.runtime.harness

import java.lang.{Integer => JInt, Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.operators.{AbstractUdfStreamOperator, LegacyKeyedProcessOperator}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.dataview.{DataView, MapView}
import org.apache.flink.table.runtime.aggregate.{GeneratedAggregations, GroupAggProcessFunction}
import org.apache.flink.table.runtime.harness.HarnessTestBase.{TestStreamQueryConfig, TupleRowKeySelector}
import org.apache.flink.table.runtime.types.CRow
import org.junit.Test

import scala.collection.JavaConverters._

class AggFunctionHarnessTest extends HarnessTestBase {
  protected var queryConfig =
    new TestStreamQueryConfig(Time.seconds(0), Time.seconds(0))

  @Test
  def testCollectAggregate(): Unit = {
    val processFunction = new LegacyKeyedProcessOperator[String, CRow, CRow](
      new GroupAggProcessFunction(
        genCollectAggFunction,
        collectAggregationStateType,
        false,
        queryConfig))

    val testHarness = createHarnessTester(
      processFunction,
      new TupleRowKeySelector[String](2),
      BasicTypeInfo.STRING_TYPE_INFO)
    testHarness.setStateBackend(getStateBackend)

    testHarness.open()

    val state = getState(processFunction, "mapView").asInstanceOf[MapView[JInt, JInt]]

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    testHarness.processElement(new StreamRecord(CRow(1L: JLong, 1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", Map(1 -> 1).asJava), 1))
    testHarness.processElement(new StreamRecord(CRow(2L: JLong, 1: JInt, "bbb"), 1))
    expectedOutput.add(new StreamRecord(CRow("bbb", Map(1 -> 1).asJava), 1))

    testHarness.processElement(new StreamRecord(CRow(3L: JLong, 1: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", Map(1 -> 2).asJava), 1))
    testHarness.processElement(new StreamRecord(CRow(4L: JLong, 2: JInt, "aaa"), 1))
    expectedOutput.add(new StreamRecord(CRow("aaa", Map(1 -> 2, 2 -> 1).asJava), 1))

    // remove some state: state may be cleaned up by the state backend if not accessed more than ttl
    processFunction.setCurrentKey("aaa")
    state.remove(2)

    // retract after state has been cleaned up
    testHarness.processElement(new StreamRecord(CRow(false, 5L: JLong, 2: JInt, "aaa"), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)

    testHarness.close()
  }

  private def getState(
      operator: AbstractUdfStreamOperator[_, _],
      stateFieldName: String): DataView = {
    val field = classOf[GroupAggProcessFunction].getDeclaredField("function")
    field.setAccessible(true)
    val generatedAggregation =
      field.get(operator.getUserFunction).asInstanceOf[GeneratedAggregations]
    generatedAggregation.getClass.getDeclaredField(stateFieldName)
      .get(generatedAggregation).asInstanceOf[DataView]
  }
}
