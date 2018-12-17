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

import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, TestHarnessUtil}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.junit.Assert.assertTrue
import org.junit.Test

class SortProcessFunctionHarnessTest extends HarnessTestBase {

  @Test
  def testSortProcTimeHarnessPartitioned(): Unit = {
    val testHarness = createTestHarness(isRowtime = false)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

   testHarness.setProcessingTime(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong), true)))

    //move the timestamp to ensure the execution
    testHarness.setProcessingTime(1005)

    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong), true)))

    testHarness.setProcessingTime(1008)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    // (1,0) (2,0)

     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 11L: JLong),true)))
     expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 2: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 0: JInt, "aaa", 11L: JLong),true)))

    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 1L: JLong, 0: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
        Row.of(1: JInt, 2L: JLong, 0: JInt, "aaa", 11L: JLong),true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 3L: JLong, 0: JInt, "aaa", 11L: JLong),true)))

    TestHarnessUtil.assertOutputEquals("Output was not correctly sorted.", expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testSortRowTimeHarnessPartitioned(): Unit = {
    val testHarness = createTestHarness(isRowtime = true)

    val operator = getOperator(testHarness)
    assertTrue(operator.getKeyedStateBackend.isInstanceOf[RocksDBKeyedStateBackend[_]])

   testHarness.processWatermark(3)

      // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1001L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 13L: JLong, 2: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2004L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2006L: JLong), true)))

    // move watermark forward
    testHarness.processWatermark(2007)

    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 1: JInt, "aaa", 2008L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong), true))) // too late
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2019L: JLong), true))) // too early
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 2: JInt, "aaa", 2008L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2010L: JLong), true)))
    testHarness.processElement(new StreamRecord(new CRow(
      Row.of(1: JInt, 19L: JLong, 0: JInt, "aaa", 2008L: JLong), true)))

    // move watermark forward
    testHarness.processWatermark(2012)

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // all elements at the same proc timestamp have the same value
    // elements should be sorted ascending on field 1 and descending on field 2
    // (10,0) (11,1) (12,2) (12,1) (12,0)
    expectedOutput.add(new Watermark(3))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 11L: JLong, 1: JInt, "aaa", 1001L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 1: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 13L: JLong, 2: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 14L: JLong, 0: JInt, "aaa", 2002L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 12L: JLong, 3: JInt, "aaa", 2004L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2006L: JLong), true)))
    expectedOutput.add(new Watermark(2007))

    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 19L: JLong, 0: JInt, "aaa", 2008L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 2: JInt, "aaa", 2008L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 20L: JLong, 1: JInt, "aaa", 2008L: JLong), true)))
    expectedOutput.add(new StreamRecord(new CRow(
      Row.of(1: JInt, 10L: JLong, 0: JInt, "aaa", 2010L: JLong), true)))

    expectedOutput.add(new Watermark(2012))

    TestHarnessUtil.assertOutputEquals("Output was not correctly sorted.", expectedOutput, result)
        
    testHarness.close()
        
  }

  private def createTestHarness(isRowtime: Boolean)
      : KeyedOneInputStreamOperatorTestHarness[String, CRow, CRow] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    if (isRowtime) {
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    }
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val data = Seq[(JInt, JLong, JInt, String)]()

    val timeAttributeField = if (isRowtime) "rowtime" else "proctime"
    val timeAttributeFieldExpr = if (isRowtime) {
      symbol2FieldExpression(Symbol(timeAttributeField)).rowtime
    } else {
      symbol2FieldExpression(Symbol(timeAttributeField)).proctime
    }
    val t = env.fromCollection(data).toTable(
      tEnv, 'a, 'b, 'c, 'd, timeAttributeFieldExpr)
    tEnv.registerTable("T", t)
    val sqlQuery = tEnv.sqlQuery(
      s"""
         |SELECT *
         |FROM T
         |ORDER BY $timeAttributeField, b ASC, c DESC
         |""".stripMargin)

    val testHarness = createHarnessTester[String, CRow, CRow](
      tEnv.toAppendStream[Row](sqlQuery), "Process")

    testHarness.setStateBackend(getStateBackend)
    testHarness.open()

    testHarness
  }
}
