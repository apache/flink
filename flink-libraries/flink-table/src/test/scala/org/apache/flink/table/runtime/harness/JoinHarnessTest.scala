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

import java.lang.{Integer => JInt}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.GeneratedJoinConditionFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.util.StreamExecUtil
import org.apache.flink.table.runtime.harness.HarnessTestBase.BaseRowResultSortComparator
import org.apache.flink.table.runtime.join.stream.{FullOuterJoinStreamOperator, InnerJoinStreamOperator, LeftOuterJoinStreamOperator, RightOuterJoinStreamOperator}
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler
import org.apache.flink.table.runtime.join.stream.state.`match`.JoinMatchStateHandler
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class JoinHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  private val tableConfig =
    new TableConfig().withIdleStateRetentionTime(Time.milliseconds(2), Time.milliseconds(4))

  private val rowType = new BaseRowTypeInfo(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)

  private val leftKeySelector = StreamExecUtil.getKeySelector(Array(0), rowType)
  private val rightKeySelector = StreamExecUtil.getKeySelector(Array(0), rowType)
  private val baseRow = classOf[BaseRow].getCanonicalName

  private val joinReturnType = new BaseRowTypeInfo(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)

  private val funcCode: String =
    s"""
      |public class TestJoinFunction
      |          extends org.apache.flink.table.codegen.JoinConditionFunction {
      |   @Override
      |   public boolean apply($baseRow in1, $baseRow in2) {
      |   return true;
      |   }
      |}
    """.stripMargin

  val funcCodeWithNonEqualPred: String =
    s"""
      |public class TestJoinFunction
      |          extends org.apache.flink.table.codegen.JoinConditionFunction {
      |   @Override
      |   public boolean apply($baseRow in1, $baseRow in2) {
      |    return in1.getString(1).compareTo(in2.getString(1)) > 0;
      |   }
      |}
    """.stripMargin

  val funcCodeWithNonEqualPred2: String =
    s"""
      |public class TestJoinFunction
      |          extends org.apache.flink.table.codegen.JoinConditionFunction {
      |   @Override
      |   public boolean apply($baseRow in1, $baseRow in2) {
      |    return in1.getString(1).compareTo(in2.getString(1)) < 0;
      |   }
      |}
    """.stripMargin


  @Test
  def testNonWindowInnerJoin() {

    val operator = new InnerJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCode),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()

    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    // left timer(5)
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    // left timer(5, 6)
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    // left timer(5, 6)
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi1")))
    // left timer(5, 6) right timer(7)
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "Hello1")))
    // left timer(5, 6) right timer(7, 8)
    assertEquals(4, testHarness.numProcessingTimeTimers())

    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    // left timer(6) right timer(7, 8)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi2")))
    // left timer(6) right timer(7, 8)
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val outputList = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(hOf(0, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(0, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(0, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(0, 2: JInt, "bbb", 2: JInt, "Hello1"))

    verify(expectedOutput, outputList)

    testHarness.close()
  }

  @Test
  def testNonWindowInnerJoinWithRetract() {

    val operator = new InnerJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCode),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()



    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi1")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "Hi1")))
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "Hello1")))
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi2")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "Hi2")))
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(hOf(0, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(0, 2: JInt, "bbb", 2: JInt, "Hello1"))

    verify(expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testNonWindowLeftJoinWithoutNonEqualPred() {

    val operator = new LeftOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCode),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      JoinMatchStateHandler.Type.EMPTY_MATCH,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()



    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    assertEquals(1, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    assertEquals(2, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi1")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "Hi1")))
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "Hello1")))
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi2")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "Hi2")))
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(hOf(0, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(0, 2: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(0, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(1, 2: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, 2: JInt, "bbb", 2: JInt, "Hello1"))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", null: JInt, null))

    verify(expectedOutput, result)

    testHarness.close()
  }

  @Test
  def testNonWindowLeftJoinWithNonEqualPred() {

    val operator = new LeftOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCodeWithNonEqualPred),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      JoinMatchStateHandler.Type.EMPTY_MATCH,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()



    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "bbb")))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1), 1 join cnt
    testHarness.setProcessingTime(2)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,6), 2 left key(1,2), 2 join cnt
    testHarness.setProcessingTime(3)

    // right stream input and output normally
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi1")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "bbb")))
    // 2 left timer(5,6), 2 left keys(1,2), 2 join cnt, 1 right timer(7), 1 right key(1)
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "ccc")))
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "Hello")))
    // 2 left timer(5,6), 2 left keys(1,2), 2 join cnt, 2 right timer(7,8), 2 right key(1,2)
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    // expired left stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "Hi2")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "Hi2")))
    // 1 left timer(6), 1 left keys(2), 1 join cnt, 2 right timer(7,8), 2 right key(1,2)
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all left stream record
    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired right stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(hOf(0, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(0, 2: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "aaa", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(0, 1: JInt, "bbb", 1: JInt, "Hi1"))
    expectedOutput.add(hOf(1, 2: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, 2: JInt, "bbb", 2: JInt, "Hello"))
    expectedOutput.add(hOf(1, 1: JInt, "aaa", 1: JInt, "Hi1"))

    verify(expectedOutput, result, new BaseRowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowRightJoinWithoutNonEqualPred() {

    val operator = new RightOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCode),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.EMPTY_MATCH,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()



    // right stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(3)
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // left stream input and output normally
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "Hi1")))
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "Hi1")))
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "Hello1")))
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    // expired right stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "Hi2")))
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "Hi2")))
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all right stream record
    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired left stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(hOf(0, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(1, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, 1: JInt, "Hi1", 1: JInt, "aaa"))
    expectedOutput.add(hOf(1, 1: JInt, "Hi1", 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(1, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, 2: JInt, "Hello1", 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, null: JInt, null, 1: JInt, "aaa"))

    verify(expectedOutput, result, new BaseRowResultSortComparator())

    testHarness.close()
  }

  @Test
  def testNonWindowRightJoinWithNonEqualPred() {

    val operator = new RightOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCodeWithNonEqualPred2),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.EMPTY_MATCH,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()



    // right stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "bbb")))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 right timer(5), 1 right key(1), 1 join cnt
    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 2 right timer(5,6), 2 right key(1,2), 2 join cnt
    testHarness.setProcessingTime(3)

    // left stream input and output normally
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "Hi1")))
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "bbb")))
    // 2 right timer(5,6), 2 right keys(1,2), 2 join cnt, 1 left timer(7), 1 left key(1)
    assertEquals(3, testHarness.numProcessingTimeTimers())
    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "ccc")))
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "Hello")))
    // 2 right timer(5,6), 2 right keys(1,2), 2 join cnt, 2 left timer(7,8), 2 left key(1,2)
    assertEquals(4, testHarness.numProcessingTimeTimers())

    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    // expired right stream record with key value of 1
    testHarness.setProcessingTime(5)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "Hi2")))
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "Hi2")))
    // 1 right timer(6), 1 right keys(2), 1 join cnt, 2 left timer(7,8), 2 left key(1,2)
    assertEquals(3, testHarness.numProcessingTimeTimers())

    // expired all right stream record
    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())

    // expired left stream record with key value of 2
    testHarness.setProcessingTime(8)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(hOf(0, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(1, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, null: JInt, null, 1: JInt, "bbb"))
    expectedOutput.add(hOf(0, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, null: JInt, null, 1: JInt, "bbb"))
    expectedOutput.add(hOf(1, null: JInt, null, 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, 1: JInt, "Hi1", 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, 1: JInt, "Hi1", 1: JInt, "bbb"))
    expectedOutput.add(hOf(1, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, 2: JInt, "Hello", 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, 1: JInt, "Hi1", 1: JInt, "aaa"))

    verify(expectedOutput, result, new BaseRowResultSortComparator())
    testHarness.close()
  }

  @Test
  def testNonWindowFullJoinWithoutNonEqualPred() {

    val operator = new FullOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCode),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()



    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "bbb")))
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "ccc")))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1)

    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "ccc")))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1)
    // 1 right timer(6), 1 right key(1)

    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "ddd")))
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2)
    // 1 right timer(6), 1 right key(1)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "ddd")))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2)
    // 2 right timer(6,7), 2 right key(1,2)

    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(hOf(1, 2: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(1, 2: JInt, "ddd")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "ddd")))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 1 left key(1)
    // 2 right timer(6,7), 1 right key(2)

    testHarness.setProcessingTime(5)
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 1 left timer(7)
    // 2 right timer(6,7), 1 right key(2)

    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(7)
    // 2 right timer(7)

    testHarness.setProcessingTime(7)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(8)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "bbb")))
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "bbb")))

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // processing time 1
    expectedOutput.add(hOf(0, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "ccc", null: JInt, null))
    // processing time 2
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "ccc"))
    // processing time 3
    expectedOutput.add(hOf(1, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, null: JInt, null, 2: JInt, "ccc"))
    expectedOutput.add(hOf(0, 2: JInt, "aaa", 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, 2: JInt, "aaa", 2: JInt, "ccc"))
    expectedOutput.add(hOf(0, 2: JInt, "ddd", 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, 2: JInt, "ddd", 2: JInt, "ccc"))
    expectedOutput.add(hOf(1, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "ccc", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "bbb", 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, 1: JInt, "ccc", 1: JInt, "aaa"))
    expectedOutput.add(hOf(0, 1: JInt, "bbb", 1: JInt, "ddd"))
    expectedOutput.add(hOf(0, 1: JInt, "ccc", 1: JInt, "ddd"))
    // processing time 4
    expectedOutput.add(hOf(1, 2: JInt, "aaa", 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, 2: JInt, "aaa", 2: JInt, "ccc"))
    expectedOutput.add(hOf(1, 2: JInt, "ddd", 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, 2: JInt, "ddd", 2: JInt, "ccc"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "ccc"))
    expectedOutput.add(hOf(1, 1: JInt, "bbb", 1: JInt, "aaa"))
    expectedOutput.add(hOf(1, 1: JInt, "ccc", 1: JInt, "aaa"))
    expectedOutput.add(hOf(1, 1: JInt, "bbb", 1: JInt, "ddd"))
    expectedOutput.add(hOf(1, 1: JInt, "ccc", 1: JInt, "ddd"))
    expectedOutput.add(hOf(0, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "ccc", null: JInt, null))
    // processing time 8
    expectedOutput.add(hOf(0, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))

    verify(expectedOutput, result, new BaseRowResultSortComparator())
    testHarness.close()
  }

  @Test
  def testNonWindowFullJoinWithNonEqualPred() {

    val operator = new FullOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCodeWithNonEqualPred2),
      leftKeySelector,
      rightKeySelector,
      null,
      null,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      JoinStateHandler.Type.WITHOUT_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH,
      Array[Boolean](false))

    val testHarness = createTwoInputHarnessTester(operator, leftKeySelector, rightKeySelector)

    testHarness.open()



    // left stream input
    testHarness.setProcessingTime(1)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "bbb")))
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "ccc")))
    assertEquals(1, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1), 1 left joincnt key(1)

    testHarness.setProcessingTime(2)
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "bbb")))
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "ccc")))
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(5), 1 left key(1), 1 left joincnt key(1)
    // 1 right timer(6), 1 right key(1), 1 right joincnt key(1)

    testHarness.setProcessingTime(3)
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 2: JInt, "ddd")))
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2), 2 left joincnt key(1,2)
    // 1 right timer(6), 1 right key(1), 1 right joincnt key(1)
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "ddd")))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2), 2 left joincnt key(1,2)
    // 2 right timer(6,7), 2 right key(1,2), 2 right joincnt key(1,2)

    testHarness.setProcessingTime(4)
    testHarness.processElement1(new StreamRecord(hOf(1, 2: JInt, "aaa")))
    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "ddd")))
    assertEquals(4, testHarness.numProcessingTimeTimers())
    // 2 left timer(5,7), 2 left key(1,2), 2 left joincnt key(1,2)
    // 2 right timer(6,7), 2 right key(1,2), 2 right joincnt key(1,2)

    testHarness.setProcessingTime(5)
    assertEquals(3, testHarness.numProcessingTimeTimers())
    // 1 left timer(7), 1 left key(2), 1 left joincnt key(2)
    // 2 right timer(6,7), 2 right key(1,2), 2 right joincnt key(1,2)

    testHarness.setProcessingTime(6)
    assertEquals(2, testHarness.numProcessingTimeTimers())
    // 1 left timer(7), 1 left key(2), 1 left joincnt key(2)
    // 1 right timer(7), 1 right key(2), 1 right joincnt key(2)

    testHarness.setProcessingTime(7)
    assertEquals(0, testHarness.numProcessingTimeTimers())

    testHarness.setProcessingTime(8)
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "bbb")))
    testHarness.processElement2(new StreamRecord(hOf(0, 2: JInt, "bbb")))

    val result = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)
    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // processing time 1
    expectedOutput.add(hOf(0, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "ccc", null: JInt, null))
    // processing time 2
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "ccc"))
    // processing time 3
    expectedOutput.add(hOf(1, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, null: JInt, null, 2: JInt, "ccc"))
    expectedOutput.add(hOf(0, 2: JInt, "aaa", 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, 2: JInt, "aaa", 2: JInt, "ccc"))
    // can not find matched row due to NonEquiJoinPred
    expectedOutput.add(hOf(0, 2: JInt, "ddd", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(1, 1: JInt, "ccc", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "bbb", 1: JInt, "ddd"))
    expectedOutput.add(hOf(0, 1: JInt, "ccc", 1: JInt, "ddd"))
    // can not find matched row due to NonEquiJoinPred
    expectedOutput.add(hOf(0, null: JInt, null, 1: JInt, "aaa"))
    // processing time 4
    expectedOutput.add(hOf(1, 2: JInt, "aaa", 2: JInt, "bbb"))
    expectedOutput.add(hOf(1, 2: JInt, "aaa", 2: JInt, "ccc"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "ccc"))
    expectedOutput.add(hOf(1, 1: JInt, "bbb", 1: JInt, "ddd"))
    expectedOutput.add(hOf(1, 1: JInt, "ccc", 1: JInt, "ddd"))
    expectedOutput.add(hOf(0, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, 1: JInt, "ccc", null: JInt, null))
    // processing time 8
    expectedOutput.add(hOf(0, 1: JInt, "bbb", null: JInt, null))
    expectedOutput.add(hOf(0, null: JInt, null, 2: JInt, "bbb"))

    verify(expectedOutput, result, new BaseRowResultSortComparator())
    testHarness.close()
  }

}
