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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness
import org.apache.flink.table.api.types.{DataTypes, RowType}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedJoinConditionFunction, ProjectionCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.plan.util.{JoinUtil, StreamExecUtil}
import org.apache.flink.table.runtime.join.stream.bundle.{MiniBatchAntiSemiJoinStreamOperator, MiniBatchRightOuterJoinStreamOperator}
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler
import org.apache.flink.table.runtime.join.stream.state.`match`.JoinMatchStateHandler
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.{Integer => JInt, Long => JLong}
import java.util.concurrent.ConcurrentLinkedQueue

@RunWith(classOf[Parameterized])
class MiniBatchStreamJoinHarnessTest(mode: StateBackendMode) extends HarnessTestBase(mode) {

  private val tableConfig =
    new TableConfig().withIdleStateRetentionTime(Time.milliseconds(200), Time.milliseconds
    (400))
  tableConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
  tableConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE, 5L)
  tableConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED, true)
  private val baseRow = classOf[BaseRow].getCanonicalName

  private val rowType = new BaseRowTypeInfo(
    BasicTypeInfo.INT_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)

  private val leftKeySelector = StreamExecUtil.getKeySelector(Array(0), rowType)
  private val rightKeySelector = StreamExecUtil.getKeySelector(Array(0), rowType)

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

  @Test
  def testMiniBatchSemiJoin() {

    val joinReturnType = new BaseRowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    val operator = new MiniBatchAntiSemiJoinStreamOperator(
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
      JoinMatchStateHandler.Type.ONLY_EQUALITY_CONDITION_EMPTY_MATCH,
      JoinMatchStateHandler.Type.EMPTY_MATCH,
      true,
      true,
      true,
      true,
      Array[Boolean](false),
      JoinUtil.getMiniBatchTrigger(tableConfig),
      tableConfig.getConf.getBoolean(
        TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))

    val testHarness =
      new KeyedTwoInputStreamOperatorTestHarness(
        operator,
        leftKeySelector,
        rightKeySelector,
        rightKeySelector.asInstanceOf[ResultTypeQueryable[BaseRow]].getProducedType,
        1, 1, 0)
    val typeSerializer1 = rowType.createSerializer(new ExecutionConfig)
    operator.setupTypeSerializer(typeSerializer1, typeSerializer1)
    testHarness.open()

    testHarness.processWatermark1(new Watermark(0))
    testHarness.processWatermark2(new Watermark(0))

    testHarness.processElement2(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))

    testHarness.processWatermark1(new Watermark(1))
    testHarness.processWatermark2(new Watermark(1))
    // trigger miniBatch
    testHarness.processElement1(new StreamRecord(hOf(0, 1: JInt, "aaa")))
    testHarness.processElement1(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(2))
    testHarness.processWatermark2(new Watermark(2))

    testHarness.processElement2(new StreamRecord(hOf(1, 1: JInt, "aaa")))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(3))
    testHarness.processWatermark2(new Watermark(3))
    val outputList = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(hOf(0, 1: JInt, "aaa"))
    expectedOutput.add(hOf(1, 1: JInt, "aaa"))
    verify(expectedOutput, outputList)

    testHarness.close()
  }

  @Test
  def testRightOuterJoin() {

    val rowType = new BaseRowTypeInfo(
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val joinReturnType = new BaseRowTypeInfo(
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val funcCode: String =
      s"""
        |public class TestJoinFunction
        |          extends org.apache.flink.table.codegen.JoinConditionFunction {
        |   @Override
        |   public boolean apply($baseRow in1, $baseRow in2) {
        |   return in1.getLong(0) > in2.getLong(0);
        |   }
        |}
      """.stripMargin;

    val leftKeySelector = StreamExecUtil.getKeySelector(Array(1), rowType)
    val rightKeySelector = StreamExecUtil.getKeySelector(Array(1), rowType)

    val config: TableConfig = new TableConfig
    val pkProject = ProjectionCodeGenerator.generateProjection(
      CodeGeneratorContext.apply(config, false),
      "pkProject",
      new RowType(DataTypes.LONG, DataTypes.INT),
      new RowType(DataTypes.INT),
      Array(1),
      "in1",
      "out",
      "outWriter",
      false
    )

    val operator = new MiniBatchRightOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCode),
      leftKeySelector,
      rightKeySelector,
      pkProject,
      pkProject,
      JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY,
      JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.EMPTY_MATCH,
      JoinMatchStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY_MATCH,
      true,
      true,
      Array[Boolean](false),
      JoinUtil.getMiniBatchTrigger(tableConfig),
      tableConfig.getConf.getBoolean(
        TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))

    val testHarness =
      new KeyedTwoInputStreamOperatorTestHarness(
        operator,
        leftKeySelector,
        rightKeySelector,
        rightKeySelector.asInstanceOf[ResultTypeQueryable[BaseRow]].getProducedType,
        1, 1, 0)
    val typeSerializer1 = rowType.createSerializer(new ExecutionConfig)
    operator.setupTypeSerializer(typeSerializer1, typeSerializer1)
    testHarness.open()

    testHarness.processWatermark1(new Watermark(0))
    testHarness.processWatermark2(new Watermark(0))
    testHarness.processElement2(new StreamRecord(hOf(0, 17L: JLong, 5: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(1))
    testHarness.processWatermark2(new Watermark(1))
    testHarness.processElement2(new StreamRecord(hOf(1, 17L: JLong, 5: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(2))
    testHarness.processWatermark2(new Watermark(2))
    testHarness.processElement2(new StreamRecord(hOf(0, 41L: JLong, 5: JInt)))
    testHarness.processElement1(new StreamRecord(hOf(0, 42L: JLong, 5: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(3))
    testHarness.processWatermark2(new Watermark(3))
    testHarness.processElement1(new StreamRecord(hOf(1, 42L: JLong, 5: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(4))
    testHarness.processWatermark2(new Watermark(4))
    val outputList = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    // watermark 1
    expectedOutput.add(hOf(0, null: JLong, null: JInt, 17L: JLong, 5: JInt))
    // watermark 2
    expectedOutput.add(hOf(1, null: JLong, null: JInt, 17L: JLong, 5: JInt))
    // watermark 3
    expectedOutput.add(hOf(0, 42L: JLong, 5: JInt, 41L: JLong, 5: JInt))
    // watermark 4
    expectedOutput.add(hOf(1, 42L: JLong, 5: JInt, 41L: JLong, 5: JInt))
    expectedOutput.add(hOf(0, null: JLong, null: JInt, 41L: JLong, 5: JInt))

    verify(expectedOutput, outputList)
    testHarness.close()
  }

  @Test
  def testRightOuterJoinWithJoinKeyNotContainPrimaryKeyMatchStateHandler() {

    val rowType = new BaseRowTypeInfo(
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val joinReturnType = new BaseRowTypeInfo(
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO)

    val funcCode: String =
      s"""
         |public class TestJoinFunction
         |          extends org.apache.flink.table.codegen.JoinConditionFunction {
         |   @Override
         |   public boolean apply($baseRow in1, $baseRow in2) {
         |   return true;
         |   }
         |}
      """.stripMargin;

    val leftKeySelector = StreamExecUtil.getKeySelector(Array(1), rowType)
    val rightKeySelector = StreamExecUtil.getKeySelector(Array(1), rowType)

    val config: TableConfig = new TableConfig
    val pkProject = ProjectionCodeGenerator.generateProjection(
      CodeGeneratorContext.apply(config, false),
      "pkProject",
      new RowType(DataTypes.LONG, DataTypes.INT),
      new RowType(DataTypes.LONG),
      Array(0),
      "in1",
      "out",
      "outWriter",
      false
    )

    val operator = new MiniBatchRightOuterJoinStreamOperator(
      rowType,
      rowType,
      GeneratedJoinConditionFunction("TestJoinFunction", funcCode),
      leftKeySelector,
      rightKeySelector,
      pkProject,
      pkProject,
      JoinStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY,
      JoinStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY,
      tableConfig.getMaxIdleStateRetentionTime,
      tableConfig.getMinIdleStateRetentionTime,
      JoinMatchStateHandler.Type.EMPTY_MATCH,
      JoinMatchStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY_MATCH,
      true,
      true,
      Array[Boolean](false),
      JoinUtil.getMiniBatchTrigger(tableConfig),
      tableConfig.getConf.getBoolean(
        TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))

    val testHarness =
      new KeyedTwoInputStreamOperatorTestHarness(
        operator,
        leftKeySelector,
        rightKeySelector,
        rightKeySelector.asInstanceOf[ResultTypeQueryable[BaseRow]].getProducedType,
        1, 1, 0)
    val typeSerializer1 = rowType.createSerializer(new ExecutionConfig)
    operator.setupTypeSerializer(typeSerializer1, typeSerializer1)
    testHarness.open()

    testHarness.processWatermark1(new Watermark(0))
    testHarness.processWatermark2(new Watermark(0))

    testHarness.processElement1(new StreamRecord(hOf(0, 3L: JLong, 2: JInt)))
    testHarness.processElement2(new StreamRecord(hOf(0, 3L: JLong, 2: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(1))
    testHarness.processWatermark2(new Watermark(1))

    testHarness.processElement2(new StreamRecord(hOf(0, 2L: JLong, 1: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(2))
    testHarness.processWatermark2(new Watermark(2))

    testHarness.processElement2(new StreamRecord(hOf(1, 2L: JLong, 1: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(3))
    testHarness.processWatermark2(new Watermark(3))

    testHarness.processElement1(new StreamRecord(hOf(1, 3L: JLong, 2: JInt)))
    // trigger miniBatch
    testHarness.processWatermark1(new Watermark(4))
    testHarness.processWatermark2(new Watermark(4))

    val outputList = convertStreamRecordToGenericRow(testHarness.getOutput, joinReturnType)

    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    // watermark 1
    expectedOutput.add(hOf(0, 3L: JLong, 2: JInt, 3L: JLong, 2: JInt))
    // watermark 2
    expectedOutput.add(hOf(0, null: JLong, null: JInt, 2L: JLong, 1: JInt))
    // watermark 3
    expectedOutput.add(hOf(1, null: JLong, null: JInt, 2L: JLong, 1: JInt))
    // watermark 4
    expectedOutput.add(hOf(1, 3L: JLong, 2: JInt, 3L: JLong, 2: JInt))
    expectedOutput.add(hOf(0, null: JLong, null: JInt, 3L: JLong, 2: JInt))

    verify(expectedOutput, outputList)
    testHarness.close()
  }
}
