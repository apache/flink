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

package org.apache.flink.table.planner.`match`

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.data.RowData
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecMatch}
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.table.types.logical.{IntType, RowType}
import org.apache.flink.types.Row
import org.apache.flink.util.TestLogger

import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RelBuilder
import org.junit.Assert._
import org.junit.rules.ExpectedException
import org.junit.{ComparisonFailure, Rule}
import org.mockito.Mockito.{mock, when}

abstract class PatternTranslatorTestBase extends TestLogger {

  private val expectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  // setup test utils
  private val testTableTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO)
  private val testTableRowType = RowType.of(new IntType)
  private val tableName = "testTable"
  private val context = prepareContext(testTableTypeInfo)
  private val calcitePlanner: FlinkPlannerImpl = context._2.createFlinkPlanner
  private val parser = context._2.plannerContext.createCalciteParser()

  private def prepareContext(typeInfo: TypeInformation[Row])
  : (RelBuilder, PlannerBase, StreamExecutionEnvironment) = {
    // create DataStreamTable
    val dataStreamMock = mock(classOf[DataStream[Row]])
    val jDataStreamMock = mock(classOf[JDataStream[Row]])
    when(dataStreamMock.javaStream).thenReturn(jDataStreamMock)
    when(jDataStreamMock.getType).thenReturn(typeInfo)
    when(jDataStreamMock.getId).thenReturn(0)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    TableTestUtil.createTemporaryView(
      tEnv, tableName, dataStreamMock.javaStream, Some(Array[Expression]('f0, 'proctime.proctime)))

    // prepare RelBuilder
    val planner = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
    val relBuilder: RelBuilder = planner.getRelBuilder
    relBuilder.scan(tableName)

    (relBuilder, planner, env)
  }

  def verifyPattern(matchRecognize: String, expected: Pattern[RowData, _ <: RowData]): Unit = {
    // create RelNode from SQL expression
    val parsed = parser.parse(
      s"""
         |SELECT *
         |FROM $tableName
         |$matchRecognize
         |""".stripMargin)
    val validated = calcitePlanner.validate(parsed)
    val converted = calcitePlanner.rel(validated).rel

    val plannerBase = context._2
    val optimized: RelNode = plannerBase.optimize(Seq(converted)).head

    // throw exception if plan contains more than a match
    // the plan should be: StreamExecMatch -> StreamExecExchange -> StreamExecDataStreamScan
    if (!optimized.getInput(0).getInput(0).isInstanceOf[StreamExecDataStreamScan]) {
      fail("Expression is converted into more than a Match operation. Use a different test method.")
    }

    val dataMatch = optimized.asInstanceOf[StreamExecMatch]
    val p = dataMatch.translatePattern(new TableConfig, context._1, testTableRowType)._1

    compare(expected, p)
  }

  private def compare(
      expected: Pattern[RowData, _ <: RowData],
      actual: Pattern[RowData, _ <: RowData]): Unit = {
    var currentLeft = expected
    var currentRight = actual
    do {
      val sameName = currentLeft.getName == currentRight.getName
      val sameQuantifier = currentLeft.getQuantifier == currentRight.getQuantifier
      val sameTimes = currentLeft.getTimes == currentRight.getTimes
      val sameSkipStrategy = currentLeft.getAfterMatchSkipStrategy ==
        currentRight.getAfterMatchSkipStrategy

      val sameTimeWindow = if (currentLeft.getWindowTime != null && currentRight != null) {
        currentLeft.getWindowTime.toMilliseconds == currentRight.getWindowTime.toMilliseconds
      } else {
        currentLeft.getWindowTime == null && currentRight.getWindowTime == null
      }

      currentLeft = currentLeft.getPrevious
      currentRight = currentRight.getPrevious

      if (!sameName || !sameQuantifier || !sameTimes || !sameSkipStrategy || !sameTimeWindow) {
        throw new ComparisonFailure("Compiled different pattern.",
          expected.toString,
          actual.toString)
      }

    } while (currentLeft != null)

    if (currentRight != null) {
      throw new ComparisonFailure("Compiled different pattern.", expected.toString, actual.toString)
    }
  }
}
