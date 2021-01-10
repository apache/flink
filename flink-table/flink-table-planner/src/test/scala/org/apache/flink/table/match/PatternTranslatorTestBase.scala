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

package org.apache.flink.table.`match`

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.plan.nodes.datastream.{DataStreamMatch, DataStreamScan}
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.types.Row
import org.apache.flink.util.TestLogger

import org.junit.Assert._
import org.junit.rules.ExpectedException
import org.junit.{ComparisonFailure, Rule}
import org.mockito.Mockito.{mock, when}

abstract class PatternTranslatorTestBase extends TestLogger{

  private val expectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  // setup test utils
  private val testTableTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO)
  private val tableName = "testTable"
  private val context = prepareContext(testTableTypeInfo)

  private def prepareContext(typeInfo: TypeInformation[Row])
  : (StreamTableEnvironment, StreamExecutionEnvironment, StreamPlanner) = {
    // create DataStreamTable
    val dataStreamMock = mock(classOf[DataStream[Row]])
    val jDataStreamMock = mock(classOf[JDataStream[Row]])
    when(dataStreamMock.javaStream).thenReturn(jDataStreamMock)
    when(jDataStreamMock.getType).thenReturn(typeInfo)
    when(jDataStreamMock.getId).thenReturn(0)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings).asInstanceOf[StreamTableEnvironmentImpl]
    tEnv.createTemporaryView(tableName, dataStreamMock, 'f0, 'proctime.proctime)

    val streamPlanner = tEnv.getPlanner.asInstanceOf[StreamPlanner]

    (tEnv, env, streamPlanner)
  }

  def verifyPattern(matchRecognize: String, expected: Pattern[Row, _ <: Row]): Unit = {
    // create RelNode from SQL expression
    val parsed = context._3.getParser.parse(
      s"""
         |SELECT *
         |FROM $tableName
         |$matchRecognize
         |""".stripMargin)

    val queryOperation = parsed.get(0).asInstanceOf[QueryOperation]
    val relNode = context._3.getRelBuilder.tableOperation(queryOperation).build()

    val optimized = context._3.optimizer
      .optimize(
        relNode,
        updatesAsRetraction = false,
        context._3.getRelBuilder)

    // throw exception if plan contains more than a match
    if (!optimized.getInput(0).isInstanceOf[DataStreamScan]) {
      fail("Expression is converted into more than a Match operation. Use a different test method.")
    }

    val dataMatch = optimized.asInstanceOf[DataStreamMatch]
    val p = dataMatch.translatePattern(new TableConfig, testTableTypeInfo)._1

    compare(expected, p)
  }

  private def compare(expected: Pattern[Row, _ <: Row], actual: Pattern[Row, _ <: Row]): Unit = {
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
