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

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.calcite.FlinkPlannerImpl
import org.apache.flink.table.plan.nodes.datastream.{DataStreamMatch, DataStreamScan, PatternVisitor}
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
  private val planner = new FlinkPlannerImpl(
    context._2.getFrameworkConfig,
    context._2.getPlanner,
    context._2.getTypeFactory)

  private def prepareContext(typeInfo: TypeInformation[Row])
  : (RelBuilder, StreamTableEnvironment, StreamExecutionEnvironment) = {
    // create DataStreamTable
    val dataStreamMock = mock(classOf[DataStream[Row]])
    val jDataStreamMock = mock(classOf[JDataStream[Row]])
    when(dataStreamMock.javaStream).thenReturn(jDataStreamMock)
    when(jDataStreamMock.getType).thenReturn(typeInfo)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerDataStream(tableName, dataStreamMock, 'f0, 'proctime.proctime)

    // prepare RelBuilder
    val relBuilder = tEnv.getRelBuilder
    relBuilder.scan(tableName)

    (relBuilder, tEnv, env)
  }

  def verifyPattern(matchRecognize: String, expected: Pattern[Row, _ <: Row]): Unit = {
    // create RelNode from SQL expression
    val parsed = planner.parse(
      s"""
         |SELECT *
         |FROM $tableName
         |$matchRecognize
         |""".stripMargin)
    val validated = planner.validate(parsed)
    val converted = planner.rel(validated).rel

    val env = context._2
    val optimized = env.optimize(converted, updatesAsRetraction = false)

    // throw exception if plan contains more than a match
    if (!optimized.getInput(0).isInstanceOf[DataStreamScan]) {
      fail("Expression is converted into more than a Match operation. Use a different test method.")
    }

    val dataMatch = optimized
      .asInstanceOf[DataStreamMatch]

    val pVisitor = new PatternVisitor(new TableConfig, testTableTypeInfo, dataMatch.getLogicalMatch)
    val p = dataMatch.getLogicalMatch.pattern.accept(pVisitor)

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

      currentLeft = currentLeft.getPrevious
      currentRight = currentRight.getPrevious

      if (!sameName || !sameQuantifier || !sameTimes || !sameSkipStrategy) {
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
