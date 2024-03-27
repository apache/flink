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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{JavaUserDefinedTableAggFunctions, StreamingWithStateTestBase, TestData, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.OverloadedDoubleMaxFunction
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData.tupleData3
import org.apache.flink.table.planner.utils.{TableAggSum, Top3, Top3Accum, Top3WithMapView, Top3WithRetractInput}
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.time.Duration

/** Tests of groupby (without window) table aggregations */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class TableAggregateITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    tEnv.getConfig.setIdleStateRetention(Duration.ofHours(1))
    // Create a Table from the array of Rows
    tEnv.executeSql(s"""
                       |CREATE TABLE myTable (
                       |  `id` INT,
                       |  `name` STRING,
                       |  `price` INT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '${TestValuesTableFactory.registerData(TestData.tupleData4)}'
                       |)
                       |""".stripMargin)
  }

  @TestTemplate
  def testFlatAggregateWithoutIncrementalUpdate(): Unit = {
    // Register the table aggregate function which does not implement emitUpdateWithRetract
    tEnv.createTemporarySystemFunction("top2", new JavaUserDefinedTableAggFunctions.Top2)

    checkRank(
      "top2",
      List(
        // output triggered by (1, "Latte", 6)
        "(true,6,1)",
        // output triggered by (2, "Milk", 3)
        "(false,6,1)",
        "(true,6,1)",
        "(true,3,2)",
        // output triggered by (3, "Breve", 5)
        "(false,6,1)",
        "(false,3,2)",
        "(true,6,1)",
        "(true,5,2)",
        // output triggered by (4, "Mocha", 8)
        "(false,6,1)",
        "(false,5,2)",
        "(true,8,1)",
        "(true,6,2)",
        // output triggered by (5, "Tea", 4)
        "(false,8,1)",
        "(false,6,2)",
        "(true,8,1)",
        "(true,6,2)"
      )
    )
  }

  @TestTemplate
  def testFlatAggregateWithIncrementalUpdate(): Unit = {
    tEnv.createTemporarySystemFunction(
      "incrementalTop2",
      new JavaUserDefinedTableAggFunctions.IncrementalTop2)
    checkRank(
      "incrementalTop2",
      List(
        // output triggered by (1, "Latte", 6)
        "(true,6,1)",
        // output triggered by (2, "Milk", 3)
        "(true,3,2)",
        // output triggered by (3, "Breve", 5)
        "(false,3,2)",
        "(true,5,2)",
        // output triggered by (4, "Mocha", 8)
        "(false,6,1)",
        "(true,8,1)",
        "(false,5,2)",
        "(true,6,2)"
      )
    )
  }

  def checkRank(func: String, expectedResult: List[String]): Unit = {
    val resultTable =
      tEnv
        .from("myTable")
        .flatAggregate(call(func, $("price")).as("top_price", "rank"))
        .select($("top_price"), $("rank"))

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    assertThat(sink.getRawResults).isEqualTo(expectedResult)
  }

  @TestTemplate
  def testGroupByFlatAggregate(): Unit = {
    val top3 = new Top3

    val resultTable = failingDataSource(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as("category", "v1", "v2")

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1,1",
      "2,2,2",
      "2,3,3",
      "3,4,4",
      "3,5,5",
      "3,6,6",
      "4,10,10",
      "4,9,9",
      "4,8,8",
      "5,15,15",
      "5,14,14",
      "5,13,13",
      "6,21,21",
      "6,20,20",
      "6,19,19"
    ).sorted
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected)
  }

  @TestTemplate
  def testNonkeyedFlatAggregate(): Unit = {

    val top3 = new Top3
    val source = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .flatAggregate(top3('a))
      .select('f0, 'f1)
      .as("v1", "v2")

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "19,19",
      "20,20",
      "21,21"
    ).sorted
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected)
  }

  @TestTemplate
  def testAggregateAfterTableAggregate(): Unit = {
    val top3 = new Top3

    val resultTable = failingDataSource(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as("category", "v1", "v2")
      .groupBy('category)
      .select('category, 'v1.max)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "2,3",
      "3,6",
      "4,10",
      "5,15",
      "6,21"
    ).sorted
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected)
  }

  @TestTemplate
  def testGroupByFlatAggregateWithMapView(): Unit = {
    val top3 = new Top3WithMapView

    val resultTable = failingDataSource(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as("category", "v1", "v2")

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1,1",
      "2,2,2",
      "2,3,3",
      "3,4,4",
      "3,5,5",
      "3,6,6",
      "4,10,10",
      "4,9,9",
      "4,8,8",
      "5,15,15",
      "5,14,14",
      "5,13,13",
      "6,21,21",
      "6,20,20",
      "6,19,19"
    ).sorted
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected)
  }

  @TestTemplate
  def testInputWithRetraction(): Unit = {

    val top3 = new Top3WithRetractInput
    val source = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .groupBy('b)
      .select('b, 'a.sum.as('a))
      .flatAggregate(call(top3, 'a).as('v1, 'v2))
      .select('v1, 'v2)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "111,111",
      "65,65",
      "34,34"
    ).sorted
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected)
  }

  @TestTemplate
  def testInternalAccumulatorType(): Unit = {
    val source = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .groupBy('b)
      .flatAggregate(call(classOf[TableAggSum], 'a).as('sum))
      .select('b, 'sum)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "6,111",
      "6,111",
      "5,65",
      "5,65",
      "4,34",
      "4,34",
      "3,15",
      "3,15",
      "2,5",
      "2,5",
      "1,1",
      "1,1").sorted
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected)
  }

  @TestTemplate
  def testTableAggFunctionWithoutRetractionMethod(): Unit = {
    val top3 = new Top3
    val source = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)

    assertThatThrownBy(
      () => {
        source
          .groupBy('b)
          .select('b, 'a.sum.as('a))
          .flatAggregate(top3('a).as('v1, 'v2))
          .select('v1, 'v2)
          .toRetractStream[Row]

        env.execute()
      })
      .hasMessage(
        s"Could not find an implementation method 'retract' in class '${classOf[Top3].getName}' " +
          s"for function 'Top3' that matches the following signature:\n" +
          s"void retract(${classOf[Top3Accum].getName}, java.lang.Integer)")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testOverloadedAccumulator(): Unit = {
    val source = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)

    val sink1 = new TestingRetractSink()
    source
      .groupBy('b)
      .flatAggregate(call(classOf[OverloadedDoubleMaxFunction], 'a).as('max))
      .select('b, 'max)
      .toRetractStream[Row]
      .addSink(sink1)
      .setParallelism(1)

    val sink2 = new TestingRetractSink()
    source
      .select(('a.cast(DataTypes.STRING()) + "str").as('a), 'b)
      .groupBy('b)
      .flatAggregate(call(classOf[OverloadedDoubleMaxFunction], 'a).as('max))
      .select('b, 'max)
      .toRetractStream[Row]
      .addSink(sink2)
      .setParallelism(1)

    env.execute()

    val expected1 = List(
      "1,1",
      "1,1",
      "2,3",
      "2,3",
      "3,6",
      "3,6",
      "4,10",
      "4,10",
      "5,15",
      "5,15",
      "6,21",
      "6,21"
    )
    assertThat(sink1.getRetractResults.sorted).isEqualTo(expected1.sorted)

    val expected2 = List(
      "1,1str",
      "1,1str",
      "2,3str",
      "2,3str",
      "3,6str",
      "3,6str",
      "4,9str",
      "4,9str",
      "5,15str",
      "5,15str",
      "6,21str",
      "6,21str"
    )
    assertThat(sink2.getRetractResults.sorted).isEqualTo(expected2.sorted)
  }
}
