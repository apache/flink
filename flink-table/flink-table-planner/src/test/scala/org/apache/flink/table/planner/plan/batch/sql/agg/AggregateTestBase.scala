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
package org.apache.flink.table.planner.plan.batch.sql.agg

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, Types, _}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{VarSum1AggFunction, VarSum2AggFunction}
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.TestTemplate

abstract class AggregateTestBase extends TableTestBase {

  protected val util: BatchTableTestUtil = batchTestUtil()
  util.addTableSource(
    "MyTable",
    Array[TypeInformation[_]](
      Types.BYTE,
      Types.SHORT,
      Types.INT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.BOOLEAN,
      Types.STRING,
      Types.LOCAL_DATE,
      Types.LOCAL_TIME,
      Types.LOCAL_DATE_TIME,
      DecimalDataTypeInfo.of(30, 20),
      DecimalDataTypeInfo.of(10, 5)
    ),
    Array(
      "byte",
      "short",
      "int",
      "long",
      "float",
      "double",
      "boolean",
      "string",
      "date",
      "time",
      "timestamp",
      "decimal3020",
      "decimal105")
  )
  util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)

  @TestTemplate
  def testAvg(): Unit = {
    util.verifyRelPlanWithType("""
                                 |SELECT AVG(`byte`),
                                 |       AVG(`short`),
                                 |       AVG(`int`),
                                 |       AVG(`long`),
                                 |       AVG(`float`),
                                 |       AVG(`double`),
                                 |       AVG(`decimal3020`),
                                 |       AVG(`decimal105`)
                                 |FROM MyTable
      """.stripMargin)
  }

  @TestTemplate
  def testSum(): Unit = {
    util.verifyRelPlanWithType("""
                                 |SELECT SUM(`byte`),
                                 |       SUM(`short`),
                                 |       SUM(`int`),
                                 |       SUM(`long`),
                                 |       SUM(`float`),
                                 |       SUM(`double`),
                                 |       SUM(`decimal3020`),
                                 |       SUM(`decimal105`)
                                 |FROM MyTable
      """.stripMargin)
  }

  @TestTemplate
  def testCount(): Unit = {
    util.verifyRelPlanWithType("""
                                 |SELECT COUNT(`byte`),
                                 |       COUNT(`short`),
                                 |       COUNT(`int`),
                                 |       COUNT(`long`),
                                 |       COUNT(`float`),
                                 |       COUNT(`double`),
                                 |       COUNT(`decimal3020`),
                                 |       COUNT(`decimal105`),
                                 |       COUNT(`boolean`),
                                 |       COUNT(`date`),
                                 |       COUNT(`time`),
                                 |       COUNT(`timestamp`),
                                 |       COUNT(`string`)
                                 |FROM MyTable
      """.stripMargin)
  }

  @TestTemplate
  def testCountStart(): Unit = {
    util.verifyRelPlanWithType("SELECT COUNT(*) FROM MyTable")
  }

  @TestTemplate
  def testCountStartWithProjectPushDown(): Unit = {
    // the test values table source supports projection push down by default
    util.tableEnv.executeSql("""
                               |CREATE TABLE src (
                               | id VARCHAR,
                               | cnt BIGINT
                               |) WITH (
                               | 'connector' = 'values'
                               | ,'bounded' = 'true'
                               |)
                               |""".stripMargin)
    util.verifyRelPlanWithType("SELECT COUNT(*) FROM src")
  }

  @TestTemplate
  def testCannotCountOnMultiFields(): Unit = {
    val sql = "SELECT b, COUNT(a, c) FROM MyTable1 GROUP BY b"

    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining("We now only support the count of one field")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testMinWithFixLengthType(): Unit = {
    util.verifyRelPlanWithType("""
                                 |SELECT MIN(`byte`),
                                 |       MIN(`short`),
                                 |       MIN(`int`),
                                 |       MIN(`long`),
                                 |       MIN(`float`),
                                 |       MIN(`double`),
                                 |       MIN(`decimal3020`),
                                 |       MIN(`decimal105`),
                                 |       MIN(`boolean`),
                                 |       MIN(`date`),
                                 |       MIN(`time`),
                                 |       MIN(`timestamp`)
                                 |FROM MyTable
      """.stripMargin)
  }

  @TestTemplate
  def testMinWithVariableLengthType(): Unit = {
    util.verifyRelPlanWithType("SELECT MIN(`string`) FROM MyTable")
  }

  @TestTemplate
  def testMaxWithFixLengthType(): Unit = {
    util.verifyRelPlanWithType("""
                                 |SELECT MAX(`byte`),
                                 |       MAX(`short`),
                                 |       MAX(`int`),
                                 |       MAX(`long`),
                                 |       MAX(`float`),
                                 |       MAX(`double`),
                                 |       MAX(`decimal3020`),
                                 |       MAX(`decimal105`),
                                 |       MAX(`boolean`),
                                 |       MAX(`date`),
                                 |       MAX(`time`),
                                 |       MAX(`timestamp`)
                                 |FROM MyTable
      """.stripMargin)
  }

  @TestTemplate
  def testMaxWithVariableLengthType(): Unit = {
    util.verifyRelPlanWithType("SELECT MAX(`string`) FROM MyTable")
  }

  @TestTemplate
  def testAggregateWithoutFunction(): Unit = {
    util.verifyExecPlan("SELECT a, b FROM MyTable1 GROUP BY a, b")
  }

  @TestTemplate
  def testAggregateWithoutGroupBy(): Unit = {
    util.verifyExecPlan("SELECT AVG(a), SUM(b), COUNT(c) FROM MyTable1")
  }

  @TestTemplate
  def testAggregateWithFilter(): Unit = {
    util.verifyExecPlan("SELECT AVG(a), SUM(b), COUNT(c) FROM MyTable1 WHERE a = 1")
  }

  @TestTemplate
  def testAggregateWithFilterOnNestedFields(): Unit = {
    util.addTableSource[(Int, Long, (Int, Long))]("MyTable2", 'a, 'b, 'c)
    util.verifyExecPlan("SELECT AVG(a), SUM(b), COUNT(c), SUM(c._1) FROM MyTable2 WHERE a = 1")
  }

  @TestTemplate
  def testGroupAggregate(): Unit = {
    util.verifyExecPlan("SELECT a, SUM(b), COUNT(c) FROM MyTable1 GROUP BY a")
  }

  @TestTemplate
  def testGroupAggregateWithFilter(): Unit = {
    util.verifyExecPlan("SELECT a, SUM(b), count(c) FROM MyTable1 WHERE a = 1 GROUP BY a")
  }

  @TestTemplate
  def testAggNotSupportMerge(): Unit = {
    util.addTemporarySystemFunction("var_sum", new VarSum2AggFunction)
    util.verifyExecPlan("SELECT b, var_sum(a) FROM MyTable1 GROUP BY b")
  }

  @TestTemplate
  def testPojoAccumulator(): Unit = {
    util.addTemporarySystemFunction("var_sum", new VarSum1AggFunction)
    util.verifyExecPlan("SELECT b, var_sum(a) FROM MyTable1 GROUP BY b")
  }

  @TestTemplate
  def testGroupByWithConstantKey(): Unit = {
    val sql =
      """
        |SELECT a, MAX(b), c FROM (SELECT a, 'test' AS c, b FROM MyTable1) t GROUP BY a, c
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  // TODO supports group sets
}
