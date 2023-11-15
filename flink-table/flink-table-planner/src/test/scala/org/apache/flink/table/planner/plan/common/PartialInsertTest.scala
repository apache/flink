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
package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.assertj.core.api.Assertions.{assertThatExceptionOfType, assertThatThrownBy}
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class PartialInsertTest(isBatch: Boolean) extends TableTestBase {

  private val util = if (isBatch) batchTestUtil() else streamTestUtil()
  util.addTableSource[(Int, String, String, String, Double)]("MyTable", 'a, 'b, 'c, 'd, 'e)
  util.tableEnv.executeSql(s"""
                              |create table sink (
                              |  `a` INT,
                              |  `b` STRING,
                              |  `c` STRING,
                              |  `d` STRING,
                              |  `e` DOUBLE,
                              |  `f` BIGINT,
                              |  `g` INT
                              |) with (
                              |  'connector' = 'values',
                              |  'sink-insert-only' = 'false'
                              |)
                              |""".stripMargin)
  util.tableEnv.executeSql(s"""
                              |create table partitioned_sink (
                              |  `a` INT,
                              |  `b` AS `a` + 1,
                              |  `c` STRING,
                              |  `d` STRING,
                              |  `e` DOUBLE,
                              |  `f` BIGINT,
                              |  `g` INT
                              |) PARTITIONED BY (`c`, `d`) with (
                              |  'connector' = 'values',
                              |  'sink-insert-only' = 'false'
                              |)
                              |""".stripMargin)
  util.tableEnv.executeSql(s"""
                              |create table complex_type_src (
                              |  `a` BIGINT,
                              |  `b` ROW<b1 STRING, b2 INT>,
                              |  `c` ROW<c1 BIGINT, c2 STRING>,
                              |  `d` MAP<STRING, STRING>,
                              |  `e` DOUBLE,
                              |  `f` BIGINT,
                              |  `g` INT
                              |) with (
                              |  'connector' = 'values'
                              |)
                              |""".stripMargin)
  util.tableEnv.executeSql(s"""
                              |create table complex_type_sink (
                              |  `a` BIGINT,
                              |  `b` ROW<b1 STRING, b2 INT>,
                              |  `c` ROW<c1 BIGINT, c2 STRING>,
                              |  `d` MAP<STRING, STRING>,
                              |  `e` DOUBLE,
                              |  `f` BIGINT METADATA,
                              |  `g` INT,
                              |  primary key (`a`) not enforced
                              |) with (
                              |  'connector' = 'values',
                              |  'sink-insert-only' = 'false',
                              |  'writable-metadata' = 'f:bigint'
                              |)
                              |""".stripMargin)

  util.tableEnv.executeSql(s"""create table metadata_sink (
                              |  `a` INT,
                              |  `b` STRING,
                              |  `c` STRING,
                              |  `d` STRING,
                              |  `e` DOUBLE,
                              |  `f` BIGINT METADATA,
                              |  `g` INT METADATA VIRTUAL,
                              |  `h` AS `a` + 1
                              |) with (
                              |  'connector' = 'values',
                              |  'sink-insert-only' = 'false',
                              |  'writable-metadata' = 'f:BIGINT, g:INT'
                              |)""".stripMargin)

  @TestTemplate
  def testPartialInsertWithComplexReorder(): Unit = {
    util.verifyRelPlanInsert(
      "INSERT INTO sink (b,e,a,g,f,c,d) " +
        "SELECT b,e,a,456,123,c,d FROM MyTable GROUP BY a,b,c,d,e")
  }

  @TestTemplate
  def testPartialInsertWithComplexReorderAndComputedColumn(): Unit = {
    util.verifyRelPlanInsert(
      "INSERT INTO partitioned_sink (e,a,g,f,c,d) " +
        "SELECT e,a,456,123,c,d FROM MyTable GROUP BY a,b,c,d,e")
  }

  @TestTemplate
  def testPartialInsertWithUnion(): Unit = {
    testPartialInsertWithSetOperator("UNION")
  }

  @TestTemplate
  def testPartialInsertWithUnionAll(): Unit = {
    testPartialInsertWithSetOperator("UNION ALL")
  }

  @TestTemplate
  def testPartialInsertWithIntersectAll(): Unit = {
    testPartialInsertWithSetOperator("INTERSECT ALL")
  }

  @TestTemplate
  def testPartialInsertWithExceptAll(): Unit = {
    testPartialInsertWithSetOperator("EXCEPT ALL")
  }

  private def testPartialInsertWithSetOperator(operator: String): Unit = {
    util.verifyRelPlanInsert(
      "INSERT INTO partitioned_sink (e,a,g,f,c,d) " +
        "SELECT e,a,456,123,c,d FROM MyTable GROUP BY a,b,c,d,e " +
        operator + " " +
        "SELECT e,a,789,456,c,d FROM MyTable GROUP BY a,b,c,d,e ")
  }

  @TestTemplate
  def testPartialInsertWithUnionAllNested(): Unit = {
    util.verifyRelPlanInsert(
      "INSERT INTO partitioned_sink (e,a,g,f,c,d) " +
        "SELECT e,a,456,123,c,d FROM MyTable GROUP BY a,b,c,d,e " +
        "UNION ALL " +
        "SELECT e,a,789,456,c,d FROM MyTable GROUP BY a,b,c,d,e " +
        "UNION ALL " +
        "SELECT e,a,123,456,c,d FROM MyTable GROUP BY a,b,c,d,e ")
  }

  @TestTemplate
  def testPartialInsertWithOrderBy(): Unit = {
    util.verifyRelPlanInsert(
      "INSERT INTO partitioned_sink (e,a,g,f,c,d) " +
        "SELECT e,a,456,123,c,d FROM MyTable ORDER BY a,e,c,d")
  }

  @TestTemplate
  def testPartialInsertWithPersistedMetadata(): Unit = {
    util.verifyRelPlanInsert(
      "INSERT INTO metadata_sink (a,b,c,d,e,f) " +
        "SELECT a,b,c,d,e,123 FROM MyTable"
    )
  }

  @TestTemplate
  def testPartialInsertWithVirtualMetaDataColumn(): Unit = {
    assertThatThrownBy(
      () =>
        util.verifyRelPlanInsert(
          "INSERT INTO metadata_sink (a,b,c,d,e,g) " +
            "SELECT a,b,c,d,e,123 FROM MyTable"
        ))
      .hasMessageContaining(
        "SQL validation failed. At line 1, column 38: Unknown target column 'g'")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testPartialInsertWithComputedColumn(): Unit = {
    assertThatThrownBy(
      () =>
        util.verifyRelPlanInsert(
          "INSERT INTO metadata_sink (a,b,c,d,e,h) " +
            "SELECT a,b,c,d,e,123 FROM MyTable"
        ))
      .hasMessageContaining(
        "SQL validation failed. At line 1, column 38: Unknown target column 'h'")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testPartialInsertWithGroupBy(): Unit = {
    util.verifyExplainInsert(
      "INSERT INTO partitioned_sink (e,a,d) " +
        "SELECT e,a,d FROM MyTable GROUP BY a,b,c,d,e")
  }

  @TestTemplate
  def testPartialInsertCompositeType(): Unit = {
    // TODO this should be updated after FLINK-31301 fixed
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          util.verifyExplainInsert(
            "INSERT INTO complex_type_sink (a,b.b1,c.c2,f) " +
              "SELECT a,b.b1,c.c2,f FROM complex_type_src"))
  }
}

object PartialInsertTest {
  @Parameters(name = "isBatch: {0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
