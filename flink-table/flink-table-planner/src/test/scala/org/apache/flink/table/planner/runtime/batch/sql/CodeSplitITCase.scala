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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData.{nullablesOfData3, smallData3, type3}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.IterableAssert.assertThatIterable
import org.junit.jupiter.api.{BeforeEach, Test}

class CodeSplitITCase extends BatchTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfData3)
  }

  @Test
  def testSelectManyColumns(): Unit = {
    val sql = new StringBuilder("SELECT ")
    for (i <- 1 to 1000) {
      sql.append(s"a + $i * b, ")
    }
    sql.append("a, b FROM SmallTable3")

    val results = new scala.collection.mutable.ArrayBuffer[Row]()
    for ((a, b) <- Seq((1, 1), (2, 2), (3, 2))) {
      val r = new Row(1002)
      for (i <- 1 to 1000) {
        r.setField(i - 1, a + i * b)
      }
      r.setField(1000, a)
      r.setField(1001, b)
      results.append(r)
    }

    runTest(sql.mkString, results)
  }

  @Test
  def testManyOrsInCondition(): Unit = {
    val sql = new StringBuilder("SELECT a, b FROM SmallTable3 WHERE ")
    for (i <- 1 to 300) {
      sql.append(s"(a + b > $i AND a * b > $i) OR ")
    }
    sql.append("CAST((a + b > 1 AND a * b > 1) AS VARCHAR) = 'true'")

    runTest(sql.mkString, Seq(row(2, 2), row(3, 2)))
  }

  @Test
  def testManyAggregations(): Unit = {
    val sql = new StringBuilder("SELECT ")
    for (i <- 1 to 300) {
      sql.append(s"SUM(a + $i * b)")
      if (i != 300) {
        sql.append(", ")
      }
    }
    sql.append(" FROM SmallTable3")

    val result = new Row(300)
    for (i <- 1 to 300) {
      result.setField(i - 1, 6 + 5 * i)
    }

    runTest(sql.mkString, Seq(result))
  }

  @Test
  def testManyValues(): Unit = {
    tEnv
      .executeSql(
        s"""
           |CREATE TABLE test_many_values (
           |${Range(0, 100).map(i => s"  f$i INT").mkString(",\n")}
           |) WITH (
           |  'connector' = 'values'
           |)
           |""".stripMargin
      )
      .await()

    tEnv
      .executeSql(
        s"""
           |INSERT INTO test_many_values VALUES
           |${Range(0, 100)
            .map(i => "(" + Range(0, 100).map(_ => s"$i").mkString(", ") + ")")
            .mkString(", ")}
           |""".stripMargin
      )
      .await()

    val expected = new java.util.ArrayList[String]()
    for (i <- 0 until 100) {
      expected.add(s"+I[${Range(0, 100).map(_ => s"$i").mkString(", ")}]")
    }
    assertThatIterable(TestValuesTableFactory.getResultsAsStrings("test_many_values"))
      .containsExactlyElementsOf(expected)
  }

  /**
   * This tests replicates a production query that was causing FLINK-27246. The generated code
   * contains long WHILE statements followed by nested IF/ELSE statements which original CodeSplit
   * logic was unable to rewrite causing compilation error on processElement(..) and endInput()
   * methods being to big.
   */
  @Test
  def testManyAggregationsWithGroupBy(): Unit = {

    tEnv.getConfig.set(TableConfigOptions.MAX_LENGTH_GENERATED_CODE, Int.box(4000))
    tEnv.getConfig.set(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, Int.box(10000))

    tEnv
      .executeSql(
        s"""
           |CREATE TABLE test_many_values (
           |`ID` INT,
           |${Range.inclusive(1, 250).map(i => s"  a_$i INT").mkString(",\n")}
           |) WITH (
           |  'connector' = 'datagen',
           |  'number-of-rows' = '10',
           |  'fields.ID.kind' = 'sequence',
           |  'fields.ID.start' = '1',
           |  'fields.ID.end' = '10',
           |${Range.inclusive(1, 250).map(i => s"  'fields.a_$i.kind' = 'sequence'").mkString(",\n")},
           |${Range.inclusive(1, 250).map(i => s"  'fields.a_$i.start' = '1'").mkString(",\n")},
           |${Range.inclusive(1, 250).map(i => s"  'fields.a_$i.end' = '10'").mkString(",\n")}
           |)
           |""".stripMargin
      )
      .await()

    val sqlQuery =
      s"""
         |SELECT
         |${Range.inclusive(1, 250).map(i => s"  SUM(a_$i) as a_$i").mkString(",\n")}
         |    FROM `test_many_values` T1
         |    GROUP BY ID ORDER BY a_1;
         |""".stripMargin

    val table = parseQuery(sqlQuery)
    val result = executeQuery(table)

    // The result table should contain 250 columns from a_1 to a_250 and 10 rows with values from 1 to 10.
    assertThat(result.size).isEqualTo(10)
    for (rowNumber <- result.indices) {
      val row = result(rowNumber)
      assertThat(row.getArity).isEqualTo(250)
      for (j <- 1 to 250) {
        // column value starts from 1 and ends at 10.
        val expectedRowValue = rowNumber + 1
        assertThat(row.getField("a_" + j))
          .withFailMessage("Invalid value for row %d and column a_%d.".format(rowNumber, j))
          .isEqualTo(expectedRowValue)

      }
    }
  }

  @Test
  def testManyIns(): Unit = {
    val sql = new StringBuilder("SELECT a FROM SmallTable3 WHERE a IN (")
    for (i <- 1 to 10000) {
      sql.append(i)
      if (i != 10000) {
        sql.append(", ")
      }
    }
    sql.append(")")

    val result = Seq(
      Row.of(java.lang.Integer.valueOf(1)),
      Row.of(java.lang.Integer.valueOf(2)),
      Row.of(java.lang.Integer.valueOf(3)))
    runTest(sql.mkString, result)
  }

  private[flink] def runTest(sql: String, results: Seq[Row]): Unit = {
    tEnv.getConfig.set(TableConfigOptions.MAX_LENGTH_GENERATED_CODE, Int.box(4000))
    tEnv.getConfig.set(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, Int.box(10000))
    checkResult(sql.mkString, results)
  }
}
