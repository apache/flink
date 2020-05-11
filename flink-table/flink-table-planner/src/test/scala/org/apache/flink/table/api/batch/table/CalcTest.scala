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

package org.apache.flink.table.api.batch.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.batch.table.CalcTest.{MyHashCode, TestCaseClass, WC, giveMeCaseClass}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test

import java.sql.Timestamp
import java.time.LocalDateTime

class CalcTest extends TableTestBase {

  @Test
  def testMultipleFlatteningsTable(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    val result = table.select('a.flatten(), 'c, 'b.flatten())

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select",
        "a._1 AS a$_1",
        "a._2 AS a$_2",
        "c",
        "b._1 AS b$_1",
        "b._2 AS b$_2"
      )
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testNestedFlattening(): Unit = {
    val util = batchTestUtil()
    val table = util
      .addTable[((((String, TestCaseClass), Boolean), String), String)]("MyTable", 'a, 'b)

    val result = table.select('a.flatten(), 'b.flatten())

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select",
        "a._1 AS a$_1",
        "a._2 AS a$_2",
        "b"
      )
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testScalarFunctionAccess(): Unit = {
    val util = batchTestUtil()
    val table = util
      .addTable[(String, Int)]("MyTable", 'a, 'b)

    val result = table.select(
      giveMeCaseClass().get("my"),
      giveMeCaseClass().get("clazz"),
      giveMeCaseClass().flatten())

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select",
        "giveMeCaseClass$().my AS _c0",
        "giveMeCaseClass$().clazz AS _c1",
        "giveMeCaseClass$().my AS _c2",
        "giveMeCaseClass$().clazz AS _c3"
      )
    )

    util.verifyTable(result, expected)
  }

  // ----------------------------------------------------------------------------------------------
  // Tests for all the situations when we can do fields projection. Like selecting few fields
  // from a large field count source.
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSimpleSelect(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a, 'b)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(sourceTable),
      term("select", "a", "b")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectLiterals(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[Int]("MyTable", 'a)
    val resultTable = sourceTable
      .select("ABC", BigDecimal(1234), Timestamp.valueOf(LocalDateTime.of(1, 1, 1, 1, 1)))
      .select('*)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(sourceTable),
      term("select", "'ABC' AS _c0", "1234:DECIMAL(1073741823, 0) AS _c1",
        "0001-01-01 01:01:00:TIMESTAMP(3) AS _c2")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupByLiteral(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[Int]("MyTable", 'a)
    val resultTable = sourceTable
      .select("ABC", BigDecimal(1234), Timestamp.valueOf(LocalDateTime.of(1, 1, 1, 1, 1)))
      .groupBy('_c0)
      .select('*)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetDistinct",
        unaryNode(
          "DataSetCalc",
          batchTableNode(sourceTable),
          term("select", "'ABC' AS _c0")
        ), term("distinct", "_c0")),
      term("select", "'ABC' AS _c0"))

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectAllFields(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable1 = sourceTable.select('*)
    val resultTable2 = sourceTable.select('a, 'b, 'c, 'd)

    val expected = batchTableNode(sourceTable)

    util.verifyTable(resultTable1, expected)
    util.verifyTable(resultTable2, expected)
  }

  @Test
  def testSelectAggregation(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a.sum, 'b.max)

    val expected = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(sourceTable),
        term("select", "a", "b")
      ),
      term("select", "SUM(a) AS EXPR$0", "MAX(b) AS EXPR$1")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFunction(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)

    util.tableEnv.registerFunction("hashCode", MyHashCode)

    val resultTable = sourceTable.select(call("hashCode", $"c"), $"b")

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(sourceTable),
      term("select", "MyHashCode$(c) AS _c0", "b")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('a, 'c).select('a)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetDistinct",
        unaryNode(
          "DataSetCalc",
          batchTableNode(sourceTable),
          term("select", "a", "c")
        ),
        term("distinct", "a", "c")
      ),
      term("select", "a")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectAllFieldsFromGroupedTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('a, 'c).select('a, 'c)

    val expected = unaryNode(
      "DataSetDistinct",
      unaryNode(
        "DataSetCalc",
        batchTableNode(sourceTable),
        term("select", "a", "c")
      ),
      term("distinct", "a", "c")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectAggregationFromGroupedTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('c).select('a.sum)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(sourceTable),
            term("select", "a", "c")
          ),
          term("groupBy", "c"),
          term("select", "c", "SUM(a) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedTableWithNonTrivialKey(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('c.upperCase() as 'k).select('a.sum)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(sourceTable),
            // As stated in https://issues.apache.org/jira/browse/CALCITE-1584
            // Calcite planner doesn't promise to retain field names.
            term("select", "a", "UPPER(c) AS k")
          ),
          term("groupBy", "k"),
          term("select", "k", "SUM(a) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedTableWithFunctionKey(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy(MyHashCode('c) as 'k).select('a.sum)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(sourceTable),
            // As stated in https://issues.apache.org/jira/browse/CALCITE-1584
            // Calcite planner doesn't promise to retain field names.
            term("select", "a", "MyHashCode$(c) AS k")
          ),
          term("groupBy", "k"),
          term("select", "k", "SUM(a) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromAggregatedPojoTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[WC]("MyTable", 'word, 'frequency)
    val resultTable = sourceTable
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2)
    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          batchTableNode(sourceTable),
          term("groupBy", "word"),
          term("select", "word", "SUM(frequency) AS EXPR$0")
        ),
        term("select", "word, EXPR$0"),
        term("where", "=(EXPR$0, 2)")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testMultiFilter(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a, 'b)
      .filter('a > 0)
      .filter('b < 2)
      .filter(('a % 2) === 1)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(sourceTable),
      term("select", "a", "b"),
      term("where", "AND(AND(>(a, 0), <(b, 2)), =(MOD(a, 2), 1))")
    )

    util.verifyTable(resultTable, expected)
  }
}

object CalcTest {

  case class TestCaseClass(my: String, clazz: Int)

  object giveMeCaseClass extends ScalarFunction {
    def eval(): TestCaseClass = {
      TestCaseClass("hello", 42)
    }

    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
      createTypeInformation[TestCaseClass]
    }
  }

  object MyHashCode extends ScalarFunction {
    def eval(s: String): Int = s.hashCode()
  }

  case class WC(word: String, frequency: Long)
}
