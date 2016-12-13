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
package org.apache.flink.api.table.plan

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.ValidationException
import org.apache.flink.api.table.expressions.{RowtimeAttribute, Upper, WindowReference}
import org.apache.flink.api.table.functions.ScalarFunction
import org.apache.flink.api.table.plan.FieldProjectionTest._
import org.apache.flink.api.table.plan.logical.EventTimeTumblingGroupWindow
import org.apache.flink.api.table.utils.TableTestBase
import org.apache.flink.api.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Tests for all the situations when we can do fields projection. Like selecting few fields
  * from a large field count source.
  */
class FieldProjectionTest extends TableTestBase {

  val util = batchTestUtil()

  val streamUtil = streamTestUtil()

  @Test
  def testSimpleSelect(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a, 'b)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a", "b")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectAllFields(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable1 = sourceTable.select('*)
    val resultTable2 = sourceTable.select('a, 'b, 'c, 'd)

    val expected = batchTableNode(0)

    util.verifyTable(resultTable1, expected)
    util.verifyTable(resultTable2, expected)
  }

  @Test
  def testSelectAggregation(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a.sum, 'b.max)

    val expected = unaryNode(
      "DataSetAggregate",
      binaryNode(
        "DataSetUnion",
        values(
          "DataSetValues",
          tuples(List(null, null)),
          term("values", "a", "b")
        ),
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        term("union", "a", "b")
      ),
      term("select", "SUM(a) AS TMP_0", "MAX(b) AS TMP_1")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFunction(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)

    util.tEnv.registerFunction("hashCode", MyHashCode)

    val resultTable = sourceTable.select("hashCode(c), b")

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", s"${MyHashCode.getClass.getCanonicalName}(c) AS _c0", "b")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedTable(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('a, 'c).select('a)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "c")
        ),
        term("groupBy", "a", "c"),
        term("select", "a", "c")
      ),
      term("select", "a")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectAllFieldsFromGroupedTable(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('a, 'c).select('a, 'c)

    val expected = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "a", "c")
      ),
      term("groupBy", "a", "c"),
      term("select", "a", "c")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectAggregationFromGroupedTable(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('c).select('a.sum)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "a", "c")
          ),
          term("groupBy", "c"),
          term("select", "c", "SUM(a) AS TMP_0")
        ),
        term("select", "TMP_0 AS TMP_1")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedTableWithNonTrivialKey(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy(Upper('c) as 'k).select('a.sum)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "a", "c", "UPPER(c) AS k")
          ),
          term("groupBy", "k"),
          term("select", "k", "SUM(a) AS TMP_0")
        ),
        term("select", "TMP_0 AS TMP_1")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedTableWithFunctionKey(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy(MyHashCode('c) as 'k).select('a.sum)

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "a", "c", s"${MyHashCode.getClass.getCanonicalName}(c) AS k")
          ),
          term("groupBy", "k"),
          term("select", "k", "SUM(a) AS TMP_0")
        ),
        term("select", "TMP_0 AS TMP_1")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromStreamingWindow(): Unit = {
    val sourceTable = streamUtil.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .select(Upper('c).count, 'a.sum)

    val expected =
      unaryNode(
        "DataStreamAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "c", "a", "UPPER(c) AS $f2")
        ),
        term("window",
          EventTimeTumblingGroupWindow(
            Some(WindowReference("w")),
            RowtimeAttribute(),
            5.millis)),
        term("select", "COUNT($f2) AS TMP_0", "SUM(a) AS TMP_1")
      )

    streamUtil.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromStreamingGroupedWindow(): Unit = {
    val sourceTable = streamUtil.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable
        .groupBy('b)
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .select(Upper('c).count, 'a.sum, 'b)

    val expected = unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "c", "a", "b", "UPPER(c) AS $f3")
          ),
          term("groupBy", "b"),
          term("window",
            EventTimeTumblingGroupWindow(
              Some(WindowReference("w")),
              RowtimeAttribute(),
              5.millis)),
          term("select", "b", "COUNT($f3) AS TMP_0", "SUM(a) AS TMP_1")
        ),
        term("select", "TMP_0 AS TMP_2", "TMP_1 AS TMP_3", "b")
    )

    streamUtil.verifyTable(resultTable, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testSelectFromBatchWindow1(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)

    // time field is selected
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'a as 'w)
        .select('a.sum, 'c.count)

    val expected = "TODO"

    util.verifyTable(resultTable, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testSelectFromBatchWindow2(): Unit = {
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)

    // time field is not selected
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'a as 'w)
        .select('c.count)

    val expected = "TODO"

    util.verifyTable(resultTable, expected)
  }
}

object FieldProjectionTest {

  object MyHashCode extends ScalarFunction {
    def eval(s: String): Int = s.hashCode()
  }

}
