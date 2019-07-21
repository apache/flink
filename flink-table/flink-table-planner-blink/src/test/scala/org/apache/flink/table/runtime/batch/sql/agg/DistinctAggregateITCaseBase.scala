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

package org.apache.flink.table.runtime.batch.sql.agg

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData._
import org.apache.flink.types.Row

import org.junit.{Before, Ignore, Test}

import scala.collection.Seq

/**
  * Distinct Aggregate IT case base class.
  */
abstract class DistinctAggregateITCaseBase extends BatchTestBase {

  def prepareAggOp(): Unit

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    registerCollection("SmallTable5", smallData5, type5, "a, b, c, d, e", nullablesOfSmallData5)
    registerCollection("EmptyTable3", Seq(), type3, "a, b, c")
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("Table5", data5, type5, "a, b, c, d, e", nullablesOfData5)

    val nullData3 = data3.map { r =>
      val v2 = if (r.getField(2).toString.contains("Hello")) {
        null
      } else {
        r.getField(2)
      }
      row(r.getField(0), r.getField(1), v2)
    }

    registerCollection("NullTable3", nullData3, type3, "a, b, c", Array(false, false, true))

    prepareAggOp()
  }

  @Test
  def testSingleDistinctAgg(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a) FROM Table3",
      Seq(row(21))
    )
    checkResult(
      "SELECT COUNT(DISTINCT a) FROM EmptyTable3",
      Seq(row(0))
    )
  }

  @Test
  def testMultiDistinctAggOnSameColumn(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT b), SUM(DISTINCT b), MAX(DISTINCT b) FROM Table3",
      Seq(row(6, 21, 6))
    )
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAgg(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT c), SUM(a) FROM NullTable3",
      Seq(row(18, 231))
    )

    checkResult(
      "SELECT COUNT(DISTINCT b), COUNT(c) FROM NullTable3",
      Seq(row(6, 18))
    )
  }

  @Test
  def testMultiDistinctAggOnDifferentColumn(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM Table3",
      Seq(row(21, 21))
    )

    checkResult(
      "SELECT COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM Table3",
      Seq(row(21, 21, 21))
    )

    checkResult(
      "SELECT COUNT(a), SUM(DISTINCT b), COUNT(DISTINCT c) FROM NullTable3",
      Seq(row(21, 21, 18))
    )

    checkResult(
      "SELECT COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM EmptyTable3",
      Seq(row(0, null, 0))
    )
  }

  @Test
  def testMultiDistinctAndNonDistinctAggOnDifferentColumn(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c), count(1) FROM Table3",
      Seq(row(21, 21, 21, 21))
    )

    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c), count(1) FROM EmptyTable3",
      Seq(row(0, null, 0, 0))
    )
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    checkResult(
      "SELECT a, COUNT(a), SUM(DISTINCT b) FROM SmallTable3 GROUP BY a",
      Seq(row(1, 1, 1), row(2, 1, 2), row(3, 1, 2))
    )

    checkResult(
      "SELECT c, COUNT(b), SUM(DISTINCT a) FROM NullTable3 WHERE a < 6 GROUP BY c",
      Seq(row(null, 3, 9), row("Hi", 1, 1), row("I am fine.", 1, 5))
    )

    checkResult(
      "SELECT a, COUNT(a), SUM(DISTINCT b) FROM EmptyTable3 GROUP BY a",
      Seq()
    )
  }

  @Test
  def testSingleDistinctAggWithGroupByAndCountStar(): Unit = {
    checkResult(
      "SELECT a, COUNT(*), SUM(DISTINCT b) FROM SmallTable3 GROUP BY a",
      Seq(row(1, 1, 1), row(2, 1, 2), row(3, 1, 2))
    )

    checkResult(
      "SELECT a, COUNT(*), SUM(DISTINCT b) FROM EmptyTable3 GROUP BY a",
      Seq()
    )
  }

  @Test
  def testTwoDistinctAggWithGroupByAndCountStar(): Unit = {
    checkResult(
      "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT b) FROM SmallTable3 GROUP BY a",
      Seq(row(1, 1, 1, 1), row(2, 1, 2, 1), row(3, 1, 2, 1))
    )

    checkResult(
      "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT b) FROM EmptyTable3 GROUP BY a",
      Seq()
    )
  }

  @Test
  def testTwoDifferentDistinctAggWithGroupByAndCountStar(): Unit = {
    checkResult(
      "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM SmallTable3 GROUP BY a",
      Seq(row(1, 1, 1, 1), row(2, 1, 2, 1), row(3, 1, 2, 1))
    )

    checkResult(
      "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM EmptyTable3 GROUP BY a",
      Seq()
    )
  }

  @Test
  def testTwoDifferentDistinctAggWithColumnBothInNonDistinctAggAndGroupBy(): Unit = {
    checkResult(
      "SELECT b, COUNT(b), SUM(DISTINCT a), COUNT(DISTINCT c) FROM SmallTable3 GROUP BY b",
      Seq(row(1, 1, 1, 1), row(2, 2, 5, 2))
    )

    checkResult(
      "SELECT b, COUNT(b), SUM(DISTINCT a), COUNT(DISTINCT c) FROM NullTable3 GROUP BY b",
      Seq(row(1, 1, 1, 1), row(2, 2, 5, 0), row(3, 3, 15, 2), row(4, 4, 34, 4), row(5, 5, 65, 5),
        row(6, 6, 111, 6))
    )

    checkResult(
      "SELECT b, COUNT(b), SUM(DISTINCT a), COUNT(DISTINCT c) FROM EmptyTable3 GROUP BY b",
      Seq()
    )
  }

  @Test
  def testMultiDifferentDistinctAggWithAndNonDistinctAggOnSameColumn(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), MAX(a), MIN(a), COUNT(a) FROM SmallTable3",
      Seq(row(3, 3, 3, 1, 3))
    )

    checkResult(
      "SELECT COUNT(DISTINCT c), SUM(DISTINCT a), MAX(a), MIN(a), COUNT(a) " +
        "FROM SmallTable3 GROUP BY b",
      Seq(row(1, 1, 1, 1, 1), row(2, 5, 3, 2, 2))
    )
  }

  @Test
  def testSomeColumnsBothInDistinctAggAndGroupBy(): Unit = {
    checkResult(
      "SELECT b, COUNT(a), SUM(DISTINCT b) FROM SmallTable3 GROUP BY b",
      Seq(row(1, 1, 1), row(2, 2, 2))
    )
    checkResult(
      "SELECT b, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM SmallTable3 GROUP BY b",
      Seq(row(1, 1, 1, 1), row(2, 2, 2, 2))
    )

    checkResult(
      "SELECT b, COUNT(1), SUM(DISTINCT b), COUNT(DISTINCT b) FROM SmallTable3 GROUP BY b",
      Seq(row(1, 1, 1, 1), row(2, 2, 2, 1))
    )

    checkResult(
      "SELECT b, COUNT(1), SUM(DISTINCT b), COUNT(DISTINCT b) FROM EmptyTable3 GROUP BY b",
      Seq()
    )
  }

  @Test
  def testSingleDistinctAggOnMultiColumnsWithGroupingSets(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a) FROM SmallTable3 GROUP BY GROUPING SETS (b, c)",
      Seq(row(1), row(2), row(1), row(1), row(1))
    )
  }

  @Test
  def testMultiDistinctAggOnSameColumnWithGroupingSets(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT a), MAX(DISTINCT a) " +
        " FROM SmallTable3 GROUP BY GROUPING SETS (b, c)",
      Seq(row(1, 1, 1), row(2, 5, 3), row(1, 1, 1), row(1, 2, 2), row(1, 3, 3))
    )
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAggWithGroupingSets(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(b) FROM SmallTable5 GROUP BY GROUPING SETS (d, e)",
      Seq(row(1, 1), row(1, 2), row(1, 3), row(2, 4), row(1, 2))
    )
  }

  @Test
  def testMultiDistinctAggOnDifferentColumnWithGroupingSets(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM SmallTable5 GROUP BY GROUPING SETS (d, e)",
      Seq(row(1, 1), row(1, 2), row(1, 3), row(2, 4), row(1, 2))
    )
  }

  @Test
  def testMultiDistinctAndNonDistinctAggOnDifferentColumnWithGroupingSets(): Unit = {
    checkResult(
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c), COUNT(1) " +
        "FROM SmallTable5 GROUP BY GROUPING SETS (d, e)",
      Seq(row(1, 1, 1, 1), row(1, 2, 1, 1), row(1, 3, 1, 1), row(2, 4, 2, 2), row(1, 2, 1, 1))
    )
  }

  // TODO remove Ignore after supporting generated code cloud be splitted into
  //  small classes or methods due to code is too large
  @Ignore
  @Test
  def testMaxDistinctAggOnDifferentColumn(): Unit = {
    // the max groupCount must be less than 64.
    // so the max number of distinct aggregate on different column without group by column is 63.
    val fields = (0 until 63).map(i => s"f$i")
    val types = new RowTypeInfo(Seq.fill(fields.size)(Types.INT): _*)
    val nullablesOfData = Array.fill(fields.size)(false)
    val data = new Row(fields.length)
    fields.indices.foreach(i => data.setField(i, i))

    registerCollection("MyTable", Seq(data), types, fields.mkString(","), nullablesOfData)

    val expected = new Row(fields.length * 2)
    fields.indices.foreach(i => expected.setField(i, 1))
    fields.indices.foreach(i => expected.setField(i + fields.length, i))

    val distinctList = fields.map(f => s"COUNT(DISTINCT $f)").mkString(", ")
    val maxList = fields.map(f => s"MAX($f)").mkString(", ")
    checkResult(
      s"SELECT $distinctList, $maxList FROM MyTable",
      Seq(expected)
    )
  }

}
