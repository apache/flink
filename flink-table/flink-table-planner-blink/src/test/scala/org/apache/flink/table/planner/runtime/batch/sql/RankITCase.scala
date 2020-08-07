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

import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._

import org.junit._

import scala.collection.Seq

class RankITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("Table5", data5, type5, "a, b, c, d, e", nullablesOfData5)
    registerCollection("Table2", data2_1, INT_DOUBLE, "a, b")
  }

  @Test
  def testRankValueFilterWithUpperValue(): Unit = {
    checkResult(
      "SELECT * FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM Table3) t " +
        "WHERE rk <= 2",
      Seq(row(1, 1, 1), row(2, 2, 1), row(3, 2, 2), row(4, 3, 1), row(5, 3, 2), row(7, 4, 1),
        row(8, 4, 2), row(11, 5, 1), row(12, 5, 2), row(16, 6, 1), row(17, 6, 2)))

    checkResult(
      "SELECT * FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY e) rk FROM Table5) t " +
        "WHERE rk <= 3",
      Seq(row(1, 1, 1), row(2, 3, 1), row(2, 2, 2), row(3, 4, 1), row(3, 5, 1), row(3, 6, 3),
        row(4, 8, 1), row(4, 9, 1), row(4, 7, 3), row(4, 10, 3),
        row(5, 11, 1), row(5, 14, 2), row(5, 15, 2)))

    checkResult(
      "SELECT * FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b DESC) rk FROM Table2) t " +
        "WHERE rk <= 2",
      Seq(row(1, 2.0, 1), row(1, 2.0, 1), row(2, 1.0, 1), row(2, 1.0, 1), row(3, 3.0, 1),
        row(6, null, 1), row(null, 5.0, 1), row(null, null, 2)))
  }

  @Test
  def testRankValueFilterWithRange(): Unit = {
    checkResult(
      "SELECT * FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM Table3) t " +
        "WHERE rk <= 4 and rk > 1",
      Seq(row(3, 2, 2), row(5, 3, 2), row(6, 3, 3), row(8, 4, 2), row(9, 4, 3), row(10, 4, 4),
        row(12, 5, 2), row(13, 5, 3), row(14, 5, 4), row(17, 6, 2), row(18, 6, 3), row(19, 6, 4)))

    checkResult(
      "SELECT a, b FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM Table3) t " +
        "WHERE rk <= 3 and rk > -2",
      Seq(row(1, 1), row(2, 2), row(3, 2), row(4, 3), row(5, 3), row(6, 3), row(7, 4), row(8, 4),
        row(9, 4), row(11, 5), row(12, 5), row(13, 5), row(16, 6), row(17, 6), row(18, 6)))

    checkResult(
      "SELECT * FROM (" +
        "SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY e) rk FROM Table5) t " +
        "WHERE rk <= 3 and rk >= 2",
      Seq(row(2, 2, 2), row(3, 6, 3), row(4, 7, 3), row(4, 10, 3), row(5, 14, 2), row(5, 15, 2)))
  }

  @Test
  def testRankValueFilterWithLowerValue(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM Table3) t " +
      "WHERE rk > 2",
      Seq(row(6, 3, 3), row(9, 4, 3), row(10, 4, 4), row(13, 5, 3), row(14, 5, 4), row(15, 5, 5),
        row(18, 6, 3), row(19, 6, 4), row(20, 6, 5), row(21, 6, 6)))

    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY e) rk FROM Table5) t " +
      "WHERE rk > 2",
      Seq(row(3, 6, 3), row(4, 10, 3), row(4, 7, 3), row(5, 12, 4), row(5, 13, 4)))
  }

  @Test
  def testRankValueFilterWithEquals(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM Table3) t " +
      "WHERE rk = 2",
      Seq(row(3, 2, 2), row(5, 3, 2), row(8, 4, 2), row(12, 5, 2), row(17, 6, 2)))
  }

  @Test
  def testWithoutPartitionBy(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (ORDER BY a) rk FROM Table3) t " +
      "WHERE rk < 5",
      Seq(row(1, 1, 1), row(2, 2, 2), row(3, 2, 3), row(4, 3, 4)))

    checkResult("SELECT a, b FROM (" +
      "SELECT a, b, RANK() OVER (ORDER BY e DESC) rk FROM Table5) t " +
      "WHERE rk < 5 and a > 2",
      Seq(row(3, 6), row(5, 12), row(5, 13), row(3, 4), row(3, 5), row(4, 7),
        row(4, 10), row(5, 14), row(5, 15)))

    checkResult("SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (ORDER BY a DESC) rk FROM Table2) t " +
      "WHERE rk < 5",
      Seq(row(6, null, 1), row(3, 3.0, 2), row(2, 1.0, 3), row(2, 1.0, 3)))
  }

  @Test
  def testMultiSameRankFunctionsWithSameGroup(): Unit = {
    checkResult("SELECT * FROM (" +
      "SELECT a, b, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk1, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk2 FROM Table3) t " +
      "WHERE rk1 < 3",
      Seq(row(1, 1, 1, 1), row(2, 2, 1, 1), row(3, 2, 2, 2), row(4, 3, 1, 1),
        row(5, 3, 2, 2), row(7, 4, 1, 1), row(8, 4, 2, 2), row(11, 5, 1, 1),
        row(12, 5, 2, 2), row(16, 6, 1, 1), row(17, 6, 2, 2)))
  }

}
