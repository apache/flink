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

package org.apache.flink.table.api.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test

class CalcTest extends TableTestBase {

  @Test
  def testMultipleFlattening(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select",
        "a._1 AS _1",
        "a._2 AS _2",
        "c",
        "b._1 AS _10",
        "b._2 AS _20"
      )
    )

    util.verifySql(
      "SELECT MyTable.a.*, c, MyTable.b.* FROM MyTable",
      expected)
  }

  @Test
  def testIn(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val resultStr = "Sarg[1L:BIGINT, 2L:BIGINT, 3L:BIGINT, 4L:BIGINT, 5L:BIGINT, " +
        "6L:BIGINT, 7L:BIGINT, 8L:BIGINT, 9L:BIGINT, 10L:BIGINT, 11L:BIGINT, " +
        "12L:BIGINT, 13L:BIGINT, 14L:BIGINT, 15L:BIGINT, 16L:BIGINT, " +
        "17L:BIGINT, 18L:BIGINT, 19L:BIGINT, 20L:BIGINT, 21L:BIGINT, " +
        "22L:BIGINT, 23L:BIGINT, 24L:BIGINT, 25L:BIGINT, 26L:BIGINT, " +
        "27L:BIGINT, 28L:BIGINT, 29L:BIGINT, 30L:BIGINT]:BIGINT"
    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select", "a", "b", "c"),
      term("where", s"SEARCH(b, $resultStr)")
    )

    val inStr = (1 to 30).mkString(", ")
    util.verifySql(
      s"SELECT * FROM MyTable WHERE b in ($inStr)",
      expected)
  }

  @Test
  def testNotIn(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val resultStr = "Sarg[(-∞..1L:BIGINT), (1L:BIGINT..2L:BIGINT), " +
        "(2L:BIGINT..3L:BIGINT), (3L:BIGINT..4L:BIGINT), " +
        "(4L:BIGINT..5L:BIGINT), (5L:BIGINT..6L:BIGINT), " +
        "(6L:BIGINT..7L:BIGINT), (7L:BIGINT..8L:BIGINT), " +
        "(8L:BIGINT..9L:BIGINT), (9L:BIGINT..10L:BIGINT), " +
        "(10L:BIGINT..11L:BIGINT), (11L:BIGINT..12L:BIGINT), (12L:BIGINT..13L:BIGINT), " +
        "(13L:BIGINT..14L:BIGINT), (14L:BIGINT..15L:BIGINT), (15L:BIGINT..16L:BIGINT), " +
        "(16L:BIGINT..17L:BIGINT), (17L:BIGINT..18L:BIGINT), (18L:BIGINT..19L:BIGINT), " +
        "(19L:BIGINT..20L:BIGINT), (20L:BIGINT..21L:BIGINT), (21L:BIGINT..22L:BIGINT), " +
        "(22L:BIGINT..23L:BIGINT), (23L:BIGINT..24L:BIGINT), (24L:BIGINT..25L:BIGINT), " +
        "(25L:BIGINT..26L:BIGINT), (26L:BIGINT..27L:BIGINT), (27L:BIGINT..28L:BIGINT), " +
        "(28L:BIGINT..29L:BIGINT), (29L:BIGINT..30L:BIGINT), (30L:BIGINT..+∞)]:BIGINT"
    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select", "a", "b", "c"),
      term("where", s"SEARCH(b, $resultStr)")
    )

    val notInStr = (1 to 30).mkString(", ")
    util.verifySql(
      s"SELECT * FROM MyTable WHERE b NOT IN ($notInStr)",
      expected)
  }
}
