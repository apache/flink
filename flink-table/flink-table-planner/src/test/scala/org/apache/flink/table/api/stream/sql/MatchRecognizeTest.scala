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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil.{term, _}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class MatchRecognizeTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  private val table = streamUtil.addTable[(Int, String, Long)](
    "MyTable",
    'a,
    'b,
    'c.rowtime,
    'proctime.proctime)

  @Test
  def testSimpleWithDefaults(): Unit = {
    val sqlQuery =
      """SELECT T.aa as ta
        |FROM MyTable
        |MATCH_RECOGNIZE (
        |  ORDER BY proctime
        |  MEASURES
        |    A.a as aa
        |  PATTERN (A B)
        |  DEFINE
        |    A AS a = 1,
        |    B AS b = 'b'
        |) AS T""".stripMargin

    val expected = unaryNode(
      "DataStreamMatch",
      streamTableNode(table),
      term("orderBy", "proctime ASC"),
      term("measures", "FINAL(A.a) AS aa"),
      term("rowsPerMatch", "ONE ROW PER MATCH"),
      term("after",
        "SKIP TO NEXT ROW"), //this is not SQL-standard compliant, SKIP PAST LAST
      // should be the default
      term("pattern", "('A', 'B')"),
      term("define", "{A==(LAST(*.$0, 0), 1)", "B==(LAST(*.$1, 0), 'b')}"))

    streamUtil.verifySql(sqlQuery, expected)
  }

}
