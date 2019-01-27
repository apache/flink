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
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class MatchRecognizeTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'rowtime.rowtime, 'proctime.proctime)

  @Test
  def testSimpleWithDefaults(): Unit = {
    val sqlQuery =
      s"""
         |SELECT T.aa as ta
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

    streamUtil.verifyPlan(sqlQuery)
  }

}
