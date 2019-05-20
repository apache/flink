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

package org.apache.flink.table.api.stream.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class MatchRecognizeValidationTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c.rowtime, 'proctime.proctime)

  /** Function 'MATCH_ROWTIME()' can only be used in MATCH_RECOGNIZE **/
  @Test(expected = classOf[ValidationException])
  def testMatchRowtimeInSelect() = {
    val sql = "SELECT MATCH_ROWTIME() FROM MyTable"
    streamUtil.verifySql(sql, "n/a")
  }

  /** Function 'MATCH_PROCTIME()' can only be used in MATCH_RECOGNIZE **/
  @Test(expected = classOf[ValidationException])
  def testMatchProctimeInSelect() = {
    val sql = "SELECT MATCH_PROCTIME() FROM MyTable"
    streamUtil.verifySql(sql, "n/a")
  }
}
