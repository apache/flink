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
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class SortValidationTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c,
      'proctime.proctime, 'rowtime.rowtime)

  // test should fail because time order is descending
  @Test(expected = classOf[TableException])
  def testSortProcessingTimeDesc(): Unit = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY proctime DESC, c"
    streamUtil.verifySql(sqlQuery, "")
  }

  // test should fail because time is not the primary order field
  @Test(expected = classOf[TableException])
  def testSortProcessingTimeSecondaryField(): Unit = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY c, proctime"
    streamUtil.verifySql(sqlQuery, "")
  }

  // test should fail because LIMIT is not supported without sorting
  @Test(expected = classOf[TableException])
  def testLimitWithoutSorting(): Unit = {

    val sqlQuery = "SELECT a FROM MyTable LIMIT 3"
    streamUtil.verifySql(sqlQuery, "")
  }
}
