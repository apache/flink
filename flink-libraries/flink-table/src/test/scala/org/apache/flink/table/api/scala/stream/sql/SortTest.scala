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
package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class SortTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c,
      'proctime.proctime, 'rowtime.rowtime)
  
  @Test
  def testSortProcessingTime() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY proctime, c"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode("DataStreamSort",
          streamTableNode(0),
          term("orderBy", "proctime ASC", "c ASC")),
        term("select", "a", "TIME_MATERIALIZATION(proctime) AS proctime", "c"))

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testSortRowTime() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY rowtime, c"
      
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode("DataStreamSort",
          streamTableNode(0),
          term("orderBy", "rowtime ASC, c ASC")),
        term("select", "a", "TIME_MATERIALIZATION(rowtime) AS rowtime", "c"))
       
    streamUtil.verifySql(sqlQuery, expected)
  }

  // test should fail because time order is descending
  @Test(expected = classOf[TableException])
  def testSortProcessingTimeDesc() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY proctime DESC, c"
    streamUtil.verifySql(sqlQuery, "")
  }
  
  
  // test should fail because time is not the primary order field
  @Test(expected = classOf[TableException])
  def testSortProcessingTimeSecondaryField() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY c, proctime"
    streamUtil.verifySql(sqlQuery, "")
  }
  
}
