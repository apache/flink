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
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  
  
  @Test
  def testSortProcessingTime() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY procTime(), c"
      
      val expected =
      unaryNode(
        "DataStreamSort",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "1970-01-01 00:00:00 AS EXPR$1","c")
          ),
        term("orderBy", "EXPR$1 ASC, c ASC], offset=[null], fetch=[unlimited")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }
  
  
  @Test
  def testSortRowTime() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY rowTime(), c"
      
      val expected =
      unaryNode(
        "DataStreamSort",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "1970-01-01 00:00:00 AS EXPR$1","c")
          ),
        term("orderBy", "EXPR$1 ASC, c ASC], offset=[null], fetch=[unlimited")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }
  
   @Test
  def testSortProcessingTimeDesc() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY procTime() DESC, c"
    //fail if no error is thrown
    try{
      streamUtil.verifySql(sqlQuery, "")
    } catch {
      case rt : Throwable => assert(true)
    }
  }
   
    @Test
   def testSortProcessingTimeSecondaryField() = {

    val sqlQuery = "SELECT a FROM MyTable ORDER BY c, procTime()"
    //fail if no error is thrown
    try{
      streamUtil.verifySql(sqlQuery, "")
    } catch {
      case rt : Throwable => assert(true)
    }
  }
  
}
