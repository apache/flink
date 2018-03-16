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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.runtime.stream.sql.SortITCase.StringRowSelectorSink
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class SortITCase extends StreamingWithStateTestBase {

  @Test
  def testEventTimeOrderBy(): Unit = {
    val data = Seq(
      Left((1500L, (1L, 15, "Hello"))),
      Left((1600L, (1L, 16, "Hello"))),
      Left((1000L, (1L, 1, "Hello"))),
      Left((2000L, (2L, 2, "Hello"))),
      Right(1000L),
      Left((2000L, (2L, 2, "Hello"))),
      Left((2000L, (2L, 3, "Hello"))),
      Left((3000L, (3L, 3, "Hello"))),
      Left((2000L, (3L, 1, "Hello"))),
      Right(2000L),
      Left((4000L, (4L, 4, "Hello"))),
      Right(3000L),
      Left((5000L, (5L, 5, "Hello"))),
      Right(5000L),
      Left((6000L, (6L, 65, "Hello"))),
      Left((6000L, (6L, 6, "Hello"))),
      Left((6000L, (6L, 67, "Hello"))),
      Left((6000L, (6L, -1, "Hello"))),
      Left((6000L, (6L, 6, "Hello"))),
      Right(7000L),
      Left((9000L, (6L, 9, "Hello"))),
      Left((8500L, (6L, 18, "Hello"))),
      Left((9000L, (6L, 7, "Hello"))),
      Right(10000L),
      Left((10000L, (7L, 7, "Hello World"))),
      Left((11000L, (7L, 77, "Hello World"))),
      Left((11000L, (7L, 17, "Hello World"))),
      Right(12000L),
      Left((14000L, (7L, 18, "Hello World"))),
      Right(14000L),
      Left((15000L, (8L, 8, "Hello World"))),
      Right(17000L),
      Left((20000L, (20L, 20, "Hello World"))), 
      Right(19000L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t1 = env.addSource(new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
      
    tEnv.registerTable("T1", t1)

    val  sqlQuery = "SELECT b FROM T1 ORDER BY rowtime, b ASC "
      
      
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StringRowSelectorSink(0)).setParallelism(1)
    env.execute()
    
    val expected = mutable.MutableList(
      "1", "15", "16",
      "1", "2", "2", "3",
      "3",
      "4",
      "5",
      "-1", "6", "6", "65", "67",
      "18", "7", "9",
      "7", "17", "77", 
      "18",
      "8",
      "20")
    assertEquals(expected, SortITCase.testResults)
  }
}

object SortITCase {

  final class StringRowSelectorSink(private val field:Int) extends RichSinkFunction[Row]() {
    def invoke(value: Row) {
      testResults.synchronized {
        testResults += value.getField(field).toString
      }
    }
  }
  
  var testResults: mutable.MutableList[String] = mutable.MutableList.empty[String]
}
