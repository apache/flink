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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class AggregateITCase extends StreamingWithStateTestBase {

  /** test cardinality count **/
  @Test
  def testCardinalityCount(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT " +
      "c, cardinality_count(0.01, a), cardinality_count(0.05, a) " +
      "FROM MyTable " +
      "GROUP BY c"

    val data = new mutable.MutableList[(Int, Long, String)]
    for (i <- 0 until 100) {
      data.+=((i, 1L, "Hi"))
      data.+=((i % 50, 2L, "Hello"))
      data.+=((i % 2, 2L, "Hello world"))
    }

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("Hello world,2,2", "Hello,49,52", "Hi,100,97")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }
}

