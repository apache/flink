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

package org.apache.flink.table.api.scala.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.utils.CommonTestData
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.catalog.TableIdentifier

class ExternalCatalogITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testSQL(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    tEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog)
    val sqlQuery = "SELECT * FROM test.db1.tb1 UNION ALL " +
        "(SELECT d, e, g FROM test.db2.tb2 WHERE d < 3)"
    tEnv.sql(sqlQuery)
        .toDataStream[Row]
        .addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world",
      "1,1,Hallo",
      "2,2,Hallo Welt",
      "2,3,Hallo Welt wie")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testTableAPI(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    tEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog)
    val table1 = tEnv.scan("test", "db1", "tb1")
    val table2 = tEnv.scan("test", "db2", "tb2")

    table2.where("d < 3")
        .select("d, e, g")
        .unionAll(table1)
        .toDataStream[Row]
        .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world",
      "1,1,Hallo",
      "2,2,Hallo Welt",
      "2,3,Hallo Welt wie"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}
