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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{CommonTestData, StreamITCase}
import org.apache.flink.table.utils.TestFilterableTableSource
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TableSourceITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testCsvTableSource(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource
    StreamITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource("csvTable", csvTable)
    tEnv.scan("csvTable")
      .where('id > 4)
      .select('last, 'score * 2)
      .toAppendStream[Row]
      .addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "Williams,69.0",
      "Miller,13.56",
      "Smith,180.2",
      "Williams,4.68")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCsvTableSourceWithFilterable(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerTableSource(tableName, TestFilterableTableSource())
    tEnv.scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
