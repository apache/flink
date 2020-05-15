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
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{CommonTestData, StreamITCase}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import _root_.scala.collection.mutable

class TableSourceITCase extends AbstractTestBase {

  @Test
  def testCsvTableSource(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    StreamITCase.testResults = mutable.MutableList()

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal("persons", csvTable)

    tEnv.sqlQuery(
      "SELECT id, `first`, `last`, score FROM persons WHERE id < 4 ")
      .toAppendStream[Row]
      .addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}
