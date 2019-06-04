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

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingAppendBaseRowSink}
import org.apache.flink.table.types.logical.IntType
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.junit.Assert._
import org.junit.Test

class ValuesITCase extends StreamingTestBase {

  @Test
  def testValues(): Unit = {
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)

    val sqlQuery = "SELECT * FROM (VALUES (1, 2, 3)) T(a, b, c)"

    val outputType = new BaseRowTypeInfo(new IntType(), new IntType(), new IntType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[BaseRow]
    val sink = new TestingAppendBaseRowSink(outputType)
    result.addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("0|1,2,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
