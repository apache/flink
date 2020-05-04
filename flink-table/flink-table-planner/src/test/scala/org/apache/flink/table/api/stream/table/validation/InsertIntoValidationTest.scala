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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Types, ValidationException}
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.utils.MemoryTableSourceSinkUtil

import org.junit.Test

class InsertIntoValidationTest {

  @Test(expected = classOf[ValidationException])
  def testInconsistentLengthInsert(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "f")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    // must fail because table sink has too few fields.
    tEnv.scan("sourceTable")
      .select('a, 'b, 'c)
      .insertInto("targetTable")

    // trigger translation
    tEnv.execute("job name")
  }

  @Test(expected = classOf[ValidationException])
  def testUnmatchedTypesInsert(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", sink.configure(fieldNames, fieldTypes))

    // must fail because field types of table sink are incompatible.
    tEnv.scan("sourceTable")
      .select('a, 'b, 'c)
      .insertInto("targetTable")

    // trigger translation
    tEnv.execute("job name")
  }
}
