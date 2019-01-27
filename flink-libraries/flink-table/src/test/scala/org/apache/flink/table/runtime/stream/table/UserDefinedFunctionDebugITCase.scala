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
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.codegen.CodeGenUtils
import org.apache.flink.table.runtime.utils.TestingAppendSink
import org.apache.flink.types.Row

import org.junit.Test

class UserDefinedFunctionDebugITCase {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))

  @Test(expected = classOf[Exception])
  def testDebugEnableCompile(): Unit = {
    val tableConfig = new TableConfig
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)
    CodeGenUtils.enableCodeGenerateDebug()
    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c)
    val result = table.select('a, DebugUDF('b) as 'b, 'c)
    result.toAppendStream[Row]
      .addSink(new TestingAppendSink)
      env.execute()
  }

}

object DebugUDF extends ScalarFunction {
  def eval(x: Int): Int = if (x == 3) {
    throw new RuntimeException("TestException")
  } else {
    x * x
  }
}
