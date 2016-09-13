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

package org.apache.flink.api.scala.stream

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert.assertEquals
import org.junit._

class ExplainStreamTest
  extends StreamingMultipleProgramsTestBase {

  val testFilePath = ExplainStreamTest.this.getClass.getResource("/").getFile

  @Test
  def testFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val table = env.fromElements((1, "hello"))
      .toTable(tEnv, 'a, 'b)
      .filter("a % 2 = 0")

    val result = tEnv.explain(table).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testFilterStream0.out").mkString.replaceAll("\\r\\n", "\n")
    assertEquals(result, source)
  }

  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table = table1.unionAll(table2)

    val result = tEnv.explain(table).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testUnionStream0.out").mkString.replaceAll("\\r\\n", "\n")
    assertEquals(result, source)
  }

}
