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

package org.apache.flink.table.api.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.BatchTableEnvironmentImpl
import org.apache.flink.table.utils.TableTestUtil.batchTableNode
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.junit.Assert.assertEquals
import org.junit._

class ExplainTest
  extends MultipleProgramsTestBase(MultipleProgramsTestBase.TestExecutionMode.CLUSTER) {

  private val testFilePath = ExplainTest.this.getClass.getResource("/").getFile

  @Test
  def testFilterWithoutExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val scan = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table = scan.filter("a % 2 = 0")

    val result = tEnv.explain(table).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testFilter0.out").mkString

    val expected = replaceString(source, scan)
    assertEquals(expected, result)
  }

  @Test
  def testFilterWithExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val scan = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table = scan.filter("a % 2 = 0")

    val result = tEnv.asInstanceOf[BatchTableEnvironmentImpl]
      .explain(table, extended = true).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testFilter1.out").mkString

    val expected = replaceString(source, scan)
    assertEquals(expected, result)
  }

  @Test
  def testJoinWithoutExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'c, 'd)
    val table = table1.join(table2).where("b = d").select("a, c")

    val result = tEnv.explain(table).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testJoin0.out").mkString

    val expected = replaceString(source, table1, table2)
    assertEquals(expected, result)
  }

  @Test
  def testJoinWithExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'c, 'd)
    val table = table1.join(table2).where("b = d").select("a, c")

    val result = tEnv.asInstanceOf[BatchTableEnvironmentImpl]
      .explain(table, extended = true).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testJoin1.out").mkString

    val expected = replaceString(source, table1, table2)
    assertEquals(expected, result)
  }

  @Test
  def testUnionWithoutExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table = table1.unionAll(table2)

    val result = tEnv.explain(table).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testUnion0.out").mkString

    val expected = replaceString(source, table1, table2)
    assertEquals(expected, result)
  }

  @Test
  def testUnionWithExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table = table1.unionAll(table2)

    val result = tEnv.asInstanceOf[BatchTableEnvironmentImpl]
      .explain(table, extended = true).replaceAll("\\r\\n", "\n")
    val source = scala.io.Source.fromFile(testFilePath +
      "../../src/test/scala/resources/testUnion1.out").mkString

    val expected = replaceString(source, table1, table2)
    assertEquals(expected, result)
  }


  def replaceString(s: String, t1: Table, t2: Table): String = {
    replaceSourceNode(replaceSourceNode(replaceString(s), t1, 0), t2, 1)
  }

  def replaceString(s: String, t: Table): String = {
    replaceSourceNode(replaceString(s), t, 0)
  }

  private def replaceSourceNode(s: String, t: Table, idx: Int) = {
    s.replace(
      s"%logicalSourceNode$idx%", batchTableNode(t)
        .replace("DataSetScan", "FlinkLogicalDataSetScan"))
      .replace(s"%sourceNode$idx%", batchTableNode(t))
  }

  def replaceString(s: String) = {
    s.replaceAll("\\r\\n", "\n")
  }
}
