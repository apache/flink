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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.BatchTableEnvironmentImpl
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.utils.MemoryTableSourceSinkUtil
import org.apache.flink.table.utils.TableTestUtil.{batchTableNode, readFromResource, replaceStageId}
import org.apache.flink.test.util.MultipleProgramsTestBase
import org.junit.Assert.assertEquals
import org.junit._

class ExplainTest
  extends MultipleProgramsTestBase(MultipleProgramsTestBase.TestExecutionMode.CLUSTER) {

  @Test
  def testFilterWithoutExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val scan = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table = scan.filter($"a" % 2 === 0)

    val result = tEnv.explain(table).replaceAll("\\r\\n", "\n")
    val source = readFromResource("testFilter0.out")

    val expected = replaceString(source, scan)
    assertEquals(expected, result)
  }

  @Test
  def testFilterWithExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val scan = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table = scan.filter($"a" % 2 === 0)

    val result = tEnv.asInstanceOf[BatchTableEnvironmentImpl]
      .explain(table, extended = true).replaceAll("\\r\\n", "\n")
    val source = readFromResource("testFilter1.out")

    val expected = replaceString(source, scan)
    assertEquals(expected, result)
  }

  @Test
  def testJoinWithoutExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'c, 'd)
    val table = table1.join(table2).where($"b" === $"d").select($"a", $"c")

    val result = tEnv.explain(table).replaceAll("\\r\\n", "\n")
    val source = readFromResource("testJoin0.out")

    val expected = replaceString(source, table1, table2)
    assertEquals(expected, result)
  }

  @Test
  def testJoinWithExtended(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'c, 'd)
    val table = table1.join(table2).where($"b" === $"d").select($"a", $"c")

    val result = tEnv.asInstanceOf[BatchTableEnvironmentImpl]
      .explain(table, extended = true).replaceAll("\\r\\n", "\n")
    val source = readFromResource("testJoin1.out")

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
    val source = readFromResource("testUnion0.out")

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
    val source = readFromResource("testUnion1.out")

    val expected = replaceString(source, table1, table2)
    assertEquals(expected, result)
  }

  @Test
  def testInsert(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "sourceTable", CommonTestData.getCsvTableSource)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING(), Types.INT())
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    tEnv.sqlUpdate("insert into targetTable select first, id from sourceTable")

    val result = tEnv.explain(false)
    val expected = readFromResource("testInsert1.out")
    assertEquals(replaceStageId(expected), replaceStageId(result))
  }

  @Test
  def testMultipleInserts(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "sourceTable", CommonTestData.getCsvTableSource)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING(), Types.INT())
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable1", sink.configure(fieldNames, fieldTypes))
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable2", sink.configure(fieldNames, fieldTypes))

    tEnv.sqlUpdate("insert into targetTable1 select first, id from sourceTable")
    tEnv.sqlUpdate("insert into targetTable2 select last, id from sourceTable")

    val result = tEnv.explain(false)
    val expected = readFromResource("testMultipleInserts1.out")
    assertEquals(replaceStageId(expected), replaceStageId(result))
  }

  def replaceString(s: String, t1: Table, t2: Table): String = {
    replaceSourceNode(replaceSourceNode(replaceString(s), t1, 0), t2, 1)
  }

  def replaceString(s: String, t: Table): String = {
    replaceSourceNode(replaceString(s), t, 0)
  }

  private def replaceSourceNode(s: String, t: Table, idx: Int): String = {
    s.replace(
      s"%logicalSourceNode$idx%", batchTableNode(t)
        .replace("DataSetScan", "FlinkLogicalDataSetScan"))
      .replace(s"%sourceNode$idx%", batchTableNode(t))
  }

  def replaceString(s: String): String = {
    s.replaceAll("\\r\\n", "\n")
  }
}
