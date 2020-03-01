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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.dataformat.BinaryString.fromString
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.{binaryRow, row}
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import org.junit._

import scala.collection.Seq

class UnionITCase extends BatchTestBase {

  val type6 = new BaseRowTypeInfo(
    new IntType(), new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH))

  val data6 = Seq(
    binaryRow(type6.getLogicalTypes, 1, 1L, fromString("Hi")),
    binaryRow(type6.getLogicalTypes, 2, 2L, fromString("Hello")),
    binaryRow(type6.getLogicalTypes, 3, 2L, fromString("Hello world")),
    binaryRow(type6.getLogicalTypes, 4, 3L, fromString("Hello world, how are you?"))
  )

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("Table3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    registerCollection("Table5", data5, type5, "d, e, f, g, h", nullablesOfData5)
    registerCollection("Table6", data6, type6, "a, b, c", Array(false, false, false))
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
  }

  @Test
  def testUnionWithDifferentRowtype(): Unit = {
    checkResult(
      "SELECT a FROM (SELECT * FROM Table3 t1 UNION ALL (SELECT * FROM Table6 t2))",
      Seq(
        row("1"),
        row("1"),
        row("2"),
        row("2"),
        row("3"),
        row("3"),
        row("4")))
  }

  @Test
  def testUnionAll(): Unit = {
    checkResult(
      "SELECT t1.c FROM Table3 t1 UNION ALL (SELECT t2.c FROM Table3 t2)",
      Seq(
        row("Hi"),
        row("Hi"),
        row("Hello"),
        row("Hello"),
        row("Hello world"),
        row("Hello world")))
  }

  @Test
  def testUnion(): Unit = {
    checkResult(
      "SELECT t1.c FROM Table3 t1 UNION (SELECT t2.c FROM Table3 t2)",
      Seq(
        row("Hi"),
        row("Hello"),
        row("Hello world")))
  }

  @Test
  def testUnionWithFilter(): Unit = {
    checkResult(
      "SELECT c FROM (" +
          "SELECT * FROM Table3 UNION ALL (SELECT d, e, g FROM Table5))" +
          "WHERE b < 2",
      Seq(
        row("Hi"),
        row("Hallo")))
  }

  @Test
  def testUnionWithAggregation(): Unit = {
    checkResult(
      "SELECT count(c) FROM (" +
          "SELECT * FROM Table3 UNION ALL (SELECT d, e, g FROM Table5))",
      Seq(row(18)))
  }

  /**
    * Test different types of two union inputs(One is GenericRow, the other is BinaryRow).
    */
  @Test
  def testJoinAfterDifferentTypeUnionAll(): Unit = {
    tEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    checkResult(
      "SELECT a, c, g FROM (SELECT t1.a, t1.b, t1.c FROM Table3 t1 UNION ALL" +
          "(SELECT a, b, c FROM Table3 ORDER BY a, b, c)), Table5 WHERE b = e",
      Seq(
        row(1, "Hi", "Hallo"),
        row(1, "Hi", "Hallo"),
        row(2, "Hello", "Hallo Welt"),
        row(2, "Hello", "Hallo Welt"),
        row(3, "Hello world", "Hallo Welt"),
        row(3, "Hello world", "Hallo Welt")
      ))
  }

  @Test
  def testUnionOfMultiInputs(): Unit = {
    checkResult(
      "select max(v) as x, min(v) as n from \n" +
        "(values cast(-86.4 as double), cast(-100 as double), cast(2 as double)) as t(v)",
      Seq(row(2.0, -100.0)))
  }
}
