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

package org.apache.flink.table.planner.runtime

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.FileSystemITCaseBase._
import org.apache.flink.table.planner.runtime.utils.BatchTableEnvUtil
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil.execInsertSqlAndWaitResult
import org.apache.flink.types.Row

import org.junit.Assert.assertTrue
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}

import java.io.File

import scala.collection.{JavaConverters, Seq}

/**
  * Test File system table factory.
  */
trait FileSystemITCaseBase {

  val fileTmpFolder = new TemporaryFolder
  protected var resultPath: String = _

  @Rule
  def fileTempFolder: TemporaryFolder = fileTmpFolder

  def formatProperties(): Array[String] = Array()

  def tableEnv: TableEnvironment

  def check(sqlQuery: String, expectedResult: Seq[Row]): Unit

  def check(sqlQuery: String, expectedResult: java.util.List[Row]): Unit = {
    check(sqlQuery,
      JavaConverters.asScalaIteratorConverter(expectedResult.iterator()).asScala.toSeq)
  }

  def open(): Unit = {
    resultPath = fileTmpFolder.newFolder().toURI.toString
    BatchTableEnvUtil.registerCollection(
      tableEnv,
      "originalT",
      data_with_partitions,
      dataType,
      "x, y, a, b")
    tableEnv.executeSql(
      s"""
         |create table partitionedTable (
         |  x string,
         |  y int,
         |  a int,
         |  b bigint
         |) partitioned by (a, b) with (
         |  'connector' = 'filesystem',
         |  'path' = '$resultPath',
         |  ${formatProperties().mkString(",\n")}
         |)
       """.stripMargin
    )
    tableEnv.executeSql(
      s"""
         |create table nonPartitionedTable (
         |  x string,
         |  y int,
         |  a int,
         |  b bigint
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$resultPath',
         |  ${formatProperties().mkString(",\n")}
         |)
       """.stripMargin
    )
  }

  @Test
  def testAllStaticPartitions1(): Unit = {
    execInsertSqlAndWaitResult(tableEnv, "insert into partitionedTable " +
        "partition(a='1', b='1') select x, y from originalT where a=1 and b=1")

    check(
      "select x, y from partitionedTable where a=1 and b=1",
      data_partition_1_1
    )

    check(
      "select x, y from partitionedTable",
      data_partition_1_1
    )
  }

  @Test
  def testAllStaticPartitions2(): Unit = {
    execInsertSqlAndWaitResult(tableEnv, "insert into partitionedTable " +
        "partition(a='2', b='1') select x, y from originalT where a=2 and b=1")

    check(
      "select x, y from partitionedTable where a=2 and b=1",
      data_partition_2_1
    )

    check(
      "select x, y from partitionedTable",
      data_partition_2_1
    )
  }

  @Test
  def testPartialDynamicPartition(): Unit = {
    execInsertSqlAndWaitResult(tableEnv, "insert into partitionedTable " +
        "partition(a=3) select x, y, b from originalT where a=3")

    check(
      "select x, y from partitionedTable where a=2 and b=1",
      Seq()
    )

    check(
      "select x, y from partitionedTable where a=3 and b=1",
      Seq(
        row("x17", 17)
      )
    )

    check(
      "select x, y from partitionedTable where a=3 and b=2",
      Seq(
        row("x18", 18)
      )
    )

    check(
      "select x, y from partitionedTable where a=3 and b=3",
      Seq(
        row("x19", 19)
      )
    )

    check(
      "select x, y from partitionedTable where a=3",
      Seq(
        row("x17", 17),
        row("x18", 18),
        row("x19", 19)
      )
    )
  }

  @Test
  def testDynamicPartition(): Unit = {
    execInsertSqlAndWaitResult(tableEnv, "insert into partitionedTable " +
        "select x, y, a, b from originalT")

    check(
      "select x, y from partitionedTable where a=1 and b=1",
      data_partition_1_1
    )

    check(
      "select x, y from partitionedTable where a=2 and b=1",
      data_partition_2_1
    )

    check(
      "select x, y from partitionedTable",
      data
    )
  }

  @Test
  def testPartitionWithHiddenFile(): Unit = {
    execInsertSqlAndWaitResult(tableEnv, "insert into partitionedTable " +
      "partition(a='1', b='1') select x, y from originalT where a=1 and b=1")

    // create hidden partition dir
    assertTrue(new File(new Path(resultPath + "/a=1/.b=2").toUri).mkdir())

    check(
      "select x, y from partitionedTable",
      data_partition_1_1
    )
  }

  @Test
  def testNonPartition(): Unit = {
    execInsertSqlAndWaitResult(tableEnv, "insert into nonPartitionedTable " +
        "select x, y, a, b from originalT where a=1 and b=1")

    check(
      "select x, y from nonPartitionedTable where a=1 and b=1",
      data_partition_1_1
    )
  }

  @Test
  def testLimitPushDown(): Unit = {
    tableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
    execInsertSqlAndWaitResult(
      tableEnv, "insert into nonPartitionedTable select x, y, a, b from originalT")

    check(
      "select x, y from nonPartitionedTable limit 3",
      Seq(
        row("x1", 1),
        row("x2", 2),
        row("x3", 3)))
  }

  @Test
  def testFilterPushDown(): Unit = {
    execInsertSqlAndWaitResult(
      tableEnv, "insert into nonPartitionedTable select x, y, a, b from originalT")

    check(
      "select x, y from nonPartitionedTable where a=10086",
      Seq())
  }

  @Test
  def testProjectPushDown(): Unit = {
    tableEnv.sqlUpdate("insert into partitionedTable select x, y, a, b from originalT")
    tableEnv.execute("test")

    check(
      "select y, b, x from partitionedTable where a=3",
      Seq(
        row(17, 1, "x17"),
        row(18, 2, "x18"),
        row(19, 3, "x19")
      ))
  }

  @Test
  def testInsertAppend(): Unit = {
    tableEnv.sqlUpdate("insert into partitionedTable select x, y, a, b from originalT")
    tableEnv.execute("test1")

    tableEnv.sqlUpdate("insert into partitionedTable select x, y, a, b from originalT")
    tableEnv.execute("test2")

    check(
      "select y, b, x from partitionedTable where a=3",
      Seq(
        row(17, 1, "x17"),
        row(18, 2, "x18"),
        row(19, 3, "x19"),
        row(17, 1, "x17"),
        row(18, 2, "x18"),
        row(19, 3, "x19")
      ))
  }

  @Test
  def testInsertOverwrite(): Unit = {
    tableEnv.sqlUpdate("insert overwrite partitionedTable select x, y, a, b from originalT")
    tableEnv.execute("test1")

    tableEnv.sqlUpdate("insert overwrite partitionedTable select x, y, a, b from originalT")
    tableEnv.execute("test2")

    check(
      "select y, b, x from partitionedTable where a=3",
      Seq(
        row(17, 1, "x17"),
        row(18, 2, "x18"),
        row(19, 3, "x19")
      ))
  }
}

object FileSystemITCaseBase {

  val fieldNames = Array("x", "y", "a", "b")

  val fieldTypes: Array[TypeInformation[_]] = Array(
    Types.STRING,
    Types.INT,
    Types.INT,
    Types.LONG)
  val dataType = new RowTypeInfo(fieldTypes :_*)

  val data_with_partitions: Seq[Row] = Seq(
    row("x1", 1, 1, 1L),
    row("x2", 2, 1, 1L),
    row("x3", 3, 1, 1L),
    row("x4", 4, 1, 1L),
    row("x5", 5, 1, 1L),
    row("x6", 6, 1, 2L),
    row("x7", 7, 1, 2L),
    row("x8", 8, 1, 2L),
    row("x9", 9, 1, 2L),
    row("x10", 10, 1, 2L),
    row("x11", 11, 2, 1L),
    row("x12", 12, 2, 1L),
    row("x13", 13, 2, 1L),
    row("x14", 14, 2, 1L),
    row("x15", 15, 2, 1L),
    row("x16", 16, 2, 2L),
    row("x17", 17, 3, 1L),
    row("x18", 18, 3, 2L),
    row("x19", 19, 3, 3L),
    row("x20", 20, 4, 1L),
    row("x21", 21, 4, 2L),
    row("x22", 22, 4, 3L),
    row("x23", 23, 4, 4L),
    row("x24", 24, 5, 1L),
    row("x25", 25, 5, 2L),
    row("x26", 26, 5, 3L),
    row("x27", 27, 5, 4L)
  )

  val data: Seq[Row] = data_with_partitions.map(row => Row.of(row.getField(0), row.getField(1)))

  val data_partition_1_1: Seq[Row] = Seq(
    row("x1", 1),
    row("x2", 2),
    row("x3", 3),
    row("x4", 4),
    row("x5", 5)
  )

  val data_partition_2_1: Seq[Row] = Seq(
    row("x11", 11),
    row("x12", 12),
    row("x13", 13),
    row("x14", 14),
    row("x15", 15)
  )
}
