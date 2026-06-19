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
import org.apache.flink.connector.file.table.PartitionCommitPolicy
import org.apache.flink.core.fs.{FileSystem, Path => FPath}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.FileSystemITCaseBase._
import org.apache.flink.table.planner.runtime.utils.BatchTableEnvUtil
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.testutils.junit.utils.TempDirUtils
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.net.URI
import java.nio.file.{Path, Paths}
import java.time.Instant

import scala.collection.{JavaConverters, Seq}

/** Test File system table factory. */
trait FileSystemITCaseBase {

  protected var resultPath: String = _

  @TempDir
  protected var fileTempFolder: Path = _

  def formatProperties(): Array[String] = Array()

  def getScheme: String = "file"

  def tableEnv: TableEnvironment

  def checkPredicate(sqlQuery: String, checkFunc: Row => Unit): Unit

  def check(sqlQuery: String, expectedResult: Seq[Row]): Unit

  def check(sqlQuery: String, expectedResult: java.util.List[Row]): Unit = {
    check(
      sqlQuery,
      JavaConverters.asScalaIteratorConverter(expectedResult.iterator()).asScala.toSeq)
  }

  def supportsReadingMetadata: Boolean = true

  def open(): Unit = {
    resultPath = TempDirUtils.newFolder(fileTempFolder).toURI.getPath
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
         |  b bigint,
         |  c as b + 1
         |) partitioned by (a, b) with (
         |  'connector' = 'filesystem',
         |  'path' = '$getScheme://$resultPath',
         |  ${formatProperties().mkString(",\n")}
         |)
       """.stripMargin
    )
    if (supportsReadingMetadata) {
      tableEnv.executeSql(
        s"""
           |create table partitionedTableWithMetadata (
           |  x string,
           |  y int,
           |  a int,
           |  b bigint,
           |  c as b + 1,
           |  f string metadata from 'file.path'
           |) partitioned by (a, b) with (
           |  'connector' = 'filesystem',
           |  'path' = '$resultPath',
           |  ${formatProperties().mkString(",\n")}
           |)
           """.stripMargin
      )
    }
    tableEnv.executeSql(
      s"""
         |create table nonPartitionedTable (
         |  x string,
         |  y int,
         |  a int,
         |  b bigint
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$getScheme://$resultPath',
         |  ${formatProperties().mkString(",\n")}
         |)
       """.stripMargin
    )
    if (supportsReadingMetadata) {
      tableEnv.executeSql(
        s"""
           |create table nonPartitionedTableWithMetadata (
           |  x string,
           |  y int,
           |  a int,
           |  f string metadata from 'file.path',
           |  b bigint
           |) with (
           |  'connector' = 'filesystem',
           |  'path' = '$resultPath',
           |  ${formatProperties().mkString(",\n")}
           |)
         """.stripMargin
      )
    }

    tableEnv.executeSql(
      s"""
         |create table hasDecimalFieldWithPrecisionTenAndZeroTable (
         |  x decimal(10, 0), y int
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$getScheme://$resultPath',
         |  ${formatProperties().mkString(",\n")}
         |)
       """.stripMargin
    )

    tableEnv.executeSql(
      s"""
         |create table hasDecimalFieldWithPrecisionThreeAndTwoTable (
         |  x decimal(3, 2), y int
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$getScheme://$resultPath',
         |  ${formatProperties().mkString(",\n")}
         |)
       """.stripMargin
    )

    tableEnv.executeSql(
      s"""
         |create table table_custom_partition_commit_policy (
         |  x varchar, y int
         |) PARTITIONED BY (x) with (
         |  'connector' = 'filesystem',
         |  'path' = '$getScheme://$resultPath',
         |  ${formatProperties().mkString(",\n")},
         |  'sink.partition-commit.policy.kind' = 'custom',
         |  'sink.partition-commit.policy.class' = '${classOf[TestPolicy].getName}'
         |)
 """.stripMargin
    )
  }

  @TestTemplate
  def testSelectDecimalWithPrecisionTenAndZeroFromFileSystem(): Unit = {
    tableEnv
      .executeSql(
        "insert into hasDecimalFieldWithPrecisionTenAndZeroTable(x, y) " +
          "values(cast(2113554011 as decimal(10, 0)), 1), " +
          "(cast(2113554022 as decimal(10,0)), 2)")
      .await()

    check(
      "select x, y from hasDecimalFieldWithPrecisionTenAndZeroTable",
      Seq(
        row(2113554011, 1),
        row(2113554022, 2)
      ))
  }

  @TestTemplate
  def testSelectDecimalWithPrecisionThreeAndTwoFromFileSystem(): Unit = {
    tableEnv
      .executeSql(
        "insert into hasDecimalFieldWithPrecisionThreeAndTwoTable(x,y) " +
          "values(cast(1.32 as decimal(3, 2)), 1), " +
          "(cast(2.64 as decimal(3, 2)), 2)")
      .await()

    check(
      "select x, y from hasDecimalFieldWithPrecisionThreeAndTwoTable",
      Seq(
        row(1.32, 1),
        row(2.64, 2)
      ))
  }

  @TestTemplate
  def testAllStaticPartitions1(): Unit = {
    tableEnv
      .executeSql(
        "insert into partitionedTable " +
          "partition(a='1', b='1') select x, y from originalT where a=1 and b=1")
      .await()

    check(
      "select x, y from partitionedTable where a=1 and b=1",
      data_partition_1_1
    )

    check(
      "select x, y from partitionedTable",
      data_partition_1_1
    )
  }

  @TestTemplate
  def testAllStaticPartitions2(): Unit = {
    tableEnv
      .executeSql(
        "insert into partitionedTable " +
          "partition(a='2', b='1') select x, y from originalT where a=2 and b=1")
      .await()

    check(
      "select x, y from partitionedTable where a=2 and b=1",
      data_partition_2_1
    )

    check(
      "select x, y from partitionedTable",
      data_partition_2_1
    )
  }

  @TestTemplate
  def testAllStaticPartitionsWithMetadata(): Unit = {
    if (!supportsReadingMetadata) {
      return
    }

    tableEnv
      .executeSql(
        "insert into partitionedTable " +
          "partition(a='1', b='1') select x, y from originalT where a=1 and b=1")
      .await()

    checkPredicate(
      "select x, f, y from partitionedTableWithMetadata where a=1 and b=1",
      row => {
        assertThat(row.getArity).isEqualTo(3)
        assertThat(row.getField("f")).isNotNull
        assertThat(row.getField(1)).isNotNull

        assertThat(row.getFieldAs[String](1).contains(fileTempFolder.getParent.toString)).isTrue
      }
    )

    checkPredicate(
      "select x, f, y from partitionedTableWithMetadata",
      row => {
        assertThat(row.getArity).isEqualTo(3)
        assertThat(row.getField("f")).isNotNull
        assertThat(row.getField(1)).isNotNull
        assertThat(row.getFieldAs[String](1).contains(fileTempFolder.getRoot.toString)).isTrue
      }
    )
  }

  @TestTemplate
  def testPartialDynamicPartition(): Unit = {
    tableEnv
      .executeSql(
        "insert into partitionedTable " +
          "partition(a=3) select x, y, b from originalT where a=3")
      .await()

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

  @TestTemplate
  def testDynamicPartition(): Unit = {
    tableEnv
      .executeSql(
        "insert into partitionedTable " +
          "select x, y, a, b from originalT")
      .await()

    check(
      "select x, y from partitionedTable where a=1 and b=1",
      data_partition_1_1
    )

    check(
      "select x, y from partitionedTable where a=2 and b=1",
      data_partition_2_1
    )

    check(
      "select x, y, a, b, c from partitionedTable where a=1 and c=2",
      data_partition_1_2
    )

    check(
      "select x, y from partitionedTable",
      data
    )
  }

  @TestTemplate
  def testPartitionWithHiddenFile(): Unit = {
    tableEnv
      .executeSql(
        "insert into partitionedTable " +
          "partition(a='1', b='1') select x, y from originalT where a=1 and b=1")
      .await()

    // create hidden partition dir
    assertThat(
      new File(new org.apache.flink.core.fs.Path("file:" + resultPath + "/a=1/.b=2").toUri)
        .mkdir()).isTrue

    check(
      "select x, y from partitionedTable",
      data_partition_1_1
    )
  }

  @TestTemplate
  def testNonPartition(): Unit = {
    tableEnv
      .executeSql(
        "insert into nonPartitionedTable " +
          "select x, y, a, b from originalT where a=1 and b=1")
      .await()

    check(
      "select x, y from nonPartitionedTable where a=1 and b=1",
      data_partition_1_1
    )
  }

  @TestTemplate
  def testNonPartitionWithMetadata(): Unit = {
    if (!supportsReadingMetadata) {
      return
    }

    tableEnv
      .executeSql(
        "insert into nonPartitionedTable " +
          "select x, y, a, b from originalT where a=1 and b=1")
      .await()

    checkPredicate(
      "select x, f, y from nonPartitionedTableWithMetadata where a=1 and b=1",
      row => {
        assertThat(row.getArity).isEqualTo(3)
        assertThat(row.getField("f")).isNotNull
        assertThat(row.getField(1)).isNotNull
        assertThat(row.getFieldAs[String](1).contains(fileTempFolder.getRoot.toString)).isTrue
      }
    )
  }

  @TestTemplate
  def testReadAllMetadata(): Unit = {
    if (!supportsReadingMetadata) {
      return
    }

    tableEnv.executeSql(
      s"""
         |CREATE TABLE metadataTable (
         |  x STRING,
         |  `file.path` STRING METADATA,
         |  `file.name` STRING METADATA,
         |  `file.size` BIGINT METADATA,
         |  `file.modification-time` TIMESTAMP_LTZ(3) METADATA
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$resultPath',
         |  ${formatProperties().mkString(",\n")}
         |)
         """.stripMargin
    )

    tableEnv
      .executeSql("INSERT INTO nonPartitionedTable (x) SELECT x FROM originalT LIMIT 1")
      .await()

    checkPredicate(
      "SELECT * FROM metadataTable",
      row => {
        assertThat(row.getArity).isEqualTo(5)

        // Only one file, because we don't have partitions
        val file = new File(URI.create(resultPath).getPath).listFiles()(0)
        val filename = Paths.get(file.toURI).getFileName.toString

        assertThat(row.getFieldAs[String](1).contains(filename)).isTrue
        assertThat(row.getFieldAs[String](2)).isEqualTo(filename)
        assertThat(row.getFieldAs[Long](3)).isEqualTo(file.length())
        assertThat(row.getFieldAs[Instant](4)).isEqualTo(
          // Note: It's TIMESTAMP_LTZ
          Instant.ofEpochMilli(file.lastModified()))
      }
    )

  }

  @TestTemplate
  def testLimitPushDown(): Unit = {
    tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(1))
    tableEnv.executeSql("insert into nonPartitionedTable select x, y, a, b from originalT").await()

    check(
      "select x, y from nonPartitionedTable limit 3",
      Seq(row("x1", 1), row("x2", 2), row("x3", 3)))
  }

  @TestTemplate
  def testFilterPushDown(): Unit = {
    tableEnv.executeSql("insert into nonPartitionedTable select x, y, a, b from originalT").await()

    check("select x, y from nonPartitionedTable where a=10086", Seq())
  }

  @TestTemplate
  def testProjectPushDown(): Unit = {
    tableEnv.executeSql("insert into partitionedTable select x, y, a, b from originalT").await()

    check(
      "select y, b, x from partitionedTable where a=3",
      Seq(
        row(17, 1, "x17"),
        row(18, 2, "x18"),
        row(19, 3, "x19")
      ))
  }

  @TestTemplate
  def testInsertAppend(): Unit = {
    tableEnv
      .executeSql("insert into partitionedTable select x, y, a, b from originalT")
      .await()

    tableEnv
      .executeSql("insert into partitionedTable select x, y, a, b from originalT")
      .await()

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

  @TestTemplate
  def testInsertOverwrite(): Unit = {
    tableEnv
      .executeSql("insert overwrite partitionedTable select x, y, a, b from originalT")
      .await()

    tableEnv
      .executeSql("insert overwrite partitionedTable select x, y, a, b from originalT")
      .await()

    check(
      "select y, b, x from partitionedTable where a=3",
      Seq(
        row(17, 1, "x17"),
        row(18, 2, "x18"),
        row(19, 3, "x19")
      ))
  }

  @TestTemplate
  def testCustomPartitionCommitPolicy(): Unit = {
    tableEnv
      .executeSql("insert into table_custom_partition_commit_policy values ('p1', 1), ('p1', 2)")
      .await()
    val file = new File(s"$resultPath/x=p1/_custom_commit")
    assertTrue(file.exists())
  }
}

object FileSystemITCaseBase {

  val fieldNames = Array("x", "y", "a", "b")

  val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.INT, Types.LONG)
  val dataType = new RowTypeInfo(fieldTypes: _*)

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

  val data_partition_1_2: Seq[Row] = Seq(
    row("x1", 1, 1, 1, 2),
    row("x2", 2, 1, 1, 2),
    row("x3", 3, 1, 1, 2),
    row("x4", 4, 1, 1, 2),
    row("x5", 5, 1, 1, 2)
  )

  class TestPolicy extends PartitionCommitPolicy {

    /** Commit a partition. */
    override def commit(context: PartitionCommitPolicy.Context): Unit = {
      val fs = context.partitionPath().getFileSystem
      fs.create(new FPath(context.partitionPath, "_custom_commit"), FileSystem.WriteMode.OVERWRITE)
        .close()
    }
  }
}
