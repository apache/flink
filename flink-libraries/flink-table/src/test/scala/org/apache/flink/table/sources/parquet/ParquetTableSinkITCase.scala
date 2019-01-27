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

package org.apache.flink.table.sources.parquet

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.{FileStatus, Path}
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sinks.parquet.ParquetTableSink
import org.apache.flink.table.util.NodeResourceUtil
import org.apache.flink.test.util.TestBaseUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.parquet.example.data.GroupValueSource
import org.apache.parquet.hadoop.example.ExampleInputFormat
import org.junit.Assert.assertEquals
import org.junit.{Assert, Before, Test}

import java.nio.file.Files
import java.util

import scala.collection.JavaConversions._

class ParquetTableSinkITCase extends BatchTestBase {

  @Before
  def setUp(): Unit = {
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_RESOURCE_INFER_MODE,
      NodeResourceUtil.InferMode.ONLY_SOURCE.toString)
  }

  @Test
  def testParquetTableSinkOverWrite():Unit = {

    // write
    val parquetTable1 = CommonParquetTestData.getParquetVectorizedColumnRowTableSource
    tEnv.registerTableSource("parquetTable1", parquetTable1)
    val tempFile = Files.createTempFile("parquet-sink", "test")
    tempFile.toFile.deleteOnExit()
    try {
      tEnv.sqlQuery("SELECT id, `first`, `last`, score FROM parquetTable1").writeToSink(
        new ParquetTableSink(tempFile.toFile.getAbsolutePath))
      Assert.fail("runtime exception expected");
    } catch {
      case _: RuntimeException => None
      case _ => Assert.fail("runtime exception expected");
    }

    tEnv.sqlQuery("SELECT id, `first`, `last`, score FROM parquetTable1").writeToSink(
      new ParquetTableSink(tempFile.toFile.getAbsolutePath, Some(WriteMode.OVERWRITE)))
    tEnv.execute()

  }

  @Test
  def testParquetTableSink(): Unit = {

    // write
    val parquetTable1 = CommonParquetTestData.getParquetVectorizedColumnRowTableSource
    tEnv.registerTableSource("parquetTable1", parquetTable1)
    val tempFile = Files.createTempDirectory("parquet-sink")
    tEnv.sqlQuery("SELECT id, `first`, `last`, score FROM parquetTable1").writeToSink(
      new ParquetTableSink(tempFile.toFile.getAbsolutePath))
    tEnv.execute()
    tempFile.toFile.deleteOnExit()

    // read
    val names = Array("id", "first", "last", "score")
    val types: Array[InternalType] = Array(
      DataTypes.INT,
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.DOUBLE
    )
    val parquetTable2 = new ParquetVectorizedColumnRowTableSource(
      new Path(tempFile.toFile.getAbsolutePath),
      types,
      names,
      true
    )
    tEnv.registerTableSource("parquetTable2", parquetTable2)
    val results = tEnv.sqlQuery("SELECT * FROM parquetTable2").collect()
    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89",
      "4,Peter,Smith,0.12",
      "5,Liz,Williams,34.5",
      "6,Sally,Miller,6.78",
      "7,Alice,Smith,90.1",
      "8,Kelly,Williams,2.34").mkString("\n")

    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDecimalType(): Unit = {

    // write
    val tableSource = CommonTestData.getWithDecimalCsvTableSource
    tEnv.registerTableSource("MyTable", tableSource)
    val tempFile = Files.createTempDirectory("parquet-sink")
    tEnv.sqlQuery("SELECT * FROM MyTable").writeToSink(
      new ParquetTableSink(tempFile.toFile.getAbsolutePath))
    tEnv.execute()
    tempFile.toFile.deleteOnExit()
    val files = new util.ArrayList[FileStatus]()
    listFileStatus(new Path(tempFile.toUri), files)
    assertEquals(1, files.size)

    // read
    val split = new FileSplit(
      new org.apache.hadoop.fs.Path(files.head.getPath.toUri), 0, files.head.getLen, null)
    val attemptId = new TaskAttemptID(new TaskID(new JobID, TaskType.MAP, 0), 0)
    val taskAttemptContext = new TaskAttemptContextImpl(new Configuration, attemptId)
    val format = new ExampleInputFormat()
    val reader = format.createRecordReader(split, taskAttemptContext)
    reader.initialize(split, taskAttemptContext)
    val result = new util.ArrayList[String]()
    while (reader.nextKeyValue()) {
      result.add(groupToString(reader.getCurrentValue))
    }

    // verify
    val expected = Seq(
      "0.00,0,0.0000,0.000000000,0,0.0,0.0000000000,0,0.00000,0.000000000000000000,0,0,0.000",
      "1.23,999999999,12345.6700,0.000000001,2147483648,12345678.9,0.0000000001," +
          "999999999999999999,9876543210.98765,0.000000000000000001,9223372036854775808," +
          "99999999999999999999999999999999999999,123456789012345678901.234",
      "-1.23,-999999999,-12345.6700,-0.000000001,-2147483649,-12345678.9,-0.0000000001," +
          "-999999999999999999,-9876543210.98765,-0.000000000000000001,-9223372036854775809," +
          "-99999999999999999999999999999999999999,-123456789012345678901.234"
    ).mkString("\n")

    TestBaseUtils.compareResultAsText(result, expected)
  }

  private def groupToString(group: GroupValueSource): String = {
    Seq(Decimal.fromUnscaledLong(7, 2, group.getInteger(0, 0)),
      Decimal.fromUnscaledLong(9, 0, group.getInteger(1, 0)),
      Decimal.fromUnscaledLong(9, 4, group.getInteger(2, 0)),
      Decimal.fromUnscaledLong(9, 9, group.getInteger(3, 0)),
      Decimal.fromUnscaledLong(10, 0, group.getLong(4, 0)),
      Decimal.fromUnscaledLong(10, 1, group.getLong(5, 0)),
      Decimal.fromUnscaledLong(10, 10, group.getLong(6, 0)),
      Decimal.fromUnscaledLong(18, 0, group.getLong(7, 0)),
      Decimal.fromUnscaledLong(18, 5, group.getLong(8, 0)),
      Decimal.fromUnscaledLong(18, 18, group.getLong(9, 0)),
      Decimal.fromUnscaledBytes(19, 0, group.getBinary(10, 0).getBytes),
      Decimal.fromUnscaledBytes(38, 0, group.getBinary(11, 0).getBytes),
      Decimal.fromUnscaledBytes(38, 3, group.getBinary(12, 0).getBytes)
    ).mkString(",")
  }

  private def listFileStatus(path: Path, files: util.List[FileStatus]): Unit = {
    path.getFileSystem.listStatus(path).foreach { s =>
      if (s.isDir) {
        listFileStatus(s.getPath, files)
      } else {
        if (!s.getPath.getName.startsWith(".")) {
          files.add(s)
        }
      }
    }
  }

}
