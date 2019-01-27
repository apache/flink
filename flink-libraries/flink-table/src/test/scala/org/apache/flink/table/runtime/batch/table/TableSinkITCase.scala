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

package org.apache.flink.table.runtime.batch.table

import java.io.File

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.CollectionBatchExecTable
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test

class TableSinkITCase extends BatchTestBase {

  @Test
  def testBatchTableSink(): Unit = {

    val path = createTempFile(0)

    val input = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    input
      .where('a < 5 || 'a > 17)
      .select('c, 'b)
      .writeToSink(
        new CsvTableSink(path, Some("|"), None, None, None, Some(WriteMode.OVERWRITE), None, None))

    tEnv.execute()

    val expected = Seq(
      "Hi|1", "Hello|2", "Hello world|2", "Hello world, how are you?|3",
      "Comment#12|6", "Comment#13|6", "Comment#14|6", "Comment#15|6").mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }

  @Test
  def testBatchTableSinkWithQuotedString(): Unit = {
    val path = createTempFile(0)

    val data = List(
      ("\"one\"", 1),
      ("\"two\"", 2),
      ("\"three\"", 3),
      ("\"four\"", 4),
      ("\"five\"", 5)
    )

    val input = tEnv.fromCollection(data).as('a, 'b)

    input
      .where('b < 3 || 'b > 4)
      .select('a, 'b)
      .writeToSink(
        new CsvTableSink(path, Some("|"), None, Some("\""),
          None, Some(WriteMode.OVERWRITE), None, None))

    tEnv.execute()

    val expected = Seq(
      "\"\"\"one\"\"\"|1", "\"\"\"two\"\"\"|2", "\"\"\"five\"\"\"|5").mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }

  @Test
  def testDecimal(): Unit = {
    val path = createTempFile(0)

    val data = List(
      (1, 1E-7),
      (2, 2E-7),
      (3, 3E-7))

    val input = tEnv.fromCollection(data).as('a, 'b)

    val tpe = DataTypes.createDecimalType(10, 8)
    input
      .select('a.cast(tpe), 'b.cast(tpe))
      .writeToSink(
        new CsvTableSink(path, Some("|"), None, Some("\""),
          None, Some(WriteMode.OVERWRITE), None, None))

    tEnv.execute()

    val expected = Seq(
      "1.00000000|0.00000010",
      "2.00000000|0.00000020",
      "3.00000000|0.00000030"
    ).mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }

  @Test
  def testMultipleBatchTableSinks1(): Unit = {
    val path1 = createTempFile(1)
    val path2 = createTempFile(2)

    conf.setSubsectionOptimization(true)

    tEnv.registerTableSource("test", CommonTestData.getCsvTableSource)

    val table = tEnv.scan("test").select('id, 'first, 'score)

    table.where('score > 10 && 'score < 40)
      .select('id, 'first, 'score)
      .writeToSink(new CsvTableSink(
        path1, Some("|"), None, None, None, Some(WriteMode.OVERWRITE), None, None))

    table.where('score <= 10 || 'score >= 40)
      .select('id, 'first)
      .writeToSink(
        new CsvTableSink(path2, Some("|"), None, None, None, Some(WriteMode.OVERWRITE), None, None))

    tEnv.execute()

    val expected1 = Seq("1|Mike|12.3", "5|Liz|34.5").mkString("\n")
    TestBaseUtils.compareResultsByLinesInMemory(expected1, path1)

    val expected2 = Seq("2|Bob", "3|Sam", "4|Peter", "6|Sally", "7|Alice", "8|Kelly").mkString("\n")
    TestBaseUtils.compareResultsByLinesInMemory(expected2, path2)
  }

  @Test
  def testMultipleBatchTableSinks2(): Unit = {
    val path1 = createTempFile(1)
    val path2 = createTempFile(2)
    val path3 = createTempFile(3)

    tEnv.registerTableSource("test", CommonTestData.getCsvTableSource)

    val table = tEnv.scan("test")

    val table1 = table.where('score <= 40).select('id as 'id1, 'first)
    table1.writeToSink(
      new CsvTableSink(path1, Some("|"), None, None, None, Some(WriteMode.OVERWRITE), None, None))

    val table2 = table.where('score >= 10).select('id as 'id2, 'last)

    val table3 = table1.join(table2, 'id1 === 'id2).select('id1 as 'id, 'first, 'last)
    table3.writeToSink(
      new CsvTableSink(path2, Some("|"), None, None, None, Some(WriteMode.OVERWRITE), None, None))

    table3.filter('id >= 3).writeToSink(
      new CsvTableSink(
        path3, Some("|"), None, None, None, Some(WriteMode.OVERWRITE), None, None))

    tEnv.execute()

    val expected1 = Seq("1|Mike", "3|Sam", "4|Peter", "5|Liz", "6|Sally", "8|Kelly").mkString("\n")
    TestBaseUtils.compareResultsByLinesInMemory(expected1, path1)

    val expected2 = Seq("1|Mike|Smith", "5|Liz|Williams").mkString("\n")
    TestBaseUtils.compareResultsByLinesInMemory(expected2, path2)

    val expected3 = Seq("5|Liz|Williams").mkString("\n")
    TestBaseUtils.compareResultsByLinesInMemory(expected3, path3)
  }

  private def createTempFile(index: Int): String = {
    val tmpFile = File.createTempFile(s"flink-table-sink-test-$index-", ".tmp")
    tmpFile.deleteOnExit()
    tmpFile.toURI.toString
  }

}
