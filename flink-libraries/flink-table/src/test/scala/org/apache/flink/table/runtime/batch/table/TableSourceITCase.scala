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

import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.util.{TestFilterableTableSource, TestPartitionableTableSource}
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Assert.assertTrue
import org.junit.Test

import scala.collection.JavaConverters._

class TableSourceITCase extends BatchTestBase {

  @Test
  def testCsvTableSourceWithProjection(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource

    tEnv.registerTableSource("csvTable", csvTable)

    val results = tEnv
      .scan("csvTable")
      .where('score < 20)
      .select('last, 'id.floor(), 'score * 2)
      .collect()

    val expected = Seq(
      "Smith,1,24.6",
      "Miller,3,15.78",
      "Smith,4,0.24",
      "Miller,6,13.56",
      "Williams,8,4.68").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCsvTableSourceWithProjectionWithCount1(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource

    tEnv.registerTableSource("csvTable", csvTable)

    val results = tEnv
      .scan("csvTable")
      .select(1.count)
      .collect()

    val expected = "8"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    val tableName = "MyTable"
    tEnv.registerTableSource(tableName, new TestFilterableTableSource)
    val results = tEnv
      .scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .collect()

    val expected = Seq(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPartitionableTableSourceWithPartitionFields(): Unit = {

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val results = tEnv.scan("partitionable_table")
      .where('part === "2" || 'part === "1" && 'id > 2)
      .collect()

    val expected = Seq(
      "3,John,2,part=1#part=2,true",
      "4,nosharp,2,part=1#part=2,true").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPartitionableTableSourceWithPartitionFieldsAndBoolTypeColumn(): Unit = {

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val results = tEnv.scan("partitionable_table")
      .where('is_ok && 'part === "2")
      .collect()

    val expected = Seq("3,John,2,part=2,true", "4,nosharp,2,part=2,true").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPartitionableTableSourceWithPartitionFieldsAndBoolTypeColumn1(): Unit = {

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val results = tEnv.scan("partitionable_table")
                  .where('is_ok === true && 'part === "2")
                  .collect()

    val expected = Seq("3,John,2,part=2,true", "4,nosharp,2,part=2,true").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPartitionableTableSourceWithoutPartitionFields(): Unit = {

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)

    val results = tEnv.scan("partitionable_table")
      .where('name === "Lucy")
      .collect()

    val expected = Seq("6,Lucy,3,null,true").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithEmptyFile(): Unit = {
    // create empty file
    val tempFile = File.createTempFile("csv-test", "tmp")
    tempFile.deleteOnExit()

    val csvTable = CsvTableSource.builder()
        .path(tempFile.getAbsolutePath)
        .field("a", DataTypes.INT)
        .build()

    tEnv.registerTableSource("csvTable", csvTable)

    val results = tEnv.scan("csvTable").where('a < 20).collect()
    assertTrue(results.isEmpty)
  }
}
