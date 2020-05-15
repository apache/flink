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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.planner.utils.MemoryTableSourceSinkUtil
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Assert._
import org.junit._
import java.util.TimeZone

import org.apache.flink.table.api.internal.TableEnvironmentInternal

import scala.collection.JavaConverters._

class LegacyTableSinkITCase extends BatchTestBase {

  @Test
  def testDecimalOutputFormatTableSink(): Unit = {
    MemoryTableSourceSinkUtil.clear()

    val schema = TableSchema.builder()
        .field("c", DataTypes.VARCHAR(5))
        .field("b", DataTypes.DECIMAL(10, 0))
        .field("d", DataTypes.CHAR(5))
        .build()

    MemoryTableSourceSinkUtil.createDataTypeOutputFormatTable(
      tEnv, schema, "testSink")
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

    val table = tEnv.from("Table3")
        .where('a > 20)
        .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    execInsertTableAndWaitResult(table, "testSink")

    val results = MemoryTableSourceSinkUtil.tableDataStrings.asJava
    val expected = Seq("12345,55,12345").mkString("\n")

    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDecimalAppendStreamTableSink(): Unit = {
    MemoryTableSourceSinkUtil.clear()

    val schema = TableSchema.builder()
        .field("c", DataTypes.VARCHAR(5))
        .field("b", DataTypes.DECIMAL(10, 0))
        .field("d", DataTypes.CHAR(5))
        .build()

    MemoryTableSourceSinkUtil.createDataTypeAppendStreamTable(
      tEnv, schema, "testSink")

    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

    val table = tEnv.from("Table3")
        .where('a > 20)
        .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    execInsertTableAndWaitResult(table, "testSink")

    val results = MemoryTableSourceSinkUtil.tableDataStrings.asJava
    val expected = Seq("12345,55,12345").mkString("\n")

    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Ignore
  @Test
  def testDecimalForLegacyTypeTableSink(): Unit = {
    MemoryTableSourceSinkUtil.clear()

    val schema = TableSchema.builder()
      .field("a", DataTypes.VARCHAR(5))
      .field("b", DataTypes.DECIMAL(10, 0))
      .build()

    // TODO: should failed at TableSinkUtils::validateLogicalPhysicalTypesCompatible
    // CatalogTable hold new type but sink hold legacy type
    MemoryTableSourceSinkUtil.createLegacyUnsafeMemoryAppendTable(
      tEnv, schema, "testSink")

    registerCollection("Table3", simpleData2, simpleType2, "a, b", nullableOfSimpleData2)

    val table = tEnv.from("Table3")
      .select('a.cast(DataTypes.STRING()), 'b.cast(DataTypes.DECIMAL(10, 2)))
      .distinct()
    execInsertTableAndWaitResult(table, "testSink")

    val results = MemoryTableSourceSinkUtil.tableDataStrings.asJava
    val expected = Seq("1,0.100000000000000000", "2,0.200000000000000000",
      "3,0.300000000000000000", "3,0.400000000000000000", "4,0.500000000000000000",
      "4,0.600000000000000000", "5,0.700000000000000000", "5,0.800000000000000000",
      "5,0.900000000000000000").mkString("\n")

    TestBaseUtils.compareResultAsText(results, expected)
  }

  private def prepareForUpsertSink(): TestingUpsertTableSink = {
    val schema = TableSchema.builder()
        .field("a", DataTypes.INT())
        .field("b", DataTypes.DOUBLE())
        .build()
    val sink = new TestingUpsertTableSink(Array(0), TimeZone.getDefault)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "testSink", sink.configure(schema.getFieldNames, schema.getFieldTypes))
    registerCollection("MyTable", simpleData2, simpleType2, "a, b", nullableOfSimpleData2)
    sink
  }

  @Test
  def testUpsertSink(): Unit = {
    val sink = prepareForUpsertSink()
    sink.expectedKeys = Some(Array("a"))
    sink.expectedIsAppendOnly = Some(false)

   val table = tEnv.from("MyTable")
        .groupBy('a)
        .select('a, 'b.sum())
    execInsertTableAndWaitResult(table, "testSink")

    val result = sink.getUpsertResults.sorted
    val expected = List(
      "1,0.1",
      "2,0.4",
      "3,1.0",
      "4,2.2",
      "5,3.9").sorted
    assertEquals(expected, result)
  }

  @Test
  def testUpsertSinkWithAppend(): Unit = {
    val sink = prepareForUpsertSink()
    sink.expectedKeys = None
    sink.expectedIsAppendOnly = Some(true)

    val table = tEnv.from("MyTable")
        .select('a, 'b)
        .where('a < 3)
    execInsertTableAndWaitResult(table, "testSink")

    val result = sink.getRawResults.sorted
    val expected = List(
      "(true,1,0.1)",
      "(true,2,0.2)",
      "(true,2,0.2)").sorted
    assertEquals(expected, result)
  }

  private def prepareForRetractSink(): TestingRetractTableSink = {
    val schema = TableSchema.builder()
        .field("a", DataTypes.INT())
        .field("b", DataTypes.DOUBLE())
        .build()
    val sink = new TestingRetractTableSink(TimeZone.getDefault)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "testSink", sink.configure(schema.getFieldNames, schema.getFieldTypes))
    registerCollection("MyTable", simpleData2, simpleType2, "a, b", nullableOfSimpleData2)
    sink
  }

  @Test
  def testRetractSink(): Unit = {
    val sink = prepareForRetractSink()

    val table = tEnv.from("MyTable")
        .groupBy('a)
        .select('a, 'b.sum())
    execInsertTableAndWaitResult(table, "testSink")

    val result = sink.getRawResults.sorted
    val expected = List(
      "(true,1,0.1)",
      "(true,2,0.4)",
      "(true,3,1.0)",
      "(true,4,2.2)",
      "(true,5,3.9)").sorted
    assertEquals(expected, result)
  }
}
