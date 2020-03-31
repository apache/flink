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
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData.{data3, nullablesOfData3, type3}
import org.apache.flink.table.planner.utils.MemoryTableSourceSinkUtil
import org.apache.flink.table.planner.utils.MemoryTableSourceSinkUtil.{DataTypeAppendStreamTableSink, DataTypeOutputFormatTableSink}
import org.apache.flink.test.util.TestBaseUtils

import org.junit._

import scala.collection.JavaConverters._

class TableSinkITCase extends BatchTestBase {

  @Test
  def testDecimalOutputFormatTableSink(): Unit = {
    MemoryTableSourceSinkUtil.clear()

    val schema = TableSchema.builder()
        .field("c", DataTypes.VARCHAR(5))
        .field("b", DataTypes.DECIMAL(10, 0))
        .field("d", DataTypes.CHAR(5))
        .build()
    val sink = new DataTypeOutputFormatTableSink(schema)
    tEnv.registerTableSink("testSink", sink)

    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

    tEnv.scan("Table3")
        .where('a > 20)
        .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
        .insertInto("testSink")

    tEnv.execute("")

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
    val sink = new DataTypeAppendStreamTableSink(schema)
    tEnv.registerTableSink("testSink", sink)

    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

    tEnv.scan("Table3")
        .where('a > 20)
        .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
        .insertInto("testSink")

    tEnv.execute("")

    val results = MemoryTableSourceSinkUtil.tableDataStrings.asJava
    val expected = Seq("12345,55,12345").mkString("\n")

    TestBaseUtils.compareResultAsText(results, expected)
  }
}
