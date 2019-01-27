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

package org.apache.flink.table.api.stream.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.util.MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class CalcValidationTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  // test should fail because where condition returns false
  @Test(expected = classOf[TableException])
  def testValuesSourceInput(): Unit = {
    val util = streamTestUtil()

    util.addTable[(Double, Double, Double)]("MyTable_1", 'a1, 'a2, 'a3)

    val sinkFieldNames = Array("c1", "c2", "c3")
    val sinkFieldTypes: Array[DataType] =
      Array(DataTypes.DOUBLE, DataTypes.DOUBLE, DataTypes.DOUBLE)
    util.tableEnv.registerTableSink(
      "sink_1",
      sinkFieldNames,
      sinkFieldTypes,
      new UnsafeMemoryAppendTableSink)

    val sql =
      """
        |INSERT INTO sink_1
        |SELECT * FROM MyTable_1 where 0=1
      """.stripMargin

    util.tableEnv.sqlUpdate(sql)
  }
}
