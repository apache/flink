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

package org.apache.flink.table.api.batch.sql.validation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.utils.{MemoryTableSourceSinkUtil, TableTestBase}

import org.junit._

class InsertIntoValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testInconsistentLengthInsert(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    val sql = "INSERT INTO targetTable SELECT a, b, c FROM sourceTable"

    // must fail because table sink schema has too few fields
    util.tableEnv.sqlUpdate(sql)
    // trigger validation
    util.tableEnv.execute("test")
  }

  @Test(expected = classOf[ValidationException])
  def testUnmatchedTypesInsert(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    val sql = "INSERT INTO targetTable SELECT a, b, c FROM sourceTable"

    // must fail because types of table sink do not match query result
    util.tableEnv.sqlUpdate(sql)
    // trigger validation
    util.tableEnv.execute("test")
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedPartialInsert(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)

    val fieldNames = Array("d", "e", "f")
    val fieldTypes = util.tableEnv.scan("sourceTable").getSchema.getFieldTypes
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    val sql = "INSERT INTO targetTable (d, f) SELECT a, c FROM sourceTable"

    // must fail because partial insert is not supported yet.
    util.tableEnv.sqlUpdate(sql)
    // trigger validation
    util.tableEnv.execute("test")
  }

  @Test
  def testValidationExceptionMessage(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("TableSink schema:    [a: Integer, b: Row" +
      "(f0: Integer, f1: Integer, f2: Integer)]")
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)
    val fieldNames = Array("a", "b")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.ROW
    (Types.INT, Types.INT, Types.INT))
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    val sql = "INSERT INTO targetTable SELECT a, b FROM sourceTable"

    util.tableEnv.sqlUpdate(sql)
    // trigger validation
    util.tableEnv.execute("test")
  }
}
