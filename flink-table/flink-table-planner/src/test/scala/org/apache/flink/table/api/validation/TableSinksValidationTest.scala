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

package org.apache.flink.table.api.validation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableException, Types, ValidationException}
import org.apache.flink.table.runtime.stream.table.TestAppendSink
import org.apache.flink.table.utils.MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class TableSinksValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable(): Unit = {
    val util = streamTestUtil()

    val t = util.addTable[(Int, Long, String)]("MyTable", 'id, 'num, 'text)
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "testSink", new TestAppendSink)

    t.groupBy('text)
    .select('text, 'id.count, 'num.sum)
    // must fail because table is not append-only
    .insertInto("testSink")
  }

  @Test(expected = classOf[ValidationException])
  def testSinkTableRegistrationUsingExistedTableName(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String)]("TargetTable", 'id, 'text)

    val fieldNames = Array("a", "b", "c")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.LONG)
    // table name already registered
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "TargetTable",
      new UnsafeMemoryAppendTableSink().configure(fieldNames, fieldTypes))
  }

  @Test(expected = classOf[ValidationException])
  def testRegistrationWithInconsistentFieldNamesAndTypesLength(): Unit = {
    val util = streamTestUtil()

    // inconsistent length of field names and types
    val fieldNames = Array("a", "b", "c")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.LONG)

    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "TargetTable",
      new UnsafeMemoryAppendTableSink().configure(fieldNames, fieldTypes))
  }
}
