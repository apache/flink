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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.utils.{TableTestBase, TestFilterableTableSourceWithoutExplainSourceOverride, TestProjectableTableSourceWithoutExplainSourceOverride}

import org.hamcrest.Matchers
import org.junit.Test

class TableSourceValidationTest extends TableTestBase {

  @Test
  def testPushProjectTableSourceWithoutExplainSource(): Unit = {
    expectedException.expectCause(Matchers.isA(classOf[TableException]))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    val util = streamTestUtil()
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "T",
      new TestProjectableTableSourceWithoutExplainSourceOverride(
        tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('name, 'val, 'id)

    // must fail since pushed projection is not explained in source
    util.explain(t)
  }

  @Test
  def testPushFilterableTableSourceWithoutExplainSource(): Unit = {
    expectedException.expectCause(Matchers.isA(classOf[TableException]))

    val tableSource = TestFilterableTableSourceWithoutExplainSourceOverride()
    val util = batchTestUtil()

    util.tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal("T", tableSource)

    val t = util.tableEnv
      .scan("T")
      .select('price, 'id, 'amount)
      .where($"price" * 2 < 32)

    // must fail since pushed filter is not explained in source
    util.explain(t)
  }
}
