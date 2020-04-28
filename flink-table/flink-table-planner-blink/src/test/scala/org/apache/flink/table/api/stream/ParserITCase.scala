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

package org.apache.flink.table.api.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala._
import org.apache.flink.table.api.config.ParserConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendBaseRowSink}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.{IntType, VarCharType}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

class ParserITCase extends StreamingTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    tEnv.getConfig.getConfiguration.setBoolean(
      ParserConfigOptions.TABLE_PARSER_CASE_SENSITIVE_ENABLED, false
    )
  }

  @Test
  def testNoneCaseSensitiveColumn(): Unit = {
    val sqlQuery = "SELECT a, b, c FROM table1 WHERE c < 3"

    val data = List(
      Row.of("Hello", "Worlds", Int.box(1)),
      Row.of("Hello", "Hiden", Int.box(5)),
      Row.of("Hello again", "Worlds", Int.box(2)))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      Types.STRING,
      Types.STRING,
      Types.INT)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'A, 'B, 'C)
    tEnv.registerTable("table1", t)

    val outputType = new BaseRowTypeInfo(
      new VarCharType(VarCharType.MAX_LENGTH),
      new VarCharType(VarCharType.MAX_LENGTH),
      new IntType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[BaseRow]
    val sink = new TestingAppendBaseRowSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("0|Hello,Worlds,1","0|Hello again,Worlds,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
