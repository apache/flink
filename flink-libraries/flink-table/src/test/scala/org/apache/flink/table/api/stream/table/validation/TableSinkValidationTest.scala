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
import org.apache.flink.table.api.{TableException, Types}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.stream.table.{TestAppendSink, TestUpsertSink}
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class TableSinkValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable(): Unit = {
    val util = streamTestUtil()

    val t = util.addTable[(Int, Long, String)]("MyTable", 'id, 'num, 'text)
    util.tableEnv.registerTableSink("testSink",
      new TestAppendSink().configure(
      Array[String]("text", "id", "sum"),
      Array[TypeInformation[_]](Types.STRING, Types.LONG, Types.LONG)))

    t.groupBy('text)
    .select('text, 'id.count, 'num.sum)
    .insertInto("testSink")
    // must fail because table is not append-only
  }

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable2(): Unit = {
    val util = streamTestUtil()

    util.tableEnv.registerTableSink("testSink",
      new TestAppendSink().configure(
      Array[String]("id", "num", "text"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)))

    val t = util.addTableFromUpsert[(Boolean, (Int, Long, String))]("MyTable", 'a.key, 'b, 'c)

    t.insertInto("testSink")
    // must fail because table is not append-only
  }

  @Test(expected = classOf[TableException])
  def testUpsertSinkOnUpdatingTableWithoutFullKey(): Unit = {
    val util = streamTestUtil()

    val t = util.addTable[(Int, Long, String)]("MyTable", 'id, 'num, 'text)
    util.tableEnv.registerTableSink("testSink",
      new TestUpsertSink(Array("len", "cTrue"), false).configure(
        Array[String]("text", "id", "sum"),
        Array[TypeInformation[_]](Types.INT, Types.LONG, Types.LONG)))

    t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
    .groupBy('len, 'cTrue)
    .select('len, 'id.count, 'num.sum)
    .insertInto("testSink")
    // must fail because table is updating table without full key
  }

  @Test(expected = classOf[TableException])
  def testAppendSinkOnLeftJoin(): Unit = {
    val util = streamTestUtil()

    val t1 = util.addTable[(Int, Long, String)]('a, 'b, 'c)
    val t2 = util.addTable[(Int, Long, Int, String, Long)]('d, 'e, 'f, 'g, 'h)
    util.tableEnv.registerTableSink("testSink",
      new TestAppendSink().configure(
      Array[String]("c", "g"),
      Array[TypeInformation[_]](Types.STRING, Types.STRING)))

    t1.leftOuterJoin(t2, 'a === 'd && 'b === 'h)
      .select('c, 'g)
      .insertInto("testSink")
    // must fail because table is not append-only
  }
}
