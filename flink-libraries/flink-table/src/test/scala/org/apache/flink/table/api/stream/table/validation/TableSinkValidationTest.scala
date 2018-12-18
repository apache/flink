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

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.stream.table.{TestAppendSink, TestUpsertSink}
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class TableSinkValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testAppendSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'id, 'num, 'text)
    tEnv.registerTableSink("testSink", new TestAppendSink)

    t.groupBy('text)
    .select('text, 'id.count, 'num.sum)
    .insertInto("testSink")

    // must fail because table is not append-only
    env.execute()
  }

  @Test(expected = classOf[TableException])
  def testUpsertSinkOnUpdatingTableWithoutFullKey(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)
    tEnv.registerTableSink("testSink", new TestUpsertSink(Array("len", "cTrue"), false))

    t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
    .groupBy('len, 'cTrue)
    .select('len, 'id.count, 'num.sum)
    .insertInto("testSink")

    // must fail because table is updating table without full key
    env.execute()
  }

  @Test(expected = classOf[TableException])
  def testAppendSinkOnLeftJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTableSink("testSink", new TestAppendSink)

    ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 'h)
      .select('c, 'g)
      .insertInto("testSink")

    // must fail because table is not append-only
    env.execute()
  }
}
