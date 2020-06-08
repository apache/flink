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

package org.apache.flink.table.planner.plan.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.runtime.utils.{TableEnvUtil, TestData, TestingAppendSink, TestingUpsertTableSink}
import org.apache.flink.table.planner.utils.{MemoryTableSourceSinkUtil, TableTestBase, TableTestUtil}
import org.apache.flink.types.Row

import org.junit.Test

class LegacyTableSinkValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testAppendSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(TestData.smallTupleData3).toTable(tEnv, 'a, 'b, 'c)

    t.groupBy('text)
    .select('text, 'id.count, 'num.sum)
    .toAppendStream[Row].addSink(new TestingAppendSink)

    // must fail because table is not append-only
    env.execute()
  }

  @Test(expected = classOf[TableException])
  def testUpsertSinkOnUpdatingTableWithoutFullKey(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(TestData.tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val sink = new TestingUpsertTableSink(Array(0, 1))

    val result = t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
    .groupBy('len, 'cTrue)
    .select('len, 'id.count, 'num.sum)
    val schema = result.getSchema
    sink.configure(schema.getFieldNames, schema.getFieldTypes)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("testSink", sink)
    tEnv.insertInto("testSink", result)
    // must fail because table is updating table without full key
    env.execute()
  }

  @Test(expected = classOf[TableException])
  def testAppendSinkOnLeftJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val ds1 = env.fromCollection(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 'h)
      .select('c, 'g)
      .toAppendStream[Row].addSink(new TestingAppendSink)

    // must fail because table is not append-only
    env.execute()
  }

  @Test
  def testValidateSink(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Field types of query result and registered TableSink default_catalog." +
      "default_database.testSink do not match.\n" +
      "Query schema: [a: INT, b: BIGINT, c: VARCHAR(2147483647), d: BIGINT]\n" +
      "Sink schema: [a: INT, b: BIGINT, c: VARCHAR(2147483647), d: INT]")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val sourceTable = env.fromCollection(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("source", sourceTable)
    val resultTable = tEnv.sqlQuery("select a, b, c, b as d from source")

    val sinkSchema = TableSchema.builder()
      .field("a", DataTypes.INT())
      .field("b", DataTypes.BIGINT())
      .field("c", DataTypes.STRING())
      .field("d", DataTypes.INT())
      .build()

    MemoryTableSourceSinkUtil.createDataTypeOutputFormatTable(
      tEnv, sinkSchema, "testSink")
    // must fail because query result table schema is different with sink table schema
    TableEnvUtil.execInsertTableAndWaitResult(resultTable, "testSink")
  }

}
