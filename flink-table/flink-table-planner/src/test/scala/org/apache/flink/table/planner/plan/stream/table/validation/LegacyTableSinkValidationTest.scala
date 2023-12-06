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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.runtime.utils.{TestData, TestingAppendSink, TestingUpsertTableSink}
import org.apache.flink.table.planner.utils.{MemoryTableSourceSinkUtil, TableTestBase, TableTestUtil}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class LegacyTableSinkValidationTest extends TableTestBase {

  @Test
  def testAppendSinkOnUpdatingTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env.fromCollection(TestData.smallTupleData3).toTable(tEnv, 'a, 'b, 'c)

    // must fail because table is not append-only
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          t.groupBy('text)
            .select('text, 'id.count, 'num.sum)
            .toDataStream
            .addSink(new TestingAppendSink)
          env.execute()
        })
  }

  @Test
  def testUpsertSinkOnUpdatingTableWithoutFullKey(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val t = env
      .fromCollection(TestData.tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    val sink = new TestingUpsertTableSink(Array(0, 1))

    val result = t
      .select('id, 'num, 'text.charLength().as('len), ('id > 0).as('cTrue))
      .groupBy('len, 'cTrue)
      .select('len, 'id.count, 'num.sum)
    val schema = result.getSchema
    sink.configure(schema.getFieldNames, schema.getFieldTypes)

    // must fail because table is updating table without full key
    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(
        () => {
          tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("testSink", sink)
          result.executeInsert("testSink")
        })
  }

  @Test
  def testAppendSinkOnLeftJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val ds1 = env.fromCollection(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(TestData.tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    // must fail because table is not append-only
    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(
        () => {
          ds1
            .leftOuterJoin(ds2, 'a === 'd && 'b === 'h)
            .select('c, 'g)
            .toDataStream
            .addSink(new TestingAppendSink)
          env.execute()
        })
  }

  @Test
  def testValidateSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val sourceTable = env.fromCollection(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("source", sourceTable)
    val resultTable = tEnv.sqlQuery("select a, b, c, b as d from source")

    val sinkSchema = TableSchema
      .builder()
      .field("a", DataTypes.INT())
      .field("b", DataTypes.BIGINT())
      .field("c", DataTypes.STRING())
      .field("d", DataTypes.INT())
      .build()

    MemoryTableSourceSinkUtil.createDataTypeOutputFormatTable(tEnv, sinkSchema, "testSink")
    // must fail because query result table schema is different with sink table schema
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => resultTable.executeInsert("testSink").await())
      .withMessageContaining(
        "Column types of query result and sink for " +
          "'default_catalog.default_database.testSink' do not match.\n" +
          "Cause: Incompatible types for sink column 'd' at position 3.\n\n" +
          "Query schema: [a: INT, b: BIGINT, c: STRING, d: BIGINT]\n" +
          "Sink schema:  [a: INT, b: BIGINT, c: STRING, d: INT]")
  }

}
