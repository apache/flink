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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.types.variant.{BinaryVariantBuilder, Variant}
import org.apache.flink.util.CollectionUtil

import org.assertj.core.api.Assertions.assertThatList
import org.junit.jupiter.api.Test

class VariantScalaITCase extends StreamingTestBase {

  @Test
  def testVariant(): Unit = {

    val sqlQuery =
      "SELECT PARSE_JSON(s), PARSE_JSON('\"hello\"') FROM (VALUES ('{\"k\":\"v1\"}'), ('{\"k\":\"v2\"}')) T(s)"

    val builder = new BinaryVariantBuilder()
    assertThatList(CollectionUtil.iteratorToList(tEnv.sqlQuery(sqlQuery).execute().collect()))
      .containsExactlyInAnyOrder(
        rowOf(builder.`object`().add("k", builder.of("v1")).build(), builder.of("hello")),
        rowOf(builder.`object`().add("k", builder.of("v2")).build(), builder.of("hello"))
      )
  }

  @Test
  def testVariantUDF(): Unit = {
    tEnv.createTemporaryFunction("my_udf", classOf[MyUdf])
    val sqlQuery =
      "SELECT my_udf(PARSE_JSON(s)) FROM (VALUES ('{\"k\":1}'), ('{\"k\":2}')) T(s)"

    assertThatList(CollectionUtil.iteratorToList(tEnv.sqlQuery(sqlQuery).execute().collect()))
      .containsExactlyInAnyOrder(
        rowOf(1),
        rowOf(2)
      )
  }

  @Test
  def testVariantAgg(): Unit = {
    tEnv.createTemporaryFunction("my_udf", classOf[MyAggFunc])

    val builder = new BinaryVariantBuilder()
    val objectBuilder = builder.`object`()
    for (i <- 1 to 1000) {
      objectBuilder.add("k" + i, builder.of("a" * i))
    }
    objectBuilder.add("k", builder.of(3))
    val longJson = objectBuilder.build().toJson

    val sqlQuery =
      raw"""SELECT k, my_udf(TRY_PARSE_JSON(s))
           |  FROM (VALUES
           |    (1, '{"k":1}'),
           |    (1, '{"k":2}'),
           |    (1, '{'),
           |    (1, '$longJson'))
           |  AS T(k, s)
           |GROUP BY k""".stripMargin

    assertThatList(CollectionUtil.iteratorToList(tEnv.sqlQuery(sqlQuery).execute().collect()))
      .containsExactlyInAnyOrder(
        rowOf(1, 1L),
        rowOf(RowKind.UPDATE_BEFORE, 1, 1L),
        rowOf(RowKind.UPDATE_AFTER, 1, 3L),
        rowOf(RowKind.UPDATE_BEFORE, 1, 3L),
        rowOf(RowKind.UPDATE_AFTER, 1, 6L)
      )
  }

  @Test
  def testVariantAsGroupKey(): Unit = {
    val builder = new BinaryVariantBuilder()

    val sqlQuery =
      raw"""SELECT TRY_PARSE_JSON(s), SUM(k)
           |  FROM (VALUES
           |    (1, '{"k":1}'),
           |    (2, '{"k":1}'),
           |    (1, '{'),
           |    (1, '}'))
           |  AS T(k, s)
           |GROUP BY TRY_PARSE_JSON(s)""".stripMargin

    assertThatList(CollectionUtil.iteratorToList(tEnv.sqlQuery(sqlQuery).execute().collect()))
      .containsExactlyInAnyOrder(
        rowOf(RowKind.INSERT, null, 1),
        rowOf(RowKind.UPDATE_BEFORE, null, 1),
        rowOf(RowKind.UPDATE_AFTER, null, 2),
        rowOf(builder.`object`().add("k", builder.of(1.toByte)).build(), 1),
        rowOf(RowKind.UPDATE_BEFORE, builder.`object`().add("k", builder.of(1.toByte)).build(), 1),
        rowOf(RowKind.UPDATE_AFTER, builder.`object`().add("k", builder.of(1.toByte)).build(), 3)
      )
  }

  @Test
  def testVariantBuiltInAgg(): Unit = {
    val builder = new BinaryVariantBuilder()

    var sqlQuery =
      raw"""SELECT k, COUNT(TRY_PARSE_JSON(s)), COUNT(distinct TRY_PARSE_JSON(s))
           |  FROM (VALUES
           |    (1, '{"k":1}'),
           |    (1, '{'),
           |    (1, '{"k":1}'))
           |  AS T(k, s)
           |GROUP BY k""".stripMargin

    assertThatList(CollectionUtil.iteratorToList(tEnv.sqlQuery(sqlQuery).execute().collect()))
      .containsExactlyInAnyOrder(
        rowOf(1, 1L, 1L),
        rowOf(RowKind.UPDATE_BEFORE, 1, 1L, 1L),
        rowOf(RowKind.UPDATE_AFTER, 1, 2L, 1L)
      )

    sqlQuery = raw"""SELECT k, FIRST_VALUE(TRY_PARSE_JSON(s)), LAST_VALUE(TRY_PARSE_JSON(s))
                    |  FROM (VALUES
                    |    (1, '{"k":1}'),
                    |    (1, '{'),
                    |    (1, '{"k":2}'))
                    |  AS T(k, s)
                    |GROUP BY k""".stripMargin

    assertThatList(CollectionUtil.iteratorToList(tEnv.sqlQuery(sqlQuery).execute().collect()))
      .containsExactlyInAnyOrder(
        rowOf(
          1,
          builder.`object`().add("k", builder.of(1.toByte)).build(),
          builder.`object`().add("k", builder.of(1.toByte)).build()),
        rowOf(
          RowKind.UPDATE_BEFORE,
          1,
          builder.`object`().add("k", builder.of(1.toByte)).build(),
          builder.`object`().add("k", builder.of(1.toByte)).build()),
        rowOf(
          RowKind.UPDATE_AFTER,
          1,
          builder.`object`().add("k", builder.of(1.toByte)).build(),
          builder.`object`().add("k", builder.of(2.toByte)).build())
      )
  }

  def rowOf(kind: RowKind, args: Any*): Row = {
    val row = new Row(kind, args.length)
    (0 until args.length).foreach(i => row.setField(i, args(i)))
    row
  }

}

class MyUdf extends ScalarFunction {
  def MyUdf(): Unit = {}

  def eval(s: Variant): Integer = {
    s.getField("k").getByte()
  }
}

class MyAggFunc extends AggregateFunction[JLong, Array[JLong]] {
  override def getValue(accumulator: Array[JLong]): JLong = {
    accumulator(0)
  }

  override def createAccumulator(): Array[JLong] = {
    val longs = new Array[JLong](1)
    longs(0) = 0
    longs
  }

  def accumulate(accumulator: Array[JLong], v: Variant): Unit = {
    if (v == null) {
      return
    }
    val variant = v.getField("k")
    if (variant == null) {
      return
    }
    accumulator(0) += variant.getByte
  }
}
