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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, RowTypeInfo}
import org.apache.flink.table.api.Types
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, TestData}
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils.unixTimestampToLocalDateTime
import org.apache.flink.types.Row

import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.Seq

class UnnestITCase extends BatchTestBase {

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    val data = List(
      row(1, Array(12, 45), Array(Array(12, 45))),
      row(2, Array(41, 5), Array(Array(18), Array(87))),
      row(3, Array(18, 42), Array(Array(1), Array(45)))
    )
    registerCollection("T", data,
      new RowTypeInfo(
        Types.INT,
        Types.PRIMITIVE_ARRAY(Types.INT),
        Types.OBJECT_ARRAY(Types.PRIMITIVE_ARRAY(Types.INT))),
      "a, b, c")

    checkResult(
      "SELECT a, b, s FROM T, UNNEST(T.b) AS A (s)",
      Seq(row(1, Array(12, 45), 12),
        row(1, Array(12, 45), 45),
        row(2, Array(41, 5), 41),
        row(2, Array(41, 5), 5),
        row(3, Array(18, 42), 18),
        row(3, Array(18, 42), 42))
    )
  }

  @Test
  def testUnnestArrayOfArrayFromTable(): Unit = {
    val data = List(
      row(1, Array(12, 45), Array(Array(12, 45))),
      row(2, Array(41, 5), Array(Array(18), Array(87))),
      row(3, Array(18, 42), Array(Array(1), Array(45)))
    )
    registerCollection("T", data,
      new RowTypeInfo(
        Types.INT,
        Types.PRIMITIVE_ARRAY(Types.INT),
        Types.OBJECT_ARRAY(Types.PRIMITIVE_ARRAY(Types.INT))),
      "a, b, c")

    checkResult(
      "SELECT a, s FROM T, UNNEST(T.c) AS A (s)",
      Seq(row(1, Array(12, 45)),
        row(2, Array(18)),
        row(2, Array(87)),
        row(3, Array(1)),
        row(3, Array(45)))
    )
  }

  @Test
  def testUnnestObjectArrayFromTableWithFilter(): Unit = {
    val data = List(
      row(1, Array(row(12, "45.6"), row(12, "45.612"))),
      row(2, Array(row(13, "41.6"), row(14, "45.2136"))),
      row(3, Array(row(18, "42.6")))
    )
    registerCollection("T", data,
      new RowTypeInfo(
        Types.INT,
        ObjectArrayTypeInfo.getInfoFor(classOf[Array[Row]],
          new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      "SELECT a, b, s, t FROM T, UNNEST(T.b) AS A (s, t) WHERE s > 13",
      Seq(row(2, Array(row(13, 41.6), row(14, 45.2136)), 14, 45.2136),
        row(3, Array(row(18, 42.6)), 18, 42.6))
    )
  }

  @Test
  def testUnnestMultiSetFromCollectResult(): Unit = {
    val data = List(
      row(1, 1, row(12, "45.6")),
      row(2, 2, row(12, "45.612")),
      row(3, 2, row(13, "41.6")),
      row(4, 3, row(14, "45.2136")),
      row(5, 3, row(18, "42.6")))
    registerCollection("T", data,
      new RowTypeInfo(Types.INT, Types.INT, new RowTypeInfo(Types.INT, Types.STRING)),
      "a, b, c")

    checkResult(
      """
        |WITH T1 AS (SELECT b, COLLECT(c) as `set` FROM T GROUP BY b)
        |SELECT b, id, point FROM T1, UNNEST(T1.`set`) AS A(id, point) WHERE b < 3
      """.stripMargin,
      Seq(row(1, 12, "45.6"), row(2, 12, "45.612"), row(2, 13, "41.6"))
    )
  }

  @Test
  def testLeftUnnestMultiSetFromCollectResult(): Unit = {
    val data = List(
      row(1, "1", "Hello"),
      row(1, "2", "Hello2"),
      row(2, "2", "Hello"),
      row(3, null.asInstanceOf[String], "Hello"),
      row(4, "4", "Hello"),
      row(5, "5", "Hello"),
      row(5, null.asInstanceOf[String], "Hello"),
      row(6, "6", "Hello"),
      row(7, "7", "Hello World"),
      row(7, "8", "Hello World"))

    registerCollection("T", data, new RowTypeInfo(Types.INT, Types.STRING, Types.STRING), "a, b, c")

    checkResult(
      """
        |WITH T1 AS (SELECT a, COLLECT(b) as `set` FROM T GROUP BY a)
        |SELECT a, s FROM T1 LEFT JOIN UNNEST(T1.`set`) AS A(s) ON TRUE WHERE a < 5
      """.stripMargin,
      Seq(row(1, "1"), row(1, "2"), row(2, "2"), row(3, null), row(4, "4"))
    )
  }

  @Test
  def testTumbleWindowAggregateWithCollectUnnest(): Unit = {
    val data = TestData.tupleData3.map {
      case (i, l, s) => row(i, l, s, unixTimestampToLocalDateTime(i * 1000))
    }
    registerCollection("T", data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.STRING, Types.LOCAL_DATE_TIME),
      "a, b, c, ts")

    checkResult(
      """
        |WITH T1 AS (SELECT b, COLLECT(b) as `set`
        |    FROM T
        |    GROUP BY b, TUMBLE(ts, INTERVAL '3' SECOND)
        |)
        |SELECT b, s FROM T1, UNNEST(T1.`set`) AS A(s) where b < 3
      """.stripMargin,
      Seq(row(1, 1), row(2, 2), row(2, 2))
    )
  }

  @Test
  def testCrossWithUnnest(): Unit = {
    val data = List(
      row(1, 1L, Array("Hi", "w")),
      row(2, 2L, Array("Hello", "k")),
      row(3, 2L, Array("Hello world", "x"))
    )
    registerCollection("T", data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.OBJECT_ARRAY(Types.STRING)),
      "a, b, c")

    checkResult(
      "SELECT a, s FROM T, UNNEST(T.c) as A (s)",
      Seq(row(1, "Hi"), row(1, "w"), row(2, "Hello"),
        row(2, "k"), row(3, "Hello world"), row(3, "x"))
    )
  }

  @Test
  def testCrossWithUnnestForMap(): Unit = {
    val data = List(
      row(1, 11L, Map("a" -> "10", "b" -> "11").asJava),
      row(2, 22L, Map("c" -> "20").asJava),
      row(3, 33L, Map("d" -> "30", "e" -> "31").asJava)
    )

    registerCollection("T", data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.MAP(Types.STRING, Types.STRING)),
      "a, b, c")

    checkResult(
      "SELECT a, b, v FROM T CROSS JOIN UNNEST(c) as f (k, v)",
      Seq(row(1, "11", "10"), row(1, "11", "11"), row(2, "22", "20"),
        row(3, "33", "30"), row(3, "33", "31"))
    )
  }

  @Test
  def testJoinWithUnnestOfTuple(): Unit = {
    val data = List(
      row(1, Array(row(12, "45.6"), row(2, "45.612"))),
      row(2, Array(row(13, "41.6"), row(1, "45.2136"))),
      row(3, Array(row(18, "42.6"))))
    registerCollection("T", data,
      new RowTypeInfo(Types.INT,
        ObjectArrayTypeInfo.getInfoFor(classOf[Array[Row]],
          new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      "SELECT a, b, x, y " +
          "FROM " +
          "  (SELECT a, b FROM T WHERE a < 3) as tf, " +
          "  UNNEST(tf.b) as A (x, y) " +
          "WHERE x > a",
      Seq(row(1, Array(row(12, 45.6), row(2, 45.612)), 12, 45.6),
        row(1, Array(row(12, 45.6), row(2, 45.612)), 2, 45.612),
        row(2, Array(row(13, 41.6), row(1, 45.2136)), 13, 41.6))
    )
  }

  @Test
  def testUnnestObjectArrayWithoutAlias(): Unit = {
    val data = List(
      row(1, Array(row(12, "45.6"), row(12, "45.612"))),
      row(2, Array(row(13, "41.6"), row(14, "45.2136"))),
      row(3, Array(row(18, "42.6")))
    )
    registerCollection("T", data,
      new RowTypeInfo(
        Types.INT,
        ObjectArrayTypeInfo.getInfoFor(classOf[Array[Row]],
          new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      "SELECT a, b, A.f0, A.f1 FROM T, UNNEST(T.b) AS A where A.f0 > 13",
      Seq(row(2, Array(row(13, 41.6), row(14, 45.2136)), 14, 45.2136),
        row(3, Array(row(18, 42.6)), 18, 42.6))
    )
  }

  @Test
  def testUnnestMapWithDifferentKeyValueType(): Unit = {
    val data = List(
      row(1, Map("a" -> 10, "b" -> 11).asJava),
      row(2, Map("c" -> 20, "d" -> 21).asJava)
    )

    registerCollection("T", data,
      new RowTypeInfo(Types.INT, Types.MAP(Types.STRING, Types.INT)),
      "a, b")

    checkResult(
      "SELECT a, k, v FROM T, UNNEST(T.b) as A(k, v)",
      Seq(row(1, "a", 10), row(1, "b", 11), row(2, "c", 20),
        row(2, "d", 21))
    )
  }

}
