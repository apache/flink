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
import org.apache.flink.table.legacy.api.Types
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, TestData}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.utils.DateTimeUtils.toLocalDateTime
import org.apache.flink.types.Row

import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

class UnnestITCase extends BatchTestBase {

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    val data = List(
      row(1, Array(12, 45), Array(Array(12, 45))),
      row(2, Array(41, 5), Array(Array(18), Array(87))),
      row(3, Array(18, 42), Array(Array(1), Array(45)))
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(
        Types.INT,
        Types.PRIMITIVE_ARRAY(Types.INT),
        Types.OBJECT_ARRAY(Types.PRIMITIVE_ARRAY(Types.INT))),
      "a, b, c")

    checkResult(
      "SELECT a, b, s FROM T, UNNEST(T.b) AS A (s)",
      Seq(
        row(1, Array(12, 45), 12),
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
    registerCollection(
      "T",
      data,
      new RowTypeInfo(
        Types.INT,
        Types.PRIMITIVE_ARRAY(Types.INT),
        Types.OBJECT_ARRAY(Types.PRIMITIVE_ARRAY(Types.INT))),
      "a, b, c")

    checkResult(
      "SELECT a, s FROM T, UNNEST(T.c) AS A (s)",
      Seq(
        row(1, Array(12, 45)),
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
    registerCollection(
      "T",
      data,
      new RowTypeInfo(
        Types.INT,
        ObjectArrayTypeInfo.getInfoFor(
          classOf[Array[Row]],
          new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      "SELECT a, b, s, t FROM T, UNNEST(T.b) AS A (s, t) WHERE s > 13",
      Seq(
        row(2, Array(row(13, 41.6), row(14, 45.2136)), 14, 45.2136),
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
    registerCollection(
      "T",
      data,
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
      row(7, "8", "Hello World")
    )

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
    val data = TestData.tupleData3.map { case (i, l, s) => row(i, l, s, toLocalDateTime(i * 1000)) }
    registerCollection(
      "T",
      data,
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
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.OBJECT_ARRAY(Types.STRING)),
      "a, b, c")

    checkResult(
      "SELECT a, s FROM T, UNNEST(T.c) as A (s)",
      Seq(
        row(1, "Hi"),
        row(1, "w"),
        row(2, "Hello"),
        row(2, "k"),
        row(3, "Hello world"),
        row(3, "x"))
    )
  }

  @Test
  def testCrossWithUnnestForMap(): Unit = {
    val data = List(
      row(1, 11L, Map("a" -> "10", "b" -> "11").asJava),
      row(2, 22L, Map("c" -> "20").asJava),
      row(3, 33L, Map("d" -> "30", "e" -> "31").asJava)
    )

    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.MAP(Types.STRING, Types.STRING)),
      "a, b, c")

    checkResult(
      "SELECT a, b, v FROM T CROSS JOIN UNNEST(c) as f (k, v)",
      Seq(
        row(1, "11", "10"),
        row(1, "11", "11"),
        row(2, "22", "20"),
        row(3, "33", "30"),
        row(3, "33", "31"))
    )
  }

  @Test
  def testJoinWithUnnestOfTuple(): Unit = {
    val data = List(
      row(1, Array(row(12, "45.6"), row(2, "45.612"))),
      row(2, Array(row(13, "41.6"), row(1, "45.2136"))),
      row(3, Array(row(18, "42.6"))))
    registerCollection(
      "T",
      data,
      new RowTypeInfo(
        Types.INT,
        ObjectArrayTypeInfo.getInfoFor(
          classOf[Array[Row]],
          new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      "SELECT a, b, x, y " +
        "FROM " +
        "  (SELECT a, b FROM T WHERE a < 3) as tf, " +
        "  UNNEST(tf.b) as A (x, y) " +
        "WHERE x > a",
      Seq(
        row(1, Array(row(12, 45.6), row(2, 45.612)), 12, 45.6),
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
    registerCollection(
      "T",
      data,
      new RowTypeInfo(
        Types.INT,
        ObjectArrayTypeInfo.getInfoFor(
          classOf[Array[Row]],
          new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      "SELECT a, b, A.f0, A.f1 FROM T, UNNEST(T.b) AS A where A.f0 > 13",
      Seq(
        row(2, Array(row(13, 41.6), row(14, 45.2136)), 14, 45.2136),
        row(3, Array(row(18, 42.6)), 18, 42.6))
    )
  }

  @Test
  def testUnnestMapWithDifferentKeyValueType(): Unit = {
    val data = List(
      row(1, Map("a" -> 10, "b" -> 11).asJava),
      row(2, Map("c" -> 20, "d" -> 21).asJava)
    )

    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.MAP(Types.STRING, Types.INT)),
      "a, b")

    checkResult(
      "SELECT a, k, v FROM T, UNNEST(T.b) as A(k, v)",
      Seq(row(1, "a", 10), row(1, "b", 11), row(2, "c", 20), row(2, "d", 21))
    )
  }

  @Test
  def testUnnestWithValuesBatch(): Unit = {
    checkResult("SELECT * FROM UNNEST(ARRAY[1,2,3])", Seq(row(1), row(2), row(3)))
  }

  @Test
  def testUnnestWithValuesBatch2(): Unit = {
    checkResult(
      "SELECT * FROM (VALUES('a')) CROSS JOIN UNNEST(ARRAY[1, 2, 3])",
      Seq(row('a', 1), row('a', 2), row('a', 3)))
  }

  @Test
  def testUnnestWithOrdinalityWithValuesStream(): Unit = {
    checkResult(
      "SELECT * FROM (VALUES('a')) CROSS JOIN UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY",
      Seq(row('a', 1, 1), row('a', 2, 2), row('a', 3, 3))
    )
  }

  @Test
  def testUnnestArrayWithOrdinality(): Unit = {
    val data = List(
      row(1, Array(12, 45)),
      row(2, Array(41, 5)),
      row(3, Array(18, 42))
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.PRIMITIVE_ARRAY(Types.INT)),
      "a, b")

    checkResult(
      """
        |SELECT a, number, ordinality 
        |FROM T CROSS JOIN UNNEST(b) WITH ORDINALITY AS t(number, ordinality)
        |""".stripMargin,
      Seq(row(1, 12, 1), row(1, 45, 2), row(2, 41, 1), row(2, 5, 2), row(3, 18, 1), row(3, 42, 2))
    )
  }

  @Test
  def testUnnestFromTableWithOrdinality(): Unit = {
    val data = List(
      row(1, 1L, Array("Hi", "w")),
      row(2, 2L, Array("Hello", "k")),
      row(3, 2L, Array("Hello world", "x"))
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.OBJECT_ARRAY(Types.STRING)),
      "a, b, c")

    checkResult(
      "SELECT a, s, o FROM T, UNNEST(T.c) WITH ORDINALITY as A (s, o)",
      Seq(
        row(1, "Hi", 1),
        row(1, "w", 2),
        row(2, "Hello", 1),
        row(2, "k", 2),
        row(3, "Hello world", 1),
        row(3, "x", 2))
    )
  }

  @Test
  def testUnnestArrayOfArrayWithOrdinality(): Unit = {
    val data = List(
      row(1, Array(Array(1, 2), Array(3))),
      row(2, Array(Array(4, 5), Array(6, 7))),
      row(3, Array(Array(8)))
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.OBJECT_ARRAY(Types.PRIMITIVE_ARRAY(Types.INT))),
      "id, nested_array")

    checkResult(
      """
        |SELECT id, array_val, array_pos, `element`, element_pos
        |FROM T
        |CROSS JOIN UNNEST(nested_array) WITH ORDINALITY AS A(array_val, array_pos)
        |CROSS JOIN UNNEST(array_val) WITH ORDINALITY AS B(`element`, element_pos)
        |""".stripMargin,
      Seq(
        row(1, Array(1, 2), 1, 1, 1),
        row(1, Array(1, 2), 1, 2, 2),
        row(1, Array(3), 2, 3, 1),
        row(2, Array(4, 5), 1, 4, 1),
        row(2, Array(4, 5), 1, 5, 2),
        row(2, Array(6, 7), 2, 6, 1),
        row(2, Array(6, 7), 2, 7, 2),
        row(3, Array(8), 1, 8, 1)
      )
    )
  }

  @Test
  def testUnnestMultisetWithOrdinality(): Unit = {
    val data = List(
      row(1, 1, "Hi"),
      row(1, 2, "Hello"),
      row(2, 2, "World"),
      row(3, 3, "Hello world")
    )
    registerCollection("T", data, new RowTypeInfo(Types.INT, Types.INT, Types.STRING), "a, b, c")

    checkResult(
      """
        |WITH T1 AS (SELECT a, COLLECT(c) as words FROM T GROUP BY a)
        |SELECT a, word, pos
        |FROM T1 CROSS JOIN UNNEST(words) WITH ORDINALITY AS A(word, pos)
        |""".stripMargin,
      Seq(row(1, "Hi", 1), row(1, "Hello", 2), row(2, "World", 1), row(3, "Hello world", 1))
    )
  }

  @Test
  def testUnnestMapWithOrdinality(): Unit = {
    val data = List(
      row(1, Map("a" -> "10", "b" -> "11").asJava),
      row(2, Map("c" -> "20", "d" -> "21").asJava)
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.MAP(Types.STRING, Types.STRING)),
      "id, map_data")

    checkResult(
      """
        |SELECT id, k, v
        |FROM T CROSS JOIN UNNEST(map_data) WITH ORDINALITY AS f(k, v, pos)
        |""".stripMargin,
      Seq(row(1, "a", "10"), row(1, "b", "11"), row(2, "c", "20"), row(2, "d", "21"))
    )
  }

  @Test
  def testUnnestForMapOfRowsWithOrdinality(): Unit = {
    val data = List(
      row(
        1,
        Map(
          Row.of("a", "a") -> Row.of(10: Integer),
          Row.of("b", "b") -> Row.of(11: Integer)
        ).asJava),
      row(
        2,
        Map(
          Row.of("c", "c") -> Row.of(20: Integer)
        ).asJava),
      row(
        3,
        Map(
          Row.of("d", "d") -> Row.of(30: Integer),
          Row.of("e", "e") -> Row.of(31: Integer)
        ).asJava)
    )

    registerCollection(
      "T",
      data,
      new RowTypeInfo(
        Types.INT,
        Types.MAP(Types.ROW(Types.STRING, Types.STRING), Types.ROW(Types.INT()))),
      "a, b")

    checkResult(
      "SELECT a, k, v, o FROM T CROSS JOIN UNNEST(b) WITH ORDINALITY as f (k, v, o)",
      Seq(
        row(1, row("a", "a"), row(10), 1),
        row(1, row("b", "b"), row(11), 2),
        row(2, row("c", "c"), row(20), 1),
        row(3, row("d", "d"), row(30), 1),
        row(3, row("e", "e"), row(31), 2))
    )
  }

  @Test
  def testUnnestWithOrdinalityForChainOfArraysAndMaps(): Unit = {
    val data = List(
      row(1, Array("a", "b"), Map("x" -> "10", "y" -> "20").asJava),
      row(2, Array("c", "d"), Map("z" -> "30", "w" -> "40").asJava)
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(
        Types.INT,
        Types.OBJECT_ARRAY(Types.STRING),
        Types.MAP(Types.STRING, Types.STRING)),
      "id, array_data, map_data")

    checkResult(
      """
        |SELECT id, array_val, array_pos, map_key, map_val, map_pos
        |FROM T
        |CROSS JOIN UNNEST(array_data) WITH ORDINALITY AS A(array_val, array_pos)
        |CROSS JOIN UNNEST(map_data) WITH ORDINALITY AS B(map_key, map_val, map_pos)
        |""".stripMargin,
      Seq(
        row(1, "a", 1, "x", "10", 1),
        row(1, "a", 1, "y", "20", 2),
        row(1, "b", 2, "x", "10", 1),
        row(1, "b", 2, "y", "20", 2),
        row(2, "c", 1, "z", "30", 1),
        row(2, "c", 1, "w", "40", 2),
        row(2, "d", 2, "z", "30", 1),
        row(2, "d", 2, "w", "40", 2)
      )
    )
  }

  @Test
  def testUnnestWithOrdinalityForEmptyArray(): Unit = {
    val data = List(row(1, Array[Int]()))
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.PRIMITIVE_ARRAY(Types.INT)),
      "a, b")

    checkResult(
      """
        |SELECT a, number, ordinality
        |FROM T CROSS JOIN UNNEST(b) WITH ORDINALITY AS t(number, ordinality)
        |""".stripMargin,
      Seq()
    )
  }

  @Test
  def testUnnestArrayOfRowsFromTableWithOrdinality(): Unit = {
    val data = List(
      row(2, Array(row(20, "41.6"), row(14, "45.2136"))),
      row(3, Array(row(18, "42.6")))
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.OBJECT_ARRAY(new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      "SELECT a, b, s, t, o FROM T, UNNEST(T.b) WITH ORDINALITY AS A(s, t, o)",
      Seq(
        row(2, Array(row(20, "41.6"), row(14, "45.2136")), 20, "41.6", 1),
        row(2, Array(row(20, "41.6"), row(14, "45.2136")), 14, "45.2136", 2),
        row(3, Array(row(18, "42.6")), 18, "42.6", 1)
      )
    )
  }

  @Test
  def testUnnestArrayOfRowsWithNestedFilterWithOrdinality(): Unit = {
    val data = List(
      row(1, Array(row(12, "45.6"), row(12, "45.612"))),
      row(2, Array(row(13, "41.6"), row(14, "45.2136"))),
      row(3, Array(row(18, "42.6")))
    )
    registerCollection(
      "T",
      data,
      new RowTypeInfo(Types.INT, Types.OBJECT_ARRAY(new RowTypeInfo(Types.INT, Types.STRING))),
      "a, b")

    checkResult(
      """
        |SELECT * FROM (
        |   SELECT a, b1, b2, ord FROM
        |       (SELECT a, b FROM T) T
        |       CROSS JOIN
        |       UNNEST(T.b) WITH ORDINALITY as S(b1, b2, ord)
        |       WHERE S.b1 >= 12
        |   ) tmp
        |WHERE b2 <> '42.6' AND ord <> 2
        |""".stripMargin,
      Seq(row(1, 12, "45.6", 1), row(2, 13, "41.6", 1))
    )
  }
}
