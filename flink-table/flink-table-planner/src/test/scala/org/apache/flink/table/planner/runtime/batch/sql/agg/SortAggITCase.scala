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
package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.config.ExecutionConfigOptions.{TABLE_EXEC_DISABLED_OPERATORS, TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.planner.{JInt, JLong}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMergeAndReset
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils.{MyPojo, MyToPojoFunc}
import org.apache.flink.table.planner.utils.{CountAccumulator, CountAggFunction, IntSumAggFunction}

import org.junit.jupiter.api.Test

import java.lang
import java.lang.{Iterable => JIterable}

import scala.annotation.varargs
import scala.collection.JavaConverters._

/** AggregateITCase using SortAgg Operator. */
class SortAggITCase extends AggregateITCaseBase("SortAggregate") {
  override def prepareAggOp(): Unit = {
    tEnv.getConfig.set(TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")

    registerFunction("countFun", new CountAggFunction())
    registerFunction("intSumFun", new IntSumAggFunction())
    registerTemporarySystemFunction("weightedAvg", classOf[WeightedAvgWithMergeAndReset])

    registerFunction("myPrimitiveArrayUdaf", new MyPrimitiveArrayUdaf())
    registerFunction("myObjectArrayUdaf", new MyObjectArrayUdaf())
    registerFunction("myNestedLongArrayUdaf", new MyNestedLongArrayUdaf())
    registerTemporarySystemFunction("myNestedStringArrayUdaf", classOf[MyNestedStringArrayUdaf])

    registerFunction("myPrimitiveMapUdaf", new MyPrimitiveMapUdaf())
    registerFunction("myObjectMapUdaf", new MyObjectMapUdaf())
    registerTemporarySystemFunction("myNestedMapUdaf", classOf[MyNestedMapUdf])
  }

  @Test
  def testBigDataSimpleArrayUDAF(): Unit = {
    tEnv.getConfig.set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(1))
    registerFunction("simplePrimitiveArrayUdaf", new SimplePrimitiveArrayUdaf())
    registerRange("RangeT", 1000000)
    env.setParallelism(1)
    checkResult("SELECT simplePrimitiveArrayUdaf(id) FROM RangeT", Seq(row(499999500000L)))
  }

  @Test
  def testMultiSetAggBufferGroupBy(): Unit = {
    checkResult(
      "SELECT collect(b) FROM Table3",
      Seq(
        row(collection.immutable.SortedMap(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4, 5 -> 5, 6 -> 6).asJava)
      )
    )
  }

  @Test
  def testUDAGGWithoutGroupby(): Unit = {
    checkResult(
      "SELECT countFun(c) FROM Table3",
      Seq(
        row(21)
      )
    )
  }

  @Test
  def testUDAGGWithGroupby(): Unit = {
    checkResult(
      "SELECT countFun(a), count(a), b FROM Table3 GROUP BY b",
      Seq(
        row(1, 1, 1),
        row(2, 2, 2),
        row(3, 3, 3),
        row(4, 4, 4),
        row(5, 5, 5),
        row(6, 6, 6)
      )
    )
  }

  @Test
  def testUDAGGNullGroupKeyAggregation(): Unit = {
    checkResult(
      "SELECT intSumFun(d), d, count(d) FROM NullTable5 GROUP BY d",
      Seq(
        row(1, 1, 1),
        row(25, 5, 5),
        row(null, null, 0),
        row(16, 4, 4),
        row(4, 2, 2),
        row(9, 3, 3)
      ))
  }

  @Test
  def testComplexUDAGGWithGroupBy(): Unit = {
    checkResult(
      "SELECT b, weightedAvg(b, a) FROM Table3 GROUP BY b",
      Seq(
        row(1, 1),
        row(2, 2),
        row(3, 3),
        row(4, 4),
        row(5, 5),
        row(6, 6)
      )
    )
  }

  // NOTE: Spark has agg functions collect_list(), collect_set().
  //       instead, we'll test LISTAGG() here
  @Test
  def testListAgg(): Unit = {
    checkResult(
      "SELECT LISTAGG(c, '-'), LISTAGG(c) FROM SmallTable3",
      Seq(
        row("Hi-Hello-Hello world", "Hi,Hello,Hello world")
      )
    )

    // EmptyTable5
    checkResult(
      "SELECT LISTAGG(g, '-'), LISTAGG(g) FROM EmptyTable5",
      Seq(
        row(null, null)
      )
    )

    checkResult(
      "SELECT LISTAGG(c, '-'), LISTAGG(c) FROM AllNullTable3",
      Seq(
        row(null, null)
      )
    )
  }

  @Test
  def testPojoField(): Unit = {
    val data = Seq(row(1, new MyPojo(5, 105)), row(1, new MyPojo(6, 11)), row(1, new MyPojo(7, 12)))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(Types.INT, TypeExtractor.createTypeInfo(classOf[MyPojo])),
      "a, b")

    registerFunction("pojoFunc", new MyPojoAggFunction)
    checkResult("SELECT pojoFunc(b) FROM MyTable group by a", Seq(row(row(128, 128))))
  }

  @Test
  def testVarArgs(): Unit = {
    val data = Seq(row(1, 1L, "5", "3"), row(1, 22L, "15", "13"), row(3, 33L, "25", "23"))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(Types.INT, Types.LONG, Types.STRING, Types.STRING),
      "id, s, s1, s2")
    val func = new VarArgsAggFunction
    registerFunction("func", func)

    // no group
    checkResult("SELECT func(s, s1, s2) FROM MyTable", Seq(row(140)))

    // with group
    checkResult("SELECT id, func(s, s1, s2) FROM MyTable group by id", Seq(row(1, 59), row(3, 81)))
  }

  @Test
  def testMaxString(): Unit = {
    checkResult(
      "SELECT max(c) FROM Table3 GROUP BY b",
      Seq(
        row("Comment#15"),
        row("Comment#4"),
        row("Comment#9"),
        row("Hello world"),
        row("Hi"),
        row("Luke Skywalker")
      )
    )

    checkResult(
      "SELECT max(c) FROM Table3",
      Seq(
        row("Luke Skywalker")
      )
    )
  }

  @Test
  def testMaxStringAllNull(): Unit = {
    checkResult(
      "SELECT max(c) FROM AllNullTable3 GROUP BY b",
      Seq(
        row(null)
      )
    )

    checkResult(
      "SELECT max(c) FROM AllNullTable3",
      Seq(
        row(null)
      )
    )
  }

  @Test
  def testFirstValueOnString(): Unit = {
    checkResult(
      "SELECT first_value(c) over () FROM Table3",
      Seq(
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi"),
        row("Hi")
      )
    )
  }

  @Test
  def testArrayUdaf(): Unit = {
    tEnv.getConfig.set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(1))
    env.setParallelism(1)
    checkResult(
      "SELECT myPrimitiveArrayUdaf(a, b) FROM Table3",
      Seq(row(Array(231, 91)))
    )
    checkResult(
      "SELECT myObjectArrayUdaf(c) FROM Table3",
      Seq(row(Array("HHHHILCCCCCCCCCCCCCCC", "iod?.r123456789012345")))
    )
    checkResult(
      "SELECT myNestedLongArrayUdaf(a, b)[2] FROM Table3",
      Seq(row(Array(91, 231)))
    )
    checkResult(
      "SELECT myNestedStringArrayUdaf(c)[2] FROM Table3",
      Seq(row(Array("iod?.r123456789012345", "HHHHILCCCCCCCCCCCCCCC")))
    )
  }

  @Test
  def testMapUdaf(): Unit = {
    checkResult(
      "SELECT myPrimitiveMapUdaf(a, b)[3] FROM Table3",
      Seq(row(15))
    )
    checkResult(
      "SELECT myPrimitiveMapUdaf(a, b)[6] FROM Table3",
      Seq(row(111))
    )
    checkResult(
      "SELECT myObjectMapUdaf(a, c)['Co'] FROM Table3",
      Seq(row(210))
    )
    checkResult(
      "SELECT myObjectMapUdaf(a, c)['He'] FROM Table3",
      Seq(row(9))
    )
    checkResult(
      "SELECT myNestedMapUdaf(a, b, c)[6]['Co'] FROM Table3",
      Seq(row(111))
    )
    checkResult(
      "SELECT myNestedMapUdaf(a, b, c)[3]['He'] FROM Table3",
      Seq(row(4))
    )
    checkResult(
      "SELECT myNestedMapUdaf(a, b, c)[3]['Co'] FROM Table3",
      Seq(row("null"))
    )
  }

  @Test
  def testApproximateCountDistinct(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.fullDataTypesData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyTable (
         |  `boolean` BOOLEAN,
         |  `byte` TINYINT,
         |  `short` SMALLINT,
         |  `int` INT,
         |  `long` BIGINT,
         |  `float` FLOAT,
         |  `double` DOUBLE,
         |  `decimal52` DECIMAL(5, 2),
         |  `decimal3010` DECIMAL(30, 10),
         |  `string` VARCHAR(5),
         |  `char` CHAR(5),
         |  `date` DATE,
         |  `time` TIME(0),
         |  `timestamp` TIMESTAMP(9),
         |  `timestamp_ltz` TIMESTAMP(9) WITH LOCAL TIME ZONE,
         |  `array` ARRAY<BIGINT>,
         |  `row` ROW<f1 BIGINT, f2 STRING, f3 DOUBLE>,
         |  `map` MAP<STRING, INT>
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    )
    checkResult(
      """
        |SELECT
        | APPROX_COUNT_DISTINCT(`byte`),
        | APPROX_COUNT_DISTINCT(`short`),
        | APPROX_COUNT_DISTINCT(`int`),
        | APPROX_COUNT_DISTINCT(`long`),
        | APPROX_COUNT_DISTINCT(`float`),
        | APPROX_COUNT_DISTINCT(`double`),
        | APPROX_COUNT_DISTINCT(`string`),
        | APPROX_COUNT_DISTINCT(`date`),
        | APPROX_COUNT_DISTINCT(`time`),
        | APPROX_COUNT_DISTINCT(`timestamp`),
        | APPROX_COUNT_DISTINCT(`timestamp_ltz`),
        | APPROX_COUNT_DISTINCT(`decimal52`),
        | APPROX_COUNT_DISTINCT(`decimal3010`)
        | FROM MyTable
      """.stripMargin,
      Seq(row(4L, 4L, 4L, 4L, 4L, 4L, 4L, 4L, 4L, 4L, 4L, 4L, 4L))
    )
  }

  @Test
  def testJsonArrayAggAndJsonObjectAggWithOtherAggs(): Unit = {
    val sql =
      s"""
         |SELECT
         |  MAX(d), JSON_OBJECTAGG(g VALUE d), JSON_ARRAYAGG(d), JSON_ARRAYAGG(g)
         |FROM Table5 WHERE d <= 3
         |""".stripMargin
    checkResult(
      sql,
      Seq(row("3, {\"ABC\":3,\"BCD\":3,\"Hallo\":1,\"Hallo Welt\":2," +
        "\"Hallo Welt wie\":2,\"Hallo Welt wie gehts?\":3}, [1,2,2,3,3,3], " +
        "[\"Hallo\",\"Hallo Welt\",\"Hallo Welt wie\",\"Hallo Welt wie gehts?\",\"ABC\",\"BCD\"]"))
    )
  }

  @Test
  def testGroupJsonArrayAggAndJsonObjectAggWithOtherAggs(): Unit = {
    val sql =
      s"""
         |SELECT
         |  d, JSON_OBJECTAGG(g VALUE f), JSON_ARRAYAGG(g), JSON_ARRAYAGG(f), max(f)
         |FROM Table5 WHERE d <= 3 GROUP BY d
         |""".stripMargin
    checkResult(
      sql,
      Seq(
        row("1, {\"Hallo\":0}, [\"Hallo\"], [0], 0"),
        row(
          "2, {\"Hallo Welt\":1,\"Hallo Welt wie\":2}, [\"Hallo Welt\",\"Hallo Welt wie\"], [1,2], 2"),
        row(
          "3, {\"ABC\":4,\"BCD\":5,\"Hallo Welt wie gehts?\":3}, [\"Hallo Welt wie gehts?\",\"ABC\",\"BCD\"], [3,4,5], 5")
      )
    )
  }
}

class MyPojoAggFunction extends AggregateFunction[MyPojo, CountAccumulator] {

  def accumulate(acc: CountAccumulator, value: MyPojo): Unit = {
    if (value != null) {
      acc.f0 += value.f2
    }
  }

  def retract(acc: CountAccumulator, value: MyPojo): Unit = {
    if (value != null) {
      acc.f0 -= value.f2
    }
  }

  override def getValue(acc: CountAccumulator): MyPojo = {
    new MyPojo(acc.f0.toInt, acc.f0.toInt)
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }

  override def getAccumulatorType: TypeInformation[CountAccumulator] = {
    new TupleTypeInfo[CountAccumulator](classOf[CountAccumulator], Types.LONG)
  }

  override def getResultType: TypeInformation[MyPojo] = MyToPojoFunc.getResultType(null)
}

class VarArgsAggFunction extends AggregateFunction[JLong, CountAccumulator] {

  @varargs
  def accumulate(acc: CountAccumulator, value: JLong, args: String*): Unit = {
    acc.f0 += value
    args.foreach(s => acc.f0 += s.toLong)
  }

  @varargs
  def retract(acc: CountAccumulator, value: JLong, args: String*): Unit = {
    acc.f0 -= value
    args.foreach(s => acc.f0 -= s.toLong)
  }

  override def getValue(acc: CountAccumulator): JLong = {
    acc.f0
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }
}

class SimplePrimitiveArrayUdaf extends AggregateFunction[lang.Long, Array[Long]] {

  var i = 0

  override def createAccumulator(): Array[Long] = new Array[Long](10000)

  override def getValue(accumulator: Array[Long]): lang.Long = Long.box(accumulator.sum)

  def accumulate(accumulator: Array[Long], a: Long): Unit = {
    accumulator(i) += a
    i += 1
    if (i >= accumulator.length) {
      i = 0
    }
  }

  override def getAccumulatorType: TypeInformation[Array[Long]] =
    PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO

  override def getResultType: TypeInformation[lang.Long] = Types.LONG
}

class MyPrimitiveArrayUdaf extends AggregateFunction[Array[Long], Array[Long]] {

  override def createAccumulator(): Array[Long] = new Array[Long](2)

  override def getValue(accumulator: Array[Long]): Array[Long] = accumulator

  def accumulate(accumulator: Array[Long], a: Int, b: Long): Unit = {
    accumulator(0) += a
    accumulator(1) += b
  }

  override def getAccumulatorType: TypeInformation[Array[Long]] =
    PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO

  override def getResultType: TypeInformation[Array[Long]] =
    PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO
}

class MyObjectArrayUdaf extends AggregateFunction[Array[String], Array[String]] {

  override def createAccumulator(): Array[String] = Array("", "")

  override def getValue(accumulator: Array[String]): Array[String] = accumulator

  def accumulate(accumulator: Array[String], c: String): Unit = {
    accumulator(0) = accumulator(0) + c.charAt(0)
    accumulator(1) = accumulator(1) + c.charAt(c.length - 1)
  }

  override def getAccumulatorType: TypeInformation[Array[String]] =
    BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO

  override def getResultType: TypeInformation[Array[String]] =
    BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO
}

class MyNestedLongArrayUdaf extends AggregateFunction[Array[Array[Long]], Array[Array[Long]]] {

  override def createAccumulator(): Array[Array[Long]] = Array(Array(0, 0), Array(0, 0))

  override def getValue(accumulator: Array[Array[Long]]): Array[Array[Long]] = accumulator

  def accumulate(accumulator: Array[Array[Long]], a: Int, b: Long): Unit = {
    accumulator(0)(0) += a
    accumulator(0)(1) += b
    accumulator(1)(0) += b
    accumulator(1)(1) += a
  }

  override def getAccumulatorType =
    ObjectArrayTypeInfo.getInfoFor(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO)

  override def getResultType = getAccumulatorType
}

class MyNestedStringArrayUdaf
  extends AggregateFunction[Array[Array[String]], Array[Array[String]]] {

  override def createAccumulator(): Array[Array[String]] = Array(Array("", ""), Array("", ""))

  override def getValue(accumulator: Array[Array[String]]): Array[Array[String]] = accumulator

  def accumulate(accumulator: Array[Array[String]], c: String): Unit = {
    accumulator(0)(0) = accumulator(0)(0) + c.charAt(0)
    accumulator(0)(1) = accumulator(0)(1) + c.charAt(c.length - 1)
    accumulator(1)(0) = accumulator(1)(0) + c.charAt(c.length - 1)
    accumulator(1)(1) = accumulator(1)(1) + c.charAt(0)
  }
}

class MyPrimitiveMapUdaf
  extends AggregateFunction[java.util.Map[Long, Int], java.util.Map[Long, Int]] {

  override def createAccumulator(): java.util.Map[Long, Int] =
    new java.util.HashMap[Long, Int]()

  override def getValue(accumulator: java.util.Map[Long, Int]): java.util.Map[Long, Int] =
    accumulator

  def accumulate(accumulator: java.util.Map[Long, Int], a: Int, b: Long): Unit = {
    accumulator.putIfAbsent(b, 0)
    accumulator.put(b, accumulator.get(b) + a)
  }

  override def getAccumulatorType =
    new MapTypeInfo(Types.LONG, Types.INT)
      .asInstanceOf[TypeInformation[java.util.Map[Long, Int]]]

  override def getResultType =
    getAccumulatorType
}

class MyObjectMapUdaf
  extends AggregateFunction[java.util.Map[String, Int], java.util.Map[String, Int]] {

  override def createAccumulator(): java.util.Map[String, Int] =
    new java.util.HashMap[String, Int]()

  override def getValue(accumulator: java.util.Map[String, Int]): java.util.Map[String, Int] =
    accumulator

  def accumulate(accumulator: java.util.Map[String, Int], a: Int, c: String): Unit = {
    val key = c.substring(0, 2)
    accumulator.putIfAbsent(key, 0)
    accumulator.put(key, accumulator.get(key) + a)
  }

  override def getAccumulatorType =
    new MapTypeInfo(Types.STRING, Types.INT)
      .asInstanceOf[TypeInformation[java.util.Map[String, Int]]]

  override def getResultType = getAccumulatorType
}

class MyNestedMapUdf
  extends AggregateFunction[
    java.util.Map[JLong, java.util.Map[String, JInt]],
    java.util.Map[JLong, java.util.Map[String, JInt]]] {

  override def createAccumulator(): java.util.Map[JLong, java.util.Map[String, JInt]] =
    new java.util.HashMap[JLong, java.util.Map[String, JInt]]()

  override def getValue(accumulator: java.util.Map[JLong, java.util.Map[String, JInt]])
      : java.util.Map[JLong, java.util.Map[String, JInt]] =
    accumulator

  def accumulate(
      accumulator: java.util.Map[JLong, java.util.Map[String, JInt]],
      a: JInt,
      b: JLong,
      c: String): Unit = {
    val key = c.substring(0, 2)
    accumulator.putIfAbsent(b, new java.util.HashMap[String, JInt]())
    accumulator.get(b).putIfAbsent(key, 0)
    accumulator.get(b).put(key, accumulator.get(b).get(key) + a)
  }
}
