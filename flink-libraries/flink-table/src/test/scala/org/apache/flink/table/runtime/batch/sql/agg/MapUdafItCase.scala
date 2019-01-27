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

package org.apache.flink.table.runtime.batch.sql.agg

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.TestData.{data3, nullablesOfData3, type3}
import org.apache.flink.types.Row
import org.junit.{Before, Test}

import scala.collection.Seq

class MapUdafITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)

    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "HashAgg")

    registerFunction("longIntMapUdaf", new LongIntMapUdaf())
    registerFunction("stringIntMapUdaf", new StringIntMapUdaf())
    registerFunction("longGenericMapUdaf", new LongGenericMapUdaf())
    registerFunction("nestedMapUdaf", new NestedMapUdaf())
  }

  @Test
  def testBoxedMapUdaf(): Unit = {
    val expected: util.Map[Long, Int] = new util.HashMap[Long, Int]()
    expected.put(1, 1)
    expected.put(2, 5)
    expected.put(3, 15)
    expected.put(4, 34)
    expected.put(5, 65)
    expected.put(6, 111)

    checkMapResult(
      "SELECT longIntMapUdaf(a, b) FROM Table3",
      expected)
  }

  @Test
  def testStringMapUdaf(): Unit = {
    val expected: util.Map[String, Int] = new util.HashMap[String, Int]()
    expected.put("Hi", 1)
    expected.put("He", 9)
    expected.put("I ", 5)
    expected.put("Lu", 6)
    expected.put("Co", 210)

    checkMapResult(
      "SELECT stringIntMapUdaf(a, c) FROM Table3",
      expected)
  }

  @Test
  def testGenericMapUdaf(): Unit = {
    val expected: util.Map[Long, util.List[Int]] = new util.HashMap[Long, util.List[Int]]()
    var v = 0
    for (i <- 1 to 6) {
      val al: util.ArrayList[Int] = new util.ArrayList[Int]()
      for (_ <- 1 to i) {
        v += 1
        al.add(v)
      }
      expected.put(i, al)
    }

    checkMapResult(
      "SELECT longGenericMapUdaf(a, b) FROM Table3",
      expected)
  }

  @Test
  def testObjectMapUdaf(): Unit = {
    val expected: util.Map[Long, util.Map[String, Int]] =
      new util.HashMap[Long, util.Map[String, Int]]()
    for (r <- data3) {
      val key1 = r.getField(1).asInstanceOf[Long]
      val key2 = r.getField(2).asInstanceOf[String].substring(0, 2)
      expected.putIfAbsent(key1, new util.HashMap[String, Int]())
      val m = expected.get(key1)
      m.put(key2, m.get(key2) + r.getField(0).asInstanceOf[Int])
    }

    checkMapResult(
      "SELECT nestedMapUdaf(a, b, c) FROM Table3",
      expected)
  }

  def checkMapResult(sql: String, expected: util.Map[_, _]): Unit = {
    check(sql, (result: Seq[Row]) =>
      if (result.size != 1) {
        Some("\nResult size is incorrect! Must contain exactly 1 row.\n")
      } else if (result.head.getArity != 1) {
        Some("\nResult size is incorrect! Must contain exactly 1 element.\n")
      } else if (!result.head.getField(0).isInstanceOf[util.Map[_, _]]) {
        Some("\nResult type is incorrect! Must be a util.Map.\n")
      } else {
        val actual = result.head.getField(0).asInstanceOf[util.Map[_, _]]
        if (actual == expected) {
          None
        } else {
          Some(
            s"""
               |Result content is incorrect!
               |
               |Expecting:
               |${expected.toString}
               |
               |Actual:
               |${actual.toString}
           """.stripMargin)
        }
      }
    )
  }
}

class LongIntMapUdaf extends AggregateFunction[
    util.Map[Long, Int], util.Map[Long, Int]] {

  override def createAccumulator(): util.Map[Long, Int] = new util.HashMap[Long, Int]()

  override def getValue(accumulator: util.Map[Long, Int]): util.Map[Long, Int] =
    accumulator

  def accumulate(accumulator: util.Map[Long, Int], a: Int, b: Long): Unit = {
    accumulator.putIfAbsent(b, 0)
    accumulator.put(b, accumulator.get(b) + a)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createMapType(DataTypes.LONG, DataTypes.INT)

  override def getResultType: DataType =
    DataTypes.createMapType(DataTypes.LONG, DataTypes.INT)
}

class StringIntMapUdaf extends AggregateFunction[
    util.Map[String, Int], util.Map[String, Int]] {

  override def createAccumulator(): util.Map[String, Int] = new util.HashMap[String, Int]()

  override def getValue(accumulator: util.Map[String, Int]): util.Map[String, Int] =
    accumulator

  def accumulate(accumulator: util.Map[String, Int], a: Int, c: String): Unit = {
    val key = c.substring(0, 2)
    accumulator.putIfAbsent(key, 0)
    accumulator.put(key, accumulator.get(key) + a)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createMapType(DataTypes.STRING, DataTypes.INT)

  override def getResultType: DataType =
    DataTypes.createMapType(DataTypes.STRING, DataTypes.INT)
}

class LongGenericMapUdaf extends AggregateFunction[
    util.Map[Long, util.List[Int]], util.Map[Long, util.List[Int]]] {

  override def createAccumulator(): util.Map[Long, util.List[Int]] =
    new util.HashMap[Long, util.List[Int]]()

  override def getValue(
      accumulator: util.Map[Long, util.List[Int]]): util.Map[Long, util.List[Int]] =
    accumulator

  def accumulate(accumulator: util.Map[Long, util.List[Int]], a: Int, b: Long): Unit = {
    accumulator.putIfAbsent(b, new util.ArrayList[Int]())
    accumulator.get(b).add(a)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createMapType(
      DataTypes.LONG,
      DataTypes.createGenericType(classOf[util.List[Int]]))

  override def getResultType: DataType =
    DataTypes.createMapType(
      DataTypes.LONG,
      DataTypes.createGenericType(classOf[util.List[Int]]))
}

class NestedMapUdaf extends AggregateFunction[
  util.Map[Long, util.Map[String, Int]],
  util.Map[Long, util.Map[String, Int]]] {

  override def createAccumulator(): util.Map[Long, util.Map[String, Int]] =
    new util.HashMap[Long, util.Map[String, Int]]()

  override def getValue(accumulator: util.Map[Long, util.Map[String, Int]])
  : util.Map[Long, util.Map[String, Int]] =
    accumulator

  def accumulate(
      accumulator: util.Map[Long, util.Map[String, Int]],
      a: Int, b: Long, c: String): Unit = {
    val key = c.substring(0, 2)
    accumulator.putIfAbsent(b, new util.HashMap[String, Int]())
    accumulator.get(b).putIfAbsent(key, 0)
    accumulator.get(b).put(key, accumulator.get(b).get(key) + a)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createMapType(
      DataTypes.LONG,
      DataTypes.createMapType(DataTypes.STRING, DataTypes.INT))

  override def getResultType: DataType =
    DataTypes.createMapType(
      DataTypes.LONG,
      DataTypes.createMapType(DataTypes.STRING, DataTypes.INT))
}
