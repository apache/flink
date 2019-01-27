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
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData.{data3, nullablesOfData3, type3}
import org.junit.{Before, Test}

import scala.collection.Seq

class ArrayUdafITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)

    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "HashAgg")

    registerFunction("primitiveLongArrayUdaf", new PrimitiveLongArrayUdaf())
    registerFunction("boxedLongArrayUdaf", new BoxedLongArrayUdaf())
    registerFunction("stringArrayUdaf", new StringArrayUdaf())
    registerFunction("genericArrayUdaf", new GenericArrayUdaf())
    registerFunction("nestedLongArrayUdaf", new NestedLongArrayUdaf())
  }

  @Test
  def testPrimitiveArrayUdaf(): Unit = {
    checkResult(
      "SELECT primitiveLongArrayUdaf(a, b) FROM Table3",
      Seq(row(Array(231, 91)))
    )
  }

  @Test
  def testBoxedArrayUdaf(): Unit = {
    checkResult(
      "SELECT boxedLongArrayUdaf(a, b) FROM Table3",
      Seq(row(Array(231, 91)))
    )
  }

  @Test
  def testStringArrayUdaf(): Unit = {
    checkResult(
      "SELECT stringArrayUdaf(c) FROM Table3",
      Seq(row(Array("HHHHILCCCCCCCCCCCCCCC", "iod?.r123456789012345")))
    )
  }

  @Test
  def testGenericArrayUdaf(): Unit = {
    val expected = Array(new util.ArrayList[Long](), new util.ArrayList[Long]())
    var idx = 1
    for (i <- 1 to 6) {
      for (_ <- 1 to i) {
        expected(idx).add(i)
        idx = 1 - idx
      }
    }

    checkResult(
      "SELECT genericArrayUdaf(a, b) FROM Table3",
      Seq(row(expected))
    )
  }

  @Test
  def testObjectArrayUdaf(): Unit = {
    checkResult(
      "SELECT nestedLongArrayUdaf(a, b)[2] FROM Table3",
      Seq(row(Array(91, 231)))
    )
  }
}

class PrimitiveLongArrayUdaf extends AggregateFunction[Array[Long], Array[Long]] {

  override def createAccumulator(): Array[Long] = new Array[Long](2)

  override def getValue(accumulator: Array[Long]): Array[Long] = accumulator

  def accumulate(accumulator: Array[Long], a: Int, b: Long): Unit = {
    accumulator(0) += a
    accumulator(1) += b
  }

  override def getAccumulatorType: DataType = DataTypes.createPrimitiveArrayType(DataTypes.LONG)

  override def getResultType: DataType = DataTypes.createPrimitiveArrayType(DataTypes.LONG)
}

class BoxedLongArrayUdaf extends AggregateFunction[Array[java.lang.Long], Array[java.lang.Long]] {

  override def createAccumulator(): Array[java.lang.Long] =
    Array(new java.lang.Long(0), new java.lang.Long(0))

  override def getValue(accumulator: Array[java.lang.Long]): Array[java.lang.Long] = accumulator

  def accumulate(accumulator: Array[java.lang.Long], a: Int, b: java.lang.Long): Unit = {
    accumulator(0) += a
    accumulator(1) += b
  }

  override def getAccumulatorType: DataType = DataTypes.createArrayType(DataTypes.LONG)

  override def getResultType: DataType = DataTypes.createArrayType(DataTypes.LONG)
}

class StringArrayUdaf extends AggregateFunction[Array[String], Array[String]] {

  override def createAccumulator(): Array[String] = Array("", "")

  override def getValue(accumulator: Array[String]): Array[String] = accumulator

  def accumulate(accumulator: Array[String], c: String): Unit = {
    accumulator(0) = accumulator(0) + c.charAt(0)
    accumulator(1) = accumulator(1) + c.charAt(c.length - 1)
  }

  override def getAccumulatorType: DataType = DataTypes.createArrayType(DataTypes.STRING)

  override def getResultType: DataType = DataTypes.createArrayType(DataTypes.STRING)
}

class GenericArrayUdaf extends AggregateFunction[
  Array[util.List[Long]], Array[util.List[Long]]] {

  override def createAccumulator(): Array[util.List[Long]] =
    Array(new util.ArrayList[Long](), new util.ArrayList[Long]())

  override def getValue(accumulator: Array[util.List[Long]]): Array[util.List[Long]] =
    accumulator

  def accumulate(accumulator: Array[util.List[Long]], a: Int, b: Long): Unit = {
    accumulator(a % 2).add(b)
  }

  override def getAccumulatorType: DataType =
    DataTypes.createArrayType(DataTypes.createGenericType(classOf[util.List[Long]]))

  override def getResultType: DataType =
    DataTypes.createArrayType(DataTypes.createGenericType(classOf[util.List[Long]]))
}

class NestedLongArrayUdaf extends AggregateFunction[Array[Array[Long]], Array[Array[Long]]] {

  override def createAccumulator(): Array[Array[Long]] = Array(Array(0, 0), Array(0, 0))

  override def getValue(accumulator: Array[Array[Long]]): Array[Array[Long]] = accumulator

  def accumulate(accumulator: Array[Array[Long]], a: Int, b: Long): Unit = {
    accumulator(0)(0) += a
    accumulator(0)(1) += b
    accumulator(1)(0) += b
    accumulator(1)(1) += a
  }

  override def getAccumulatorType: DataType =
    DataTypes.createArrayType(DataTypes.createPrimitiveArrayType(DataTypes.LONG))

  override def getResultType: DataType =
    DataTypes.createArrayType(DataTypes.createPrimitiveArrayType(DataTypes.LONG))
}
