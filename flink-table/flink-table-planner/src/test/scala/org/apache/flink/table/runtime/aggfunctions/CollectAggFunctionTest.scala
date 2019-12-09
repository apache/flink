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

package org.apache.flink.table.runtime.aggfunctions

import java.util

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions._

import scala.collection.JavaConverters._

/**
  * Test case for built-in collect aggregate functions
  */
class StringCollectAggFunctionTest
  extends AggFunctionTestBase[util.Map[String, Integer], CollectAccumulator[String]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq("a", "a", "b", null, "c", null, "d", "e", null, "f"),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[String, Integer]] = {
    val map = new util.HashMap[String, Integer]()
    map.put("a", 2)
    map.put("b", 1)
    map.put("c", 1)
    map.put("d", 1)
    map.put("e", 1)
    map.put("f", 1)
    Seq(map, Map[String, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[
    util.Map[String, Integer], CollectAccumulator[String]] =
    new CollectAggFunction(BasicTypeInfo.STRING_TYPE_INFO)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class IntCollectAggFunctionTest
  extends AggFunctionTestBase[util.Map[Int, Integer], CollectAccumulator[Int]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1, 1, 2, null, 3, null, 4, 5, null),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[Int, Integer]] = {
    val map = new util.HashMap[Int, Integer]()
    map.put(1, 2)
    map.put(2, 1)
    map.put(3, 1)
    map.put(4, 1)
    map.put(5, 1)
    Seq(map, Map[Int, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[util.Map[Int, Integer], CollectAccumulator[Int]] =
    new CollectAggFunction(BasicTypeInfo.INT_TYPE_INFO)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ByteCollectAggFunctionTest
  extends AggFunctionTestBase[util.Map[Byte, Integer], CollectAccumulator[Byte]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1.toByte, 1.toByte, 2.toByte, null, 3.toByte, null, 4.toByte, 5.toByte, null),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[Byte, Integer]] = {
    val map = new util.HashMap[Byte, Integer]()
    map.put(1, 2)
    map.put(2, 1)
    map.put(3, 1)
    map.put(4, 1)
    map.put(5, 1)
    Seq(map, Map[Byte, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[util.Map[Byte, Integer], CollectAccumulator[Byte]] =
    new CollectAggFunction(BasicTypeInfo.BYTE_TYPE_INFO)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ShortCollectAggFunctionTest
  extends AggFunctionTestBase[util.Map[Short, Integer], CollectAccumulator[Short]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1.toShort, 1.toShort, 2.toShort, null,
      3.toShort, null, 4.toShort, 5.toShort, null),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[Short, Integer]] = {
    val map = new util.HashMap[Short, Integer]()
    map.put(1, 2)
    map.put(2, 1)
    map.put(3, 1)
    map.put(4, 1)
    map.put(5, 1)
    Seq(map, Map[Short, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[util.Map[Short, Integer], CollectAccumulator[Short]] =
    new CollectAggFunction(BasicTypeInfo.SHORT_TYPE_INFO)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class LongCollectAggFunctionTest
  extends AggFunctionTestBase[util.Map[Long, Integer], CollectAccumulator[Long]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1L, 1L, 2L, null, 3L, null, 4L, 5L, null),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[Long, Integer]] = {
    val map = new util.HashMap[Long, Integer]()
    map.put(1, 2)
    map.put(2, 1)
    map.put(3, 1)
    map.put(4, 1)
    map.put(5, 1)
    Seq(map, Map[Long, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[util.Map[Long, Integer], CollectAccumulator[Long]] =
    new CollectAggFunction(BasicTypeInfo.LONG_TYPE_INFO)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class FloatAggFunctionTest
  extends AggFunctionTestBase[util.Map[Float, Integer], CollectAccumulator[Float]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1f, 1f, 2f, null, 3.2f, null, 4f, 5f, null),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[Float, Integer]] = {
    val map = new util.HashMap[Float, Integer]()
    map.put(1, 2)
    map.put(2, 1)
    map.put(3.2f, 1)
    map.put(4, 1)
    map.put(5, 1)
    Seq(map, Map[Float, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[util.Map[Float, Integer], CollectAccumulator[Float]] =
    new CollectAggFunction(BasicTypeInfo.FLOAT_TYPE_INFO)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class DoubleAggFunctionTest
  extends AggFunctionTestBase[util.Map[Double, Integer], CollectAccumulator[Double]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1d, 1d, 2d, null, 3.2d, null, 4d, 5d),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[Double, Integer]] = {
    val map = new util.HashMap[Double, Integer]()
    map.put(1, 2)
    map.put(2, 1)
    map.put(3.2d, 1)
    map.put(4, 1)
    map.put(5, 1)
    Seq(map, Map[Double, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[
    util.Map[Double, Integer], CollectAccumulator[Double]] =
    new CollectAggFunction(BasicTypeInfo.DOUBLE_TYPE_INFO)

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ObjectCollectAggFunctionTest
  extends AggFunctionTestBase[util.Map[Object, Integer], CollectAccumulator[Object]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(Tuple2(1, "a"), Tuple2(1, "a"), null, Tuple2(2, "b")),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[util.Map[Object, Integer]] = {
    val map = new util.HashMap[Object, Integer]()
    map.put(Tuple2(1, "a"), 2)
    map.put(Tuple2(2, "b"), 1)
    Seq(map, Map[Object, Integer]().asJava)
  }

  override def aggregator: AggregateFunction[
    util.Map[Object, Integer], CollectAccumulator[Object]] =
    new CollectAggFunction(new GenericTypeInfo[Object](classOf[Object]))

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

