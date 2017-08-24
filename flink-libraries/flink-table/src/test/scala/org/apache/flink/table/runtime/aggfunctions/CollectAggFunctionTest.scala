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

import com.google.common.collect.ImmutableList
import org.apache.commons.collections4.multiset.{AbstractMultiSet, HashMultiSet}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions._

/**
  * Test case for built-in collect aggregate functions
  */
class StringCollectAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[String], CollectAccumulator[String]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq("a", "a", "b", null, "c", null, "d", "e", null, "f"),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[String]] = {
    val set1 = new HashMultiSet[String]()
    set1.addAll(ImmutableList.of("a", "a", "b", "c", "d", "e", "f"))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[String], CollectAccumulator[String]] =
    new StringCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class IntCollectAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[Int], CollectAccumulator[Int]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1, 1, 2, null, 3, null, 4, 5, null, 6),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[Int]] = {
    val set1 = new HashMultiSet[Int]()
    set1.addAll(ImmutableList.of(1, 1, 2, 3, 4, 5, 6))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[Int], CollectAccumulator[Int]] =
    new IntCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ByteCollectAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[Byte], CollectAccumulator[Byte]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1.toByte, 1.toByte, 2.toByte, null, 3.toByte, null, 4.toByte, 5.toByte, null, 6.toByte),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[Byte]] = {
    val set1 = new HashMultiSet[Byte]()
    set1.addAll(ImmutableList.of(1, 1, 2, 3, 4, 5, 6))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[Byte], CollectAccumulator[Byte]] =
    new ByteCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ShortCollectAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[Short], CollectAccumulator[Short]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1.toShort, 1.toShort, 2.toShort, null,
      3.toShort, null, 4.toShort, 5.toShort, null, 6.toShort),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[Short]] = {
    val set1 = new HashMultiSet[Short]()
    set1.addAll(ImmutableList.of(1, 1, 2, 3, 4, 5, 6))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[Short], CollectAccumulator[Short]] =
    new ShortCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class LongCollectAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[Long], CollectAccumulator[Long]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1L, 1L, 2L, null, 3L, null, 4L, 5L, null, 6L),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[Long]] = {
    val set1 = new HashMultiSet[Long]()
    set1.addAll(ImmutableList.of(1, 1, 2, 3, 4, 5, 6))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[Long], CollectAccumulator[Long]] =
    new LongCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class FloatAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[Float], CollectAccumulator[Float]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1f, 1f, 2f, null, 3.2f, null, 4f, 5f, null, 6f),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[Float]] = {
    val set1 = new HashMultiSet[Float]()
    set1.addAll(ImmutableList.of(1f, 1f, 2f, 3.2f, 4f, 5f, 6f))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[Float], CollectAccumulator[Float]] =
    new FloatCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class DoubleAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[Double], CollectAccumulator[Double]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(1d, 1d, 2d, null, 3.2d, null, 4d, 5d, null, 6d),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[Double]] = {
    val set1 = new HashMultiSet[Double]()
    set1.addAll(ImmutableList.of(1, 1, 2, 3.2, 4, 5, 6))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[Double], CollectAccumulator[Double]] =
    new DoubleCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

class ObjectAggFunctionTest
  extends AggFunctionTestBase[AbstractMultiSet[Object], CollectAccumulator[Object]] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(Tuple2(1, "a"), Tuple2(1, "a"), null, Tuple2(2, "b")),
    Seq(null, null, null, null, null, null)
  )

  override def expectedResults: Seq[AbstractMultiSet[Object]] = {
    val set1 = new HashMultiSet[Object]()
    set1.addAll(ImmutableList.of(Tuple2(1, "a"), Tuple2(1, "a"), Tuple2(2, "b")))
    Seq(set1, null)
  }

  override def aggregator: AggregateFunction[AbstractMultiSet[Object], CollectAccumulator[Object]] =
    new ObjectCollectAggFunction()

  override def retractFunc = aggregator.getClass.getMethod("retract", accType, classOf[Any])
}

