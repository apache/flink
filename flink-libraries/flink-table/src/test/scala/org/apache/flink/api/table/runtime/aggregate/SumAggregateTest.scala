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

package org.apache.flink.api.table.runtime.aggregate

abstract class SumAggregateTestBase[T: Numeric] extends AggregateTestBase[T] {

  private val numeric: Numeric[T] = implicitly[Numeric[T]]

  def maxVal: T
  private val minVal = numeric.negate(maxVal)

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      minVal,
      numeric.fromInt(1),
      null.asInstanceOf[T],
      numeric.fromInt(2),
      numeric.fromInt(3),
      numeric.fromInt(4),
      numeric.fromInt(5),
      numeric.fromInt(-10),
      numeric.fromInt(-20),
      numeric.fromInt(17),
      null.asInstanceOf[T],
      maxVal
    )
  )

  override def expectedResults: Seq[T] = Seq(numeric.fromInt(2))

}

class ByteSumAggregateTest extends SumAggregateTestBase[Byte] {

  override def maxVal = (Byte.MaxValue / 2).toByte

  override def aggregator: Aggregate[Byte] = new ByteSumAggregate
}

class ShortSumAggregateTest extends SumAggregateTestBase[Short] {

  override def maxVal = (Short.MaxValue / 2).toShort

  override def aggregator: Aggregate[Short] = new ShortSumAggregate
}

class IntSumAggregateTest extends SumAggregateTestBase[Int] {

  override def maxVal = Int.MaxValue / 2

  override def aggregator: Aggregate[Int] = new IntSumAggregate
}

class LongSumAggregateTest extends SumAggregateTestBase[Long] {

  override def maxVal = Long.MaxValue / 2

  override def aggregator: Aggregate[Long] = new LongSumAggregate
}

class FloatSumAggregateTest extends SumAggregateTestBase[Float] {

  override def maxVal = 12345.6789f

  override def aggregator: Aggregate[Float] = new FloatSumAggregate
}

class DoubleSumAggregateTest extends SumAggregateTestBase[Double] {

  override def maxVal = 12345.6789d

  override def aggregator: Aggregate[Double] = new DoubleSumAggregate
}
