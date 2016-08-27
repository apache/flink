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

import java.math.BigDecimal

abstract class AvgAggregateTestBase[T: Numeric] extends AggregateTestBase[T] {

  private val numeric: Numeric[T] = implicitly[Numeric[T]]

  def minVal: T
  def maxVal: T

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      minVal,
      minVal,
      null.asInstanceOf[T],
      minVal,
      minVal,
      null.asInstanceOf[T],
      minVal,
      minVal,
      minVal
    ),
    Seq(
      maxVal,
      maxVal,
      null.asInstanceOf[T],
      maxVal,
      maxVal,
      null.asInstanceOf[T],
      maxVal,
      maxVal,
      maxVal
    ),
    Seq(
      minVal,
      maxVal,
      null.asInstanceOf[T],
      numeric.fromInt(0),
      numeric.negate(maxVal),
      numeric.negate(minVal),
      null.asInstanceOf[T]
    ),
    Seq(
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T],
      null.asInstanceOf[T]
    )
  )

  override def expectedResults: Seq[T] = Seq(
    minVal,
    maxVal,
    numeric.fromInt(0),
    null.asInstanceOf[T]
  )
}

class ByteAvgAggregateTest extends AvgAggregateTestBase[Byte] {

  override def minVal = (Byte.MinValue + 1).toByte
  override def maxVal = (Byte.MaxValue - 1).toByte

  override def aggregator = new ByteAvgAggregate()
}

class ShortAvgAggregateTest extends AvgAggregateTestBase[Short] {

  override def minVal = (Short.MinValue + 1).toShort
  override def maxVal = (Short.MaxValue - 1).toShort

  override def aggregator = new ShortAvgAggregate()
}

class IntAvgAggregateTest extends AvgAggregateTestBase[Int] {

  override def minVal = Int.MinValue + 1
  override def maxVal = Int.MaxValue - 1

  override def aggregator = new IntAvgAggregate()
}

class LongAvgAggregateTest extends AvgAggregateTestBase[Long] {

  override def minVal = Long.MinValue + 1
  override def maxVal = Long.MaxValue - 1

  override def aggregator = new LongAvgAggregate()
}

class FloatAvgAggregateTest extends AvgAggregateTestBase[Float] {

  override def minVal = Float.MinValue
  override def maxVal = Float.MaxValue

  override def aggregator = new FloatAvgAggregate()
}

class DoubleAvgAggregateTest extends AvgAggregateTestBase[Double] {

  override def minVal = Float.MinValue
  override def maxVal = Float.MaxValue

  override def aggregator = new DoubleAvgAggregate()
}

class DecimalAvgAggregateTest extends AggregateTestBase[BigDecimal] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new BigDecimal("987654321000000"),
      new BigDecimal("-0.000000000012345"),
      null,
      new BigDecimal("0.000000000012345"),
      new BigDecimal("-987654321000000"),
      null,
      new BigDecimal("0")
    ),
    Seq(
      null,
      null,
      null,
      null
    )
  )

  override def expectedResults: Seq[BigDecimal] = Seq(
    BigDecimal.ZERO,
    null
  )

  override def aggregator: Aggregate[BigDecimal] = new DecimalAvgAggregate()
}
