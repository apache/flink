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

abstract class MinAggregateTestBase[T: Numeric] extends AggregateTestBase[T] {

  private val numeric: Numeric[T] = implicitly[Numeric[T]]

  def minVal: T
  def maxVal: T

  override def inputValueSets: Seq[Seq[T]] = Seq(
    Seq(
      numeric.fromInt(1),
      null.asInstanceOf[T],
      maxVal,
      numeric.fromInt(-99),
      numeric.fromInt(3),
      numeric.fromInt(56),
      numeric.fromInt(0),
      minVal,
      numeric.fromInt(-20),
      numeric.fromInt(17),
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
    null.asInstanceOf[T]
  )
}

class ByteMinAggregateTest extends MinAggregateTestBase[Byte] {

  override def minVal = (Byte.MinValue + 1).toByte
  override def maxVal = (Byte.MaxValue - 1).toByte

  override def aggregator: Aggregate[Byte] = new ByteMinAggregate()
}

class ShortMinAggregateTest extends MinAggregateTestBase[Short] {

  override def minVal = (Short.MinValue + 1).toShort
  override def maxVal = (Short.MaxValue - 1).toShort

  override def aggregator: Aggregate[Short] = new ShortMinAggregate()
}

class IntMinAggregateTest extends MinAggregateTestBase[Int] {

  override def minVal = Int.MinValue + 1
  override def maxVal = Int.MaxValue - 1

  override def aggregator: Aggregate[Int] = new IntMinAggregate()
}

class LongMinAggregateTest extends MinAggregateTestBase[Long] {

  override def minVal = Long.MinValue + 1
  override def maxVal = Long.MaxValue - 1

  override def aggregator: Aggregate[Long] = new LongMinAggregate()
}

class FloatMinAggregateTest extends MinAggregateTestBase[Float] {

  override def minVal = Float.MinValue / 2
  override def maxVal = Float.MaxValue / 2

  override def aggregator: Aggregate[Float] = new FloatMinAggregate()
}

class DoubleMinAggregateTest extends MinAggregateTestBase[Double] {

  override def minVal = Double.MinValue / 2
  override def maxVal = Double.MaxValue / 2

  override def aggregator: Aggregate[Double] = new DoubleMinAggregate()
}

class BooleanMinAggregateTest extends AggregateTestBase[Boolean] {

  override def inputValueSets: Seq[Seq[Boolean]] = Seq(
    Seq(
      false,
      false,
      false
    ),
    Seq(
      true,
      true,
      true
    ),
    Seq(
      true,
      false,
      null.asInstanceOf[Boolean],
      true,
      false,
      true,
      null.asInstanceOf[Boolean]
    ),
    Seq(
      null.asInstanceOf[Boolean],
      null.asInstanceOf[Boolean],
      null.asInstanceOf[Boolean]
    )
  )

  override def expectedResults: Seq[Boolean] = Seq(
    false,
    true,
    false,
    null.asInstanceOf[Boolean]
  )

  override def aggregator: Aggregate[Boolean] = new BooleanMinAggregate()
}

class DecimalMinAggregateTest extends AggregateTestBase[BigDecimal] {

  override def inputValueSets: Seq[Seq[_]] = Seq(
    Seq(
      new BigDecimal("1"),
      new BigDecimal("1000"),
      new BigDecimal("-1"),
      new BigDecimal("-999.998999"),
      null,
      new BigDecimal("0"),
      new BigDecimal("-999.999"),
      null,
      new BigDecimal("999.999")
    ),
    Seq(
      null,
      null,
      null,
      null,
      null
    )
  )

  override def expectedResults: Seq[BigDecimal] = Seq(
    new BigDecimal("-999.999"),
    null
  )

  override def aggregator: Aggregate[BigDecimal] = new DecimalMinAggregate()

}
