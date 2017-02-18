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
package org.apache.flink.table.functions.builtInAggFuncs

import java.math.{BigDecimal, BigInteger}
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/**
  * Base class for built-in Integral Avg aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class IntegralAvgAggFunction[T] extends AggregateFunction[T] {
  /** The initial accumulator for Integral Avg aggregate function */
  class IntegralAvgAccumulator extends Accumulator {
    var sum: Long = 0
    var count: Long = 0
  }

  override def createAccumulator(): Accumulator = {
    new IntegralAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[Number].longValue()
      val accum = accumulator.asInstanceOf[IntegralAvgAccumulator]
      accum.sum += v
      accum.count += 1
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val accum = accumulator.asInstanceOf[IntegralAvgAccumulator]
    val sum = accum.sum
    if (accum.count == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(accum.sum / accum.count)
    }
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
    val aAccum = a.asInstanceOf[IntegralAvgAccumulator]
    val bAccum = b.asInstanceOf[IntegralAvgAccumulator]
    aAccum.count += bAccum.count
    aAccum.sum += bAccum.sum
    a
  }
  /**
    * Convert the intermediate result to the expected aggregation result type
    *
    * @param value the intermediate result. We use a Long container to save
    *         the intermediate result to avoid the overflow by sum operation.
    * @return the result value with the expected aggregation result type
    */
  def resultTypeConvert(value: Long): T
}

/**
  * Built-in Byte Avg aggregate function
  */
class ByteAvgAggFunction extends IntegralAvgAggFunction[Byte] {
  override def resultTypeConvert(value: Long): Byte = value.toByte
}

/**
  * Built-in Short Avg aggregate function
  */
class ShortAvgAggFunction extends IntegralAvgAggFunction[Short] {
  override def resultTypeConvert(value: Long): Short = value.toShort
}

/**
  * Built-in Int Avg aggregate function
  */
class IntAvgAggFunction extends IntegralAvgAggFunction[Int] {
  override def resultTypeConvert(value: Long): Int = value.toInt
}

/**
  * Base Class for Built-in Big Integral Avg aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class BigIntegralAvgAggFunction[T] extends AggregateFunction[T] {
  /** The initial accumulator for Big Integral Avg aggregate function */
  class BigIntegralAvgAccumulator extends Accumulator {
    var sum: BigInteger = BigInteger.ZERO
    var count: Long = 0
  }

  override def createAccumulator(): Accumulator = {
    new BigIntegralAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[Long]
      val accum = accumulator.asInstanceOf[BigIntegralAvgAccumulator]
      accum.sum = accum.sum.add(BigInteger.valueOf(v))
      accum.count += 1
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val accum = accumulator.asInstanceOf[BigIntegralAvgAccumulator]
    val sum = accum.sum
    if (accum.count == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(accum.sum.divide(BigInteger.valueOf(accum.count)))
    }
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
    val aAccum = a.asInstanceOf[BigIntegralAvgAccumulator]
    val bAccum = b.asInstanceOf[BigIntegralAvgAccumulator]
    aAccum.count += bAccum.count
    aAccum.sum = aAccum.sum.add(bAccum.sum)
    a
  }

  /**
    * Convert the intermediate result to the expected aggregation result type
    *
    * @param value the intermediate result. We use a BigInteger container to
    *         save the intermediate result to avoid the overflow by sum
    *         operation.
    * @return the result value with the expected aggregation result type
    */
  def resultTypeConvert(value: BigInteger): T
}

/**
  * Built-in Long Avg aggregate function
  */
class LongAvgAggFunction extends BigIntegralAvgAggFunction[Long] {
  override def resultTypeConvert(value: BigInteger): Long = value.longValue()
}

/**
  * Base class for built-in Floating Avg aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class FloatingAvgAggFunction[T] extends AggregateFunction[T] {
  /** The initial accumulator for Floating Avg aggregate function */
  class FloatingAvgAccumulator extends Accumulator {
    var sum: Double = 0
    var count: Long = 0
  }

  override def createAccumulator(): Accumulator = {
    new FloatingAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[Number].doubleValue()
      val accum = accumulator.asInstanceOf[FloatingAvgAccumulator]
      accum.sum += v
      accum.count += 1
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val accum = accumulator.asInstanceOf[FloatingAvgAccumulator]
    val sum = accum.sum
    if (accum.count == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(accum.sum / accum.count)
    }
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
    val aAccum = a.asInstanceOf[FloatingAvgAccumulator]
    val bAccum = b.asInstanceOf[FloatingAvgAccumulator]
    aAccum.count += bAccum.count
    aAccum.sum += bAccum.sum
    a
  }

  /**
    * Convert the intermediate result to the expected aggregation result type
    *
    * @param value the intermediate result. We use a Double container to save
    *         the intermediate result to avoid the overflow by sum operation.
    * @return the result value with the expected aggregation result type
    */
  def resultTypeConvert(value: Double): T
}

/**
  * Built-in Float Avg aggregate function
  */
class FloatAvgAggFunction extends FloatingAvgAggFunction[Float] {
  override def resultTypeConvert(value: Double): Float = value.toFloat
}

/**
  * Built-in Int Double aggregate function
  */
class DoubleAvgAggFunction extends FloatingAvgAggFunction[Double] {
  override def resultTypeConvert(value: Double): Double = value
}

/**
  * Base class for built-in Big Decimal Avg aggregate function
  */
class DecimalAvgAggFunction extends AggregateFunction[BigDecimal] {
  /** The initial accumulator for Big Decimal Avg aggregate function */
  class DecimalAvgAccumulator extends Accumulator {
    var sum: BigDecimal = null
    var count: Long = 0
  }

  override def createAccumulator(): Accumulator = {
    new DecimalAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any) = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[DecimalAvgAccumulator]
      accum.count += 1
      if (accum.sum == null) {
        accum.sum = v
      } else {
        accum.sum = accum.sum.add(v)
      }
    }
  }

  override def getValue(accumulator: Accumulator): BigDecimal = {
    val sum = accumulator.asInstanceOf[DecimalAvgAccumulator].sum
    val count = accumulator.asInstanceOf[DecimalAvgAccumulator].count
    if (sum == null || count == 0) {
      null.asInstanceOf[BigDecimal]
    } else {
      sum.divide(BigDecimal.valueOf(count))
    }
  }

  override def merge(a: Accumulator, b: Accumulator): Accumulator = {
    val aAccum = a.asInstanceOf[DecimalAvgAccumulator]
    val bAccum = b.asInstanceOf[DecimalAvgAccumulator]
    aAccum.count += bAccum.count
    accumulate(a, b.asInstanceOf[DecimalAvgAccumulator].sum)
    a
  }
}
