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
package org.apache.flink.table.functions.aggfunctions

import java.math.{BigDecimal, BigInteger}
import java.util.{List => JList}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/** The initial accumulator for Integral Avg aggregate function */
class IntegralAvgAccumulator extends JTuple2[Long, Long] with Accumulator {
  f0 = 0L //sum
  f1 = 0L //count
}

/**
  * Base class for built-in Integral Avg aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class IntegralAvgAggFunction[T] extends AggregateFunction[T] {

  override def createAccumulator(): Accumulator = {
    new IntegralAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].longValue()
      val accum = accumulator.asInstanceOf[IntegralAvgAccumulator]
      accum.f0 += v
      accum.f1 += 1L
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].longValue()
      val accum = accumulator.asInstanceOf[IntegralAvgAccumulator]
      accum.f0 -= v
      accum.f1 -= 1L
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val accum = accumulator.asInstanceOf[IntegralAvgAccumulator]
    if (accum.f1 == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(accum.f0 / accum.f1)
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[IntegralAvgAccumulator]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[IntegralAvgAccumulator]
      ret.f1 += a.f1
      ret.f0 += a.f0
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[IntegralAvgAccumulator].f0 = 0L
    accumulator.asInstanceOf[IntegralAvgAccumulator].f1 = 0L
  }

  override def getAccumulatorType: TypeInformation[_] = {
    new TupleTypeInfo(
      new IntegralAvgAccumulator().getClass,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO)
  }

  /**
    * Convert the intermediate result to the expected aggregation result type
    *
    * @param value the intermediate result. We use a Long container to save
    *              the intermediate result to avoid the overflow by sum operation.
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

/** The initial accumulator for Big Integral Avg aggregate function */
class BigIntegralAvgAccumulator
  extends JTuple2[BigInteger, Long] with Accumulator {
  f0 = BigInteger.ZERO //sum
  f1 = 0L //count
}

/**
  * Base Class for Built-in Big Integral Avg aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class BigIntegralAvgAggFunction[T] extends AggregateFunction[T] {

  override def createAccumulator(): Accumulator = {
    new BigIntegralAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Long]
      val a = accumulator.asInstanceOf[BigIntegralAvgAccumulator]
      a.f0 = a.f0.add(BigInteger.valueOf(v))
      a.f1 += 1L
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Long]
      val a = accumulator.asInstanceOf[BigIntegralAvgAccumulator]
      a.f0 = a.f0.subtract(BigInteger.valueOf(v))
      a.f1 -= 1L
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val a = accumulator.asInstanceOf[BigIntegralAvgAccumulator]
    if (a.f1 == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(a.f0.divide(BigInteger.valueOf(a.f1)))
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[BigIntegralAvgAccumulator]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[BigIntegralAvgAccumulator]
      ret.f1 += a.f1
      ret.f0 = ret.f0.add(a.f0)
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[BigIntegralAvgAccumulator].f0 = BigInteger.ZERO
    accumulator.asInstanceOf[BigIntegralAvgAccumulator].f1 = 0
  }

  override def getAccumulatorType: TypeInformation[_] = {
    new TupleTypeInfo(
      new BigIntegralAvgAccumulator().getClass,
      BasicTypeInfo.BIG_INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO)
  }

  /**
    * Convert the intermediate result to the expected aggregation result type
    *
    * @param value the intermediate result. We use a BigInteger container to
    *              save the intermediate result to avoid the overflow by sum
    *              operation.
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

/** The initial accumulator for Floating Avg aggregate function */
class FloatingAvgAccumulator extends JTuple2[Double, Long] with Accumulator {
  f0 = 0 //sum
  f1 = 0L //count
}

/**
  * Base class for built-in Floating Avg aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class FloatingAvgAggFunction[T] extends AggregateFunction[T] {

  override def createAccumulator(): Accumulator = {
    new FloatingAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].doubleValue()
      val accum = accumulator.asInstanceOf[FloatingAvgAccumulator]
      accum.f0 += v
      accum.f1 += 1L
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].doubleValue()
      val accum = accumulator.asInstanceOf[FloatingAvgAccumulator]
      accum.f0 -= v
      accum.f1 -= 1L
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val accum = accumulator.asInstanceOf[FloatingAvgAccumulator]
    if (accum.f1 == 0) {
      null.asInstanceOf[T]
    } else {
      resultTypeConvert(accum.f0 / accum.f1)
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[FloatingAvgAccumulator]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[FloatingAvgAccumulator]
      ret.f1 += a.f1
      ret.f0 += a.f0
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[FloatingAvgAccumulator].f0 = 0
    accumulator.asInstanceOf[FloatingAvgAccumulator].f1 = 0L
  }

  override def getAccumulatorType: TypeInformation[_] = {
    new TupleTypeInfo(
      new FloatingAvgAccumulator().getClass,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO)
  }

  /**
    * Convert the intermediate result to the expected aggregation result type
    *
    * @param value the intermediate result. We use a Double container to save
    *              the intermediate result to avoid the overflow by sum operation.
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

/** The initial accumulator for Big Decimal Avg aggregate function */
class DecimalAvgAccumulator
  extends JTuple2[BigDecimal, Long] with Accumulator {
  f0 = BigDecimal.ZERO //sum
  f1 = 0L //count
}

/**
  * Base class for built-in Big Decimal Avg aggregate function
  */
class DecimalAvgAggFunction extends AggregateFunction[BigDecimal] {

  override def createAccumulator(): Accumulator = {
    new DecimalAvgAccumulator
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[DecimalAvgAccumulator]
      accum.f0 = accum.f0.add(v)
      accum.f1 += 1L
    }
  }

  override def retract(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[DecimalAvgAccumulator]
      accum.f0 = accum.f0.subtract(v)
      accum.f1 -= 1L
    }
  }

  override def getValue(accumulator: Accumulator): BigDecimal = {
    val a = accumulator.asInstanceOf[DecimalAvgAccumulator]
    if (a.f1 == 0) {
      null.asInstanceOf[BigDecimal]
    } else {
      a.f0.divide(BigDecimal.valueOf(a.f1))
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0).asInstanceOf[DecimalAvgAccumulator]
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[DecimalAvgAccumulator]
      ret.f0 = ret.f0.add(a.f0)
      ret.f1 += a.f1
      i += 1
    }
    ret
  }

  override def resetAccumulator(accumulator: Accumulator): Unit = {
    accumulator.asInstanceOf[DecimalAvgAccumulator].f0 = BigDecimal.ZERO
    accumulator.asInstanceOf[DecimalAvgAccumulator].f1 = 0L
  }

  override def getAccumulatorType: TypeInformation[_] = {
    new TupleTypeInfo(
      new DecimalAvgAccumulator().getClass,
      BasicTypeInfo.BIG_DEC_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO)
  }
}
