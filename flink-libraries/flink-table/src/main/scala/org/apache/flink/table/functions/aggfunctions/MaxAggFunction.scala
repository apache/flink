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

import java.math.BigDecimal
import java.util.{List => JList}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}

/** The initial accumulator for Max aggregate function */
class MaxAccumulator[T] extends JTuple2[T, Boolean] with Accumulator {
  f0 = 0.asInstanceOf[T] //max
  f1 = false
}

/**
  * Base class for built-in Max aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MaxAggFunction[T](implicit ord: Ordering[T]) extends AggregateFunction[T] {

  override def createAccumulator(): Accumulator = {
    new MaxAccumulator[T]
  }

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val a = accumulator.asInstanceOf[MaxAccumulator[T]]
      if (!a.f1 || ord.compare(a.f0, v) < 0) {
        a.f0 = v
        a.f1 = true
      }
    }
  }

  override def getValue(accumulator: Accumulator): T = {
    val a = accumulator.asInstanceOf[MaxAccumulator[T]]
    if (a.f1) {
      a.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  override def merge(accumulators: JList[Accumulator]): Accumulator = {
    val ret = accumulators.get(0)
    var i: Int = 1
    while (i < accumulators.size()) {
      val a = accumulators.get(i).asInstanceOf[MaxAccumulator[T]]
      if (a.f1) {
        accumulate(ret.asInstanceOf[MaxAccumulator[T]], a.f0)
      }
      i += 1
    }
    ret
  }

  override def getAccumulatorType(): TypeInformation[_] = {
    new TupleTypeInfo(
      new MaxAccumulator[T].getClass,
      getValueTypeInfo,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte Max aggregate function
  */
class ByteMaxAggFunction extends MaxAggFunction[Byte] {
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Max aggregate function
  */
class ShortMaxAggFunction extends MaxAggFunction[Short] {
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Max aggregate function
  */
class IntMaxAggFunction extends MaxAggFunction[Int] {
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Max aggregate function
  */
class LongMaxAggFunction extends MaxAggFunction[Long] {
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Max aggregate function
  */
class FloatMaxAggFunction extends MaxAggFunction[Float] {
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Max aggregate function
  */
class DoubleMaxAggFunction extends MaxAggFunction[Double] {
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Boolean Max aggregate function
  */
class BooleanMaxAggFunction extends MaxAggFunction[Boolean] {
  override def getValueTypeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO
}

/**
  * Built-in Big Decimal Max aggregate function
  */
class DecimalMaxAggFunction extends MaxAggFunction[BigDecimal] {

  override def accumulate(accumulator: Accumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      val accum = accumulator.asInstanceOf[MaxAccumulator[BigDecimal]]
      if (!accum.f1 || accum.f0.compareTo(v) < 0) {
        accum.f0 = v
        accum.f1 = true
      }
    }
  }

  override def getValueTypeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO
}
