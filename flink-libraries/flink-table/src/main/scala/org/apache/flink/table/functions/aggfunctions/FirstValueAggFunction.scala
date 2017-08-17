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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/**
  * The initial accumulator for first value aggregate function
  *
  * @tparam T the type for the aggregation result
  */
class FirstValueAccumulator[T] extends JTuple1[T]

abstract class FirstValueAggFunction[T] extends AggregateFunction[T, FirstValueAccumulator[T]] {

  override def createAccumulator(): FirstValueAccumulator[T] = {
    val acc = new FirstValueAccumulator[T]
    acc.f0 = getInitValue
    acc
  }

  def accumulate(acc: FirstValueAccumulator[T], value: Any): Unit = {
    if (null == acc.f0) {
      acc.f0 = value.asInstanceOf[T]
    }
  }

  override def getValue(acc: FirstValueAccumulator[T]): T = {
    acc.f0
  }

  def resetAccumulator(acc: FirstValueAccumulator[T]): Unit = {
    acc.f0 = getInitValue
  }

  def getInitValue: T = {
    null.asInstanceOf[T]
  }

  override def isDeterministic: Boolean = false

  override def requiresOver: Boolean = true

  def getValueTypeInfo: TypeInformation[T]

  override def getResultType(): TypeInformation[T] = getValueTypeInfo

  override def getAccumulatorType(): TypeInformation[FirstValueAccumulator[T]] = {
    new TupleTypeInfo(classOf[FirstValueAccumulator[T]], getValueTypeInfo)
  }

}

class StringFirstValueAggFunction extends FirstValueAggFunction[String] {
  override def getValueTypeInfo = BasicTypeInfo.STRING_TYPE_INFO
}

class ByteFirstValueAggFunction extends FirstValueAggFunction[Byte] {
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO.asInstanceOf[TypeInformation[Byte]]
}

class ShortFirstValueAggFunction extends FirstValueAggFunction[Short] {
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO.asInstanceOf[TypeInformation[Short]]
}

class IntFirstValueAggFunction extends FirstValueAggFunction[Int] {
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO.asInstanceOf[TypeInformation[Int]]
}

class LongFirstValueAggFunction extends FirstValueAggFunction[Long] {
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
}

class FloatFirstValueAggFunction extends FirstValueAggFunction[Float] {
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO.asInstanceOf[TypeInformation[Float]]
}

class DoubleFirstValueAggFunction extends FirstValueAggFunction[Double] {
  override def getValueTypeInfo =
    BasicTypeInfo.DOUBLE_TYPE_INFO.asInstanceOf[TypeInformation[Double]]
}

class BooleanFirstValueAggFunction extends FirstValueAggFunction[Boolean] {
  override def getValueTypeInfo =
    BasicTypeInfo.BOOLEAN_TYPE_INFO.asInstanceOf[TypeInformation[Boolean]]
}

class DecimalFirstValueAggFunction extends FirstValueAggFunction[BigDecimal] {
  override def getValueTypeInfo =
    BasicTypeInfo.BIG_DEC_TYPE_INFO.asInstanceOf[TypeInformation[BigDecimal]]
}