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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo

/**
  * Base class for built-in Sum0 aggregate function. If all values are null, 0 is returned.
  *
  * @tparam T the type for the aggregation result
  */
abstract class Sum0AggFunction[T: Numeric] extends SumAggFunction[T] {

  override def getValue(acc: SumAccumulator[T]): T = {
    if (acc.f1) {
      acc.f0
    } else {
      0.asInstanceOf[T]
    }
  }
}

/**
  * Built-in Byte Sum0 aggregate function
  */
class ByteSum0AggFunction extends Sum0AggFunction[Byte] {
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Sum0 aggregate function
  */
class ShortSum0AggFunction extends Sum0AggFunction[Short] {
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Sum0 aggregate function
  */
class IntSum0AggFunction extends Sum0AggFunction[Int] {
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Sum0 aggregate function
  */
class LongSum0AggFunction extends Sum0AggFunction[Long] {
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Sum0 aggregate function
  */
class FloatSum0AggFunction extends Sum0AggFunction[Float] {
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Sum0 aggregate function
  */
class DoubleSum0AggFunction extends Sum0AggFunction[Double] {
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Big Decimal Sum0 aggregate function
  */
class DecimalSum0AggFunction extends DecimalSumAggFunction {

  override def getValue(acc: DecimalSumAccumulator): BigDecimal = {
    if (acc.f1) {
      acc.f0
    } else {
      BigDecimal.ZERO
    }
  }
}
