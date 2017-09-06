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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.dataview.{ListView, MapView}
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for first value with retraction aggregate function */
class FirstValueWithRetractAccumulator[T] {
  var data: ListView[T] = _
  var retractedData: MapView[T, Byte] = _

  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[FirstValueWithRetractAccumulator[T]]) {
      val target = obj.asInstanceOf[FirstValueWithRetractAccumulator[T]]
      if (target.data.equals(this.data) && target.retractedData.equals(this.retractedData)) {
        return true
      }
    }
    false
  }
}

/**
  * Base class for built-in first value with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class FirstValueWithRetractAggFunction[T]
  extends AggregateFunction[T, FirstValueWithRetractAccumulator[T]] {

  override def createAccumulator(): FirstValueWithRetractAccumulator[T] = {
    val acc = new FirstValueWithRetractAccumulator[T]
    acc.data = new ListView[T]
    acc.retractedData = new MapView[T, Byte]
    acc
  }

  def accumulate(acc: FirstValueWithRetractAccumulator[T], value: Any): Unit = {
    if (null != value) {
      val v = value.asInstanceOf[T]
      acc.data.add(v)
    }
  }

  def retract(acc: FirstValueWithRetractAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      acc.retractedData.put(v, Byte.MinValue)
    }
  }

  override def getValue(acc: FirstValueWithRetractAccumulator[T]): T = {
    val it = acc.data.get.iterator()
    while (it.hasNext) {
      val value = it.next()
      if (!acc.retractedData.contains(value)) {
        return value
      }
    }
    resetAccumulator(acc)
    null.asInstanceOf[T]
  }

  def resetAccumulator(acc: FirstValueWithRetractAccumulator[T]): Unit = {
    acc.data.clear()
    acc.retractedData.clear()
  }

  override def getAccumulatorType: TypeInformation[FirstValueWithRetractAccumulator[T]] = {
    TypeInformation.of(classOf[FirstValueWithRetractAccumulator[T]])
  }

  def getValueTypeInfo: TypeInformation[_]
}


class StringFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[String] {
  override def getValueTypeInfo = TypeInformation.of(classOf[String])
}

class ByteFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[Byte] {
  override def getValueTypeInfo = TypeInformation.of(classOf[Byte])
}

class ShortFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[Short] {
  override def getValueTypeInfo = TypeInformation.of(classOf[Short])
}

class IntFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[Int] {
  override def getValueTypeInfo = TypeInformation.of(classOf[Int])
}

class LongFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[Long] {
  override def getValueTypeInfo = TypeInformation.of(classOf[Long])
}

class FloatFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[Float] {
  override def getValueTypeInfo = TypeInformation.of(classOf[Float])
}

class DoubleFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[Double] {
  override def getValueTypeInfo = TypeInformation.of(classOf[Double])
}

class BooleanFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[Boolean] {
  override def getValueTypeInfo = TypeInformation.of(classOf[Boolean])
}

class DecimalFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[BigDecimal] {
  override def getValueTypeInfo = TypeInformation.of(classOf[BigDecimal])
}
