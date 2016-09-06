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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.Row

abstract class SumAggregate[T: Numeric]
  extends Aggregate[T] {

  private val numeric = implicitly[Numeric[T]]
  protected var sumIndex: Int = _

  override def initiate(partial: Row): Unit = {
    partial.setField(sumIndex, null)
  }

  override def merge(partial1: Row, buffer: Row): Unit = {
    val partialValue = partial1.productElement(sumIndex).asInstanceOf[T]
    if (partialValue != null) {
      val bufferValue = buffer.productElement(sumIndex).asInstanceOf[T]
      if (bufferValue != null) {
        buffer.setField(sumIndex, numeric.plus(partialValue, bufferValue))
      } else {
        buffer.setField(sumIndex, partialValue)
      }
    }
  }

  override def evaluate(buffer: Row): T = {
    buffer.productElement(sumIndex).asInstanceOf[T]
  }

  override def prepare(value: Any, partial: Row): Unit = {
    if (value == null) {
      initiate(partial)
    } else {
      val input = value.asInstanceOf[T]
      partial.setField(sumIndex, input)
    }
  }

  override def supportPartial: Boolean = true

  override def setAggOffsetInRow(aggOffset: Int): Unit = {
    sumIndex = aggOffset
  }
}

class ByteSumAggregate extends SumAggregate[Byte] {
  override def intermediateDataType = Array(BasicTypeInfo.BYTE_TYPE_INFO)
}

class ShortSumAggregate extends SumAggregate[Short] {
  override def intermediateDataType = Array(BasicTypeInfo.SHORT_TYPE_INFO)
}

class IntSumAggregate extends SumAggregate[Int] {
  override def intermediateDataType = Array(BasicTypeInfo.INT_TYPE_INFO)
}

class LongSumAggregate extends SumAggregate[Long] {
  override def intermediateDataType = Array(BasicTypeInfo.LONG_TYPE_INFO)
}

class FloatSumAggregate extends SumAggregate[Float] {
  override def intermediateDataType = Array(BasicTypeInfo.FLOAT_TYPE_INFO)
}

class DoubleSumAggregate extends SumAggregate[Double] {
  override def intermediateDataType = Array(BasicTypeInfo.DOUBLE_TYPE_INFO)
}

class DecimalSumAggregate extends Aggregate[BigDecimal] {

  protected var sumIndex: Int = _

  override def intermediateDataType = Array(BasicTypeInfo.BIG_DEC_TYPE_INFO)

  override def initiate(partial: Row): Unit = {
    partial.setField(sumIndex, null)
  }

  override def merge(partial1: Row, buffer: Row): Unit = {
    val partialValue = partial1.productElement(sumIndex).asInstanceOf[BigDecimal]
    if (partialValue != null) {
      val bufferValue = buffer.productElement(sumIndex).asInstanceOf[BigDecimal]
      if (bufferValue != null) {
        buffer.setField(sumIndex, partialValue.add(bufferValue))
      } else {
        buffer.setField(sumIndex, partialValue)
      }
    }
  }

  override def evaluate(buffer: Row): BigDecimal = {
    buffer.productElement(sumIndex).asInstanceOf[BigDecimal]
  }

  override def prepare(value: Any, partial: Row): Unit = {
    if (value == null) {
      initiate(partial)
    } else {
      val input = value.asInstanceOf[BigDecimal]
      partial.setField(sumIndex, input)
    }
  }

  override def supportPartial: Boolean = true

  override def setAggOffsetInRow(aggOffset: Int): Unit = {
    sumIndex = aggOffset
  }
}
