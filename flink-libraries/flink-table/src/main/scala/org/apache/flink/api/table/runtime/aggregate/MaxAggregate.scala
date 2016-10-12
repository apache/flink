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

abstract class MaxAggregate[T](implicit ord: Ordering[T]) extends Aggregate[T] {

  protected var maxIndex = -1

  /**
   * Initiate the intermediate aggregate value in Row.
   *
   * @param intermediate The intermediate aggregate row to initiate.
   */
  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(maxIndex, null)
  }

  /**
   * Accessed in MapFunction, prepare the input of partial aggregate.
   *
   * @param value
   * @param intermediate
   */
  override def prepare(value: Any, intermediate: Row): Unit = {
    if (value == null) {
      initiate(intermediate)
    } else {
      intermediate.setField(maxIndex, value)
    }
  }

  /**
   * Accessed in CombineFunction and GroupReduceFunction, merge partial
   * aggregate result into aggregate buffer.
   *
   * @param intermediate
   * @param buffer
   */
  override def merge(intermediate: Row, buffer: Row): Unit = {
    val partialValue = intermediate.productElement(maxIndex).asInstanceOf[T]
    if (partialValue != null) {
      val bufferValue = buffer.productElement(maxIndex).asInstanceOf[T]
      if (bufferValue != null) {
        val max : T = if (ord.compare(partialValue, bufferValue) > 0) partialValue else bufferValue
        buffer.setField(maxIndex, max)
      } else {
        buffer.setField(maxIndex, partialValue)
      }
    }
  }

  /**
   * Return the final aggregated result based on aggregate buffer.
   *
   * @param buffer
   * @return
   */
  override def evaluate(buffer: Row): T = {
    buffer.productElement(maxIndex).asInstanceOf[T]
  }

  override def supportPartial: Boolean = true

  override def setAggOffsetInRow(aggOffset: Int): Unit = {
    maxIndex = aggOffset
  }
}

class ByteMaxAggregate extends MaxAggregate[Byte] {

  override def intermediateDataType = Array(BasicTypeInfo.BYTE_TYPE_INFO)

}

class ShortMaxAggregate extends MaxAggregate[Short] {

  override def intermediateDataType = Array(BasicTypeInfo.SHORT_TYPE_INFO)

}

class IntMaxAggregate extends MaxAggregate[Int] {

  override def intermediateDataType = Array(BasicTypeInfo.INT_TYPE_INFO)

}

class LongMaxAggregate extends MaxAggregate[Long] {

  override def intermediateDataType = Array(BasicTypeInfo.LONG_TYPE_INFO)

}

class FloatMaxAggregate extends MaxAggregate[Float] {

  override def intermediateDataType = Array(BasicTypeInfo.FLOAT_TYPE_INFO)

}

class DoubleMaxAggregate extends MaxAggregate[Double] {

  override def intermediateDataType = Array(BasicTypeInfo.DOUBLE_TYPE_INFO)

}

class BooleanMaxAggregate extends MaxAggregate[Boolean] {

  override def intermediateDataType = Array(BasicTypeInfo.BOOLEAN_TYPE_INFO)

}

class DecimalMaxAggregate extends Aggregate[BigDecimal] {

  protected var minIndex: Int = _

  override def intermediateDataType = Array(BasicTypeInfo.BIG_DEC_TYPE_INFO)

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(minIndex, null)
  }

  override def prepare(value: Any, partial: Row): Unit = {
    if (value == null) {
      initiate(partial)
    } else {
      partial.setField(minIndex, value)
    }
  }

  override def merge(partial: Row, buffer: Row): Unit = {
    val partialValue = partial.productElement(minIndex).asInstanceOf[BigDecimal]
    if (partialValue != null) {
      val bufferValue = buffer.productElement(minIndex).asInstanceOf[BigDecimal]
      if (bufferValue != null) {
        val min = if (partialValue.compareTo(bufferValue) > 0) partialValue else bufferValue
        buffer.setField(minIndex, min)
      } else {
        buffer.setField(minIndex, partialValue)
      }
    }
  }

  override def evaluate(buffer: Row): BigDecimal = {
    buffer.productElement(minIndex).asInstanceOf[BigDecimal]
  }

  override def supportPartial: Boolean = true

  override def setAggOffsetInRow(aggOffset: Int): Unit = {
    minIndex = aggOffset
  }
}
