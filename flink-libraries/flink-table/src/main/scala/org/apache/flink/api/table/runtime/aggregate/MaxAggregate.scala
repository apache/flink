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

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.table.Row

abstract class MaxAggregate[T: Numeric] extends Aggregate[T] {

  private val numeric = implicitly[Numeric[T]]
  protected var maxIndex = -1

  /**
   * Accessed in MapFunction, prepare the input of partial aggregate.
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
   * @param intermediate
   * @param buffer
   */
  override def merge(intermediate: Row, buffer: Row): Unit = {
    val partialValue = intermediate.productElement(maxIndex).asInstanceOf[T]
    val bufferValue = buffer.productElement(maxIndex).asInstanceOf[T]
    buffer.setField(maxIndex, numeric.max(partialValue, bufferValue))
  }

  /**
   * Return the final aggregated result based on aggregate buffer.
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
  private val intermediateType = Array(SqlTypeName.TINYINT)

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(maxIndex, Byte.MinValue)
  }
}

class ShortMaxAggregate extends MaxAggregate[Short] {
  private val intermediateType = Array(SqlTypeName.SMALLINT)

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(maxIndex, Short.MinValue)
  }
}

class IntMaxAggregate extends MaxAggregate[Int] {
  private val intermediateType = Array(SqlTypeName.INTEGER)

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(maxIndex, Int.MinValue)
  }
}

class LongMaxAggregate extends MaxAggregate[Long] {
  private val intermediateType = Array(SqlTypeName.BIGINT)

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(maxIndex, Long.MinValue)
  }
}

class FloatMaxAggregate extends MaxAggregate[Float] {
  private val intermediateType = Array(SqlTypeName.FLOAT)

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(maxIndex, Float.MinValue)
  }
}

class DoubleMaxAggregate extends MaxAggregate[Double] {
  private val intermediateType = Array(SqlTypeName.DOUBLE)

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }

  override def initiate(intermediate: Row): Unit = {
    intermediate.setField(maxIndex, Double.MinValue)
  }
}
