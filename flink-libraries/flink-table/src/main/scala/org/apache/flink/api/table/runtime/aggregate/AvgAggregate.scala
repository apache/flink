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

import com.google.common.math.LongMath
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.table.Row

abstract class AvgAggregate[T] extends Aggregate[T] {
  protected var partialSumIndex: Int = _
  protected var partialCountIndex: Int = _

  override def supportPartial: Boolean = true

  override def setAggOffsetInRow(aggOffset: Int): Unit = {
    partialSumIndex = aggOffset
    partialCountIndex = aggOffset + 1
  }
}

abstract class IntegralAvgAggregate[T] extends AvgAggregate[T] {
  private final val intermediateType = Array(SqlTypeName.BIGINT, SqlTypeName.BIGINT)


  override def initiate(partial: Row): Unit = {
    partial.setField(partialSumIndex, 0L)
    partial.setField(partialCountIndex, 0L)
  }

  override def prepare(value: Any, partial: Row): Unit = {
    if (value == null) {
      partial.setField(partialSumIndex, 0L)
      partial.setField(partialCountIndex, 0L)
    } else {
      doPrepare(value, partial)
    }
  }

  override def merge(partial: Row, buffer: Row): Unit = {
    val partialSum = partial.productElement(partialSumIndex).asInstanceOf[Long]
    val partialCount = partial.productElement(partialCountIndex).asInstanceOf[Long]
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    buffer.setField(partialSumIndex, LongMath.checkedAdd(partialSum, bufferSum))
    buffer.setField(partialCountIndex, LongMath.checkedAdd(partialCount, bufferCount))
  }

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }

  def doPrepare(value: Any, partial: Row): Unit
}

class ByteAvgAggregate extends IntegralAvgAggregate[Byte] {
  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Byte]
    partial.setField(partialSumIndex, input.toLong)
    partial.setField(partialCountIndex, 1L)
  }

  override def evaluate(buffer: Row): Byte = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    (bufferSum / bufferCount).toByte
  }
}

class ShortAvgAggregate extends IntegralAvgAggregate[Short] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Short]
    partial.setField(partialSumIndex, input.toLong)
    partial.setField(partialCountIndex, 1L)
  }

  override def evaluate(buffer: Row): Short = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    (bufferSum / bufferCount).toShort
  }
}

class IntAvgAggregate extends IntegralAvgAggregate[Int] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Int]
    partial.setField(partialSumIndex, input.toLong)
    partial.setField(partialCountIndex, 1L)
  }

  override def evaluate(buffer: Row): Int = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    (bufferSum / bufferCount).toInt
  }
}

class LongAvgAggregate extends IntegralAvgAggregate[Long] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Long]
    partial.setField(partialSumIndex, input)
    partial.setField(partialCountIndex, 1L)
  }

  override def evaluate(buffer: Row): Long = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    (bufferSum / bufferCount)
  }
}

abstract class FloatingAvgAggregate[T: Numeric] extends AvgAggregate[T] {
  private val partialType = Array(SqlTypeName.DOUBLE, SqlTypeName.BIGINT)

  override def initiate(partial: Row): Unit = {
    partial.setField(partialSumIndex, 0D)
    partial.setField(partialCountIndex, 0L)
  }

  override def prepare(value: Any, partial: Row): Unit = {
    if (value == null) {
      partial.setField(partialSumIndex, 0D)
      partial.setField(partialCountIndex, 0L)
    } else {
      doPrepare(value, partial)
    }
  }

  override def merge(partial: Row, buffer: Row): Unit = {
    val partialSum = partial.productElement(partialSumIndex).asInstanceOf[Double]
    val partialCount = partial.productElement(partialCountIndex).asInstanceOf[Long]
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Double]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]

    buffer.setField(partialSumIndex, partialSum + bufferSum)
    buffer.setField(partialCountIndex, partialCount + bufferCount)
  }

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }

  def doPrepare(value: Any, partial: Row): Unit
}

class FloatAvgAggregate extends FloatingAvgAggregate[Float] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Float]
    partial.setField(partialSumIndex, input.toDouble)
    partial.setField(partialCountIndex, 1L)
  }


  override def evaluate(buffer: Row): Float = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Double]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    (bufferSum / bufferCount).toFloat
  }
}

class DoubleAvgAggregate extends FloatingAvgAggregate[Double] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Double]
    partial.setField(partialSumIndex, input)
    partial.setField(partialCountIndex, 1L)
  }

  override def evaluate(buffer: Row): Double = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Double]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    (bufferSum / bufferCount)
  }
}
