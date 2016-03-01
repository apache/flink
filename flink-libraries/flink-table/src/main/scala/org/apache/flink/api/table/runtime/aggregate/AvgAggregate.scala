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

abstract class IntegralAvgAggregate[T] extends Aggregate[T] {
  private final val intermediateType = Array(SqlTypeName.BIGINT, SqlTypeName.BIGINT)
  
  override def initiate(partial: Row): Unit = {
    partial.setField(aggOffsetInRow, 0L)
    partial.setField(aggOffsetInRow + 1, 0L)
  }

  override def merge(partial: Row, buffer: Row): Unit = {
    val partialSum = partial.productElement(aggOffsetInRow).asInstanceOf[Long]
    val partialCount = partial.productElement(aggOffsetInRow + 1).asInstanceOf[Long]
    val bufferSum = buffer.productElement(aggOffsetInRow).asInstanceOf[Long]
    val bufferCount = buffer.productElement(aggOffsetInRow + 1).asInstanceOf[Long]
    buffer.setField(aggOffsetInRow, LongMath.checkedAdd(partialSum, bufferSum))
    buffer.setField(aggOffsetInRow + 1, LongMath.checkedAdd(partialCount, bufferCount))
  }

  override def intermediateDataType: Array[SqlTypeName] = {
    intermediateType
  }
}

class ByteAvgAggregate extends IntegralAvgAggregate[Byte] {
  override def prepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Byte]
    partial.setField(aggOffsetInRow, input.toLong)
    partial.setField(aggOffsetInRow + 1, 1L)
  }

  override def evaluate(buffer: Row): Byte = {
    val bufferSum = buffer.productElement(aggOffsetInRow).asInstanceOf[Long]
    val bufferCount = buffer.productElement(aggOffsetInRow + 1).asInstanceOf[Long]
    (bufferSum / bufferCount).toByte
  }
}

class ShortAvgAggregate extends IntegralAvgAggregate[Short] {

  override def prepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Short]
    partial.setField(aggOffsetInRow, input.toLong)
    partial.setField(aggOffsetInRow + 1, 1L)
  }

  override def evaluate(buffer: Row): Short = {
    val bufferSum = buffer.productElement(aggOffsetInRow).asInstanceOf[Long]
    val bufferCount = buffer.productElement(aggOffsetInRow + 1).asInstanceOf[Long]
    (bufferSum / bufferCount).toShort
  }
}

class IntAvgAggregate extends IntegralAvgAggregate[Int] {

  override def prepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Int]
    partial.setField(aggOffsetInRow, input.toLong)
    partial.setField(aggOffsetInRow + 1, 1L)
  }

  override def evaluate(buffer: Row): Int = {
    val bufferSum = buffer.productElement(aggOffsetInRow).asInstanceOf[Long]
    val bufferCount = buffer.productElement(aggOffsetInRow + 1).asInstanceOf[Long]
    (bufferSum / bufferCount).toInt
  }
}

class LongAvgAggregate extends IntegralAvgAggregate[Long] {

  override def prepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Long]
    partial.setField(aggOffsetInRow, input)
    partial.setField(aggOffsetInRow + 1, 1L)
  }

  override def evaluate(buffer: Row): Long = {
    val bufferSum = buffer.productElement(aggOffsetInRow).asInstanceOf[Long]
    val bufferCount = buffer.productElement(aggOffsetInRow + 1).asInstanceOf[Long]
    (bufferSum / bufferCount)
  }
}

abstract class FloatingAvgAggregate[T: Numeric] extends Aggregate[T] {
  private val partialType = Array(SqlTypeName.DOUBLE, SqlTypeName.BIGINT)

  override def initiate(partial: Row): Unit = {
    partial.setField(aggOffsetInRow, 0D)
    partial.setField(aggOffsetInRow + 1, 0L)
  }

  override def merge(partial: Row, buffer: Row): Unit = {
    val partialAvg = partial.productElement(aggOffsetInRow).asInstanceOf[Double]
    val partialCount = partial.productElement(aggOffsetInRow + 1).asInstanceOf[Long]
    val bufferAvg = buffer.productElement(aggOffsetInRow).asInstanceOf[Double]
    val bufferCount = buffer.productElement(aggOffsetInRow + 1).asInstanceOf[Long]

    val updatedCount = partialCount + bufferCount
    val updatedAvg = bufferAvg + (partialAvg - bufferAvg) / updatedCount
    buffer.setField(aggOffsetInRow, updatedAvg)
    buffer.setField(aggOffsetInRow + 1, updatedCount)
  }

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }
}

class FloatAvgAggregate extends FloatingAvgAggregate[Float] {

  override def prepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Float]
    partial.setField(aggOffsetInRow, input.toDouble)
    partial.setField(aggOffsetInRow + 1, 1L)
  }


  override def evaluate(buffer: Row): Float = {
    buffer.productElement(aggOffsetInRow).asInstanceOf[Double].toFloat
  }
}

class DoubleAvgAggregate extends FloatingAvgAggregate[Double] {

  override def prepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Double]
    partial.setField(aggOffsetInRow, input)
    partial.setField(aggOffsetInRow + 1, 1L)
  }


  override def evaluate(buffer: Row): Double = {
    buffer.productElement(aggOffsetInRow).asInstanceOf[Double]
  }
}
