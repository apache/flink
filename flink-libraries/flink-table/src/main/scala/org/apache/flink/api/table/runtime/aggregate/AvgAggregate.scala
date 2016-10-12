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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.Row
import java.math.BigDecimal
import java.math.BigInteger

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

  override def evaluate(buffer : Row): T = {
    doEvaluate(buffer).asInstanceOf[T]
  }

  override def intermediateDataType = Array(
    BasicTypeInfo.LONG_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO)

  def doPrepare(value: Any, partial: Row): Unit

  def doEvaluate(buffer: Row): Any
}

class ByteAvgAggregate extends IntegralAvgAggregate[Byte] {
  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Byte]
    partial.setField(partialSumIndex, input.toLong)
    partial.setField(partialCountIndex, 1L)
  }

  override def doEvaluate(buffer: Row): Any = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    if (bufferCount == 0L) {
      null
    } else {
      (bufferSum / bufferCount).toByte
    }
  }
}

class ShortAvgAggregate extends IntegralAvgAggregate[Short] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Short]
    partial.setField(partialSumIndex, input.toLong)
    partial.setField(partialCountIndex, 1L)
  }

  override def doEvaluate(buffer: Row): Any = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    if (bufferCount == 0L) {
      null
    } else {
      (bufferSum / bufferCount).toShort
    }
  }
}

class IntAvgAggregate extends IntegralAvgAggregate[Int] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Int]
    partial.setField(partialSumIndex, input.toLong)
    partial.setField(partialCountIndex, 1L)
  }

  override def doEvaluate(buffer: Row): Any = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Long]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    if (bufferCount == 0L) {
      null
    } else {
      (bufferSum / bufferCount).toInt
    }
  }
}

class LongAvgAggregate extends IntegralAvgAggregate[Long] {

  override def intermediateDataType = Array(
    BasicTypeInfo.BIG_INT_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO)

  override def initiate(partial: Row): Unit = {
    partial.setField(partialSumIndex, BigInteger.ZERO)
    partial.setField(partialCountIndex, 0L)
  }

  override def prepare(value: Any, partial: Row): Unit = {
    if (value == null) {
      partial.setField(partialSumIndex, BigInteger.ZERO)
      partial.setField(partialCountIndex, 0L)
    } else {
      doPrepare(value, partial)
    }
  }

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Long]
    partial.setField(partialSumIndex, BigInteger.valueOf(input))
    partial.setField(partialCountIndex, 1L)
  }

  override def merge(partial: Row, buffer: Row): Unit = {
    val partialSum = partial.productElement(partialSumIndex).asInstanceOf[BigInteger]
    val partialCount = partial.productElement(partialCountIndex).asInstanceOf[Long]
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[BigInteger]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    buffer.setField(partialSumIndex, partialSum.add(bufferSum))
    buffer.setField(partialCountIndex, LongMath.checkedAdd(partialCount, bufferCount))
  }

  override def doEvaluate(buffer: Row): Any = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[BigInteger]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    if (bufferCount == 0L) {
      null
    } else {
      bufferSum.divide(BigInteger.valueOf(bufferCount)).longValue()
    }
  }
}

abstract class FloatingAvgAggregate[T: Numeric] extends AvgAggregate[T] {

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

  override def evaluate(buffer : Row): T = {
    doEvaluate(buffer).asInstanceOf[T]
  }

  override def intermediateDataType = Array(
    BasicTypeInfo.DOUBLE_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO)

  def doPrepare(value: Any, partial: Row): Unit

  def doEvaluate(buffer: Row): Any
}

class FloatAvgAggregate extends FloatingAvgAggregate[Float] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Float]
    partial.setField(partialSumIndex, input.toDouble)
    partial.setField(partialCountIndex, 1L)
  }


  override def doEvaluate(buffer: Row): Any = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Double]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    if (bufferCount == 0L) {
      null
    } else {
      (bufferSum / bufferCount).toFloat
    }
  }
}

class DoubleAvgAggregate extends FloatingAvgAggregate[Double] {

  override def doPrepare(value: Any, partial: Row): Unit = {
    val input = value.asInstanceOf[Double]
    partial.setField(partialSumIndex, input)
    partial.setField(partialCountIndex, 1L)
  }

  override def doEvaluate(buffer: Row): Any = {
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[Double]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    if (bufferCount == 0L) {
      null
    } else {
      (bufferSum / bufferCount)
    }
  }
}

class DecimalAvgAggregate extends AvgAggregate[BigDecimal] {

  override def intermediateDataType = Array(
    BasicTypeInfo.BIG_DEC_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO)

  override def initiate(partial: Row): Unit = {
    partial.setField(partialSumIndex, BigDecimal.ZERO)
    partial.setField(partialCountIndex, 0L)
  }

  override def prepare(value: Any, partial: Row): Unit = {
    if (value == null) {
      initiate(partial)
    } else {
      val input = value.asInstanceOf[BigDecimal]
      partial.setField(partialSumIndex, input)
      partial.setField(partialCountIndex, 1L)
    }
  }

  override def merge(partial: Row, buffer: Row): Unit = {
    val partialSum = partial.productElement(partialSumIndex).asInstanceOf[BigDecimal]
    val partialCount = partial.productElement(partialCountIndex).asInstanceOf[Long]
    val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[BigDecimal]
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    buffer.setField(partialSumIndex, partialSum.add(bufferSum))
    buffer.setField(partialCountIndex, LongMath.checkedAdd(partialCount, bufferCount))
  }

  override def evaluate(buffer: Row): BigDecimal = {
    val bufferCount = buffer.productElement(partialCountIndex).asInstanceOf[Long]
    if (bufferCount != 0) {
      val bufferSum = buffer.productElement(partialSumIndex).asInstanceOf[BigDecimal]
      bufferSum.divide(BigDecimal.valueOf(bufferCount))
    } else {
      null.asInstanceOf[BigDecimal]
    }
  }

}
