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

abstract class SumAggregate[T: Numeric]
  extends Aggregate[T] {

  private val numeric = implicitly[Numeric[T]]
  protected var sumIndex: Int = _

  override def initiate(partial: Row): Unit = {
    partial.setField(sumIndex, numeric.zero)
  }

  override def merge(partial1: Row, buffer: Row): Unit = {
    val partialValue = partial1.productElement(sumIndex).asInstanceOf[T]
    val bufferValue = buffer.productElement(sumIndex).asInstanceOf[T]
    buffer.setField(sumIndex, numeric.plus(partialValue, bufferValue))
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
  private val partialType = Array(SqlTypeName.TINYINT)

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }
}

class ShortSumAggregate extends SumAggregate[Short] {
  private val partialType = Array(SqlTypeName.SMALLINT)

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }
}

class IntSumAggregate extends SumAggregate[Int] {
  private val partialType = Array(SqlTypeName.INTEGER)

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }
}

class LongSumAggregate extends SumAggregate[Long] {
  private val partialType = Array(SqlTypeName.BIGINT)

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }
}

class FloatSumAggregate extends SumAggregate[Float] {
  private val partialType = Array(SqlTypeName.FLOAT)

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }
}

class DoubleSumAggregate extends SumAggregate[Double] {
  private val partialType = Array(SqlTypeName.DOUBLE)

  override def intermediateDataType: Array[SqlTypeName] = {
    partialType
  }
}
