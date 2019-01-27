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
package org.apache.flink.table.runtime.functions.aggfunctions

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, DecimalType, InternalType, RowType}
import org.apache.flink.table.dataformat.{BinaryString, Decimal, GenericRow}
import org.apache.flink.table.typeutils.{BinaryStringTypeInfo, DecimalTypeInfo}

/**
  * The initial accumulator for first value aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class FirstValueAggFunction[T] extends AggregateFunction[T, GenericRow] {

  def accumulate(acc: GenericRow, value: Any): Unit = {
    if (null != value && acc.getField(1).asInstanceOf[JLong] == JLong.MAX_VALUE) {
      acc.update(0, value.asInstanceOf[T])
      acc.update(1, System.currentTimeMillis())
    }
  }

  def accumulate(acc: GenericRow, value: Any, order: Long): Unit = {
    if (null != value && acc.getField(1).asInstanceOf[JLong] > order) {
      acc.update(0, value.asInstanceOf[T])
      acc.update(1, order)
    }
  }

  override def getValue(acc: GenericRow): T = {
    acc.getField(0).asInstanceOf[T]
  }

  def resetAccumulator(acc: GenericRow): Unit = {
    acc.update(0, getInitValue)
    acc.update(1, JLong.MAX_VALUE)
  }

  def getInitValue: T = {
    null.asInstanceOf[T]
  }

  override def createAccumulator(): GenericRow = {
    val acc = new GenericRow(2)
    acc.update(0, getInitValue)
    acc.update(1, JLong.MAX_VALUE)
    acc
  }

  override def isDeterministic: Boolean = false

  /**
    * DataTypes.createBaseRowType only accept InternalType, so we add the getInternalValueType
    * interface here
    */
  def getInternalValueType: InternalType

  def getValueType: DataType = getInternalValueType

  override def getResultType: DataType = getValueType

  override def getAccumulatorType: DataType = {
    val fieldTypes: Array[DataType] = Array(getInternalValueType, DataTypes.LONG)
    val fieldNames = Array("value", "time")
    new RowType(fieldTypes, fieldNames)
  }

  override def getUserDefinedInputTypes(signature: Array[Class[_]]): Array[DataType] = {
    if (signature.length == 1) {
      Array[DataType](getValueType)
    } else if (signature.length == 2) {
      Array[DataType](getValueType, DataTypes.LONG)
    } else {
      throw new UnsupportedOperationException
    }
  }
}

class ByteFirstValueAggFunction extends FirstValueAggFunction[JByte] {
  override def getInternalValueType: InternalType = DataTypes.BYTE
}

class ShortFirstValueAggFunction extends FirstValueAggFunction[JShort] {
  override def getInternalValueType: InternalType = DataTypes.SHORT
}

class IntFirstValueAggFunction extends FirstValueAggFunction[JInt] {
  override def getInternalValueType: InternalType = DataTypes.INT
}

class LongFirstValueAggFunction extends FirstValueAggFunction[JLong] {
  override def getInternalValueType: InternalType = DataTypes.LONG
}

class FloatFirstValueAggFunction extends FirstValueAggFunction[JFloat] {
  override def getInternalValueType: InternalType = DataTypes.FLOAT
}

class DoubleFirstValueAggFunction extends FirstValueAggFunction[JDouble] {
  override def getInternalValueType: InternalType = DataTypes.DOUBLE
}

class BooleanFirstValueAggFunction extends FirstValueAggFunction[JBoolean] {
  override def getInternalValueType: InternalType = DataTypes.BOOLEAN
}

class DecimalFirstValueAggFunction(decimalType: DecimalType)
  extends FirstValueAggFunction[Decimal] {
  override def getInternalValueType: InternalType = DataTypes.createGenericType(
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale()))
  override def getValueType: DataType =
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale())
}

class StringFirstValueAggFunction extends FirstValueAggFunction[BinaryString] {
  override def getInternalValueType: InternalType = DataTypes.createGenericType(
    BinaryStringTypeInfo.INSTANCE)
  override def getValueType: DataType = BinaryStringTypeInfo.INSTANCE

  override def accumulate(acc: GenericRow, value: Any): Unit = {
    if (null != value) {
      super.accumulate(acc, value.asInstanceOf[BinaryString].copy())
    }
  }

  override def accumulate(acc: GenericRow, value: Any, order: Long): Unit = {
    // just ignore nulls values and orders
    if (null != value) {
      super.accumulate(acc, value.asInstanceOf[BinaryString].copy(), order)
    }
  }
}
