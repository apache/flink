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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Iterable => JIterable, Long => JLong, Short => JShort}
import org.apache.flink.table.api.dataview.{MapView, Order, SortedMapView}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, DecimalType, InternalType, RowType}
import org.apache.flink.table.dataformat.{BinaryString, Decimal, GenericRow}
import org.apache.flink.table.typeutils.{BinaryStringTypeInfo, DecimalTypeInfo}

/**
  * Base class for built-in Min with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class MinWithRetractAggFunction[T](implicit ord: Ordering[T])
  extends AggregateFunction[T, GenericRow] {

  override def createAccumulator(): GenericRow = {
    // acc schema:
    // min: T
    // map: SortedMapView[T, JLong]
    // retractMap: MapView[T, JLong]
    val acc = new GenericRow(3)
    acc.update(0, getInitValue) // min
    // store the count for each value
    val map = new SortedMapView(Order.ASCENDING, getValueType, DataTypes.LONG)
      .asInstanceOf[SortedMapView[T, JLong]]
    acc.update(1, map)
    val retractMap = new MapView(getValueType, DataTypes.LONG)
      .asInstanceOf[MapView[T, JLong]]
    acc.update(2, retractMap)
    acc
  }

  def accumulate(acc: GenericRow, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val retractMap = acc.getField(2).asInstanceOf[MapView[T, JLong]]
      // check whether the value is retracted before
      var retractCount = retractMap.get(v)
      if (retractCount != null) {
        retractCount -= 1
        if (retractCount == 0) {
          retractMap.remove(v)
        } else {
          retractMap.put(v, retractCount)
        }
        return
      }

      val map = acc.getField(1).asInstanceOf[SortedMapView[T, JLong]]
      val iterator = map.iterator
      val min = acc.getField(0).asInstanceOf[T]
      if (iterator == null || !iterator.hasNext || (ord.compare(min, v) > 0)) {
        // update min to acc
        acc.update(0, v)
      }
      val count = map.get(v)
      if (count == null) {
        map.put(v, 1L)
      } else {
        map.put(v, count + 1L)
      }
    }
  }

  def retract(acc: GenericRow, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      val map = acc.getField(1).asInstanceOf[SortedMapView[T, JLong]]
      var count = map.get(v)

      if (count == null) {
        // the retract message is arrived before add message, store it and return
        val retractMap = acc.getField(2).asInstanceOf[MapView[T, JLong]]
        val retractCount = retractMap.get(v)
        if (retractCount == null) {
          retractMap.put(v, 1L)
        } else {
          retractMap.put(v, retractCount + 1)
        }
        return
      }

      count -= 1L
      if (count == 0) {
        //remove the key v from the map if the number of appearance of the value v is 0
        map.remove(v)
        val min = acc.getField(0).asInstanceOf[T]
        if (ord.compare(min, v) == 0) {
          updateMin(acc, map)
        }
      } else {
        map.put(v, count)
      }
    }

  }

  override def getValue(acc: GenericRow): T = {
    val map = acc.getField(1).asInstanceOf[SortedMapView[T, JLong]]
    val iterator = map.iterator
    if (iterator != null && iterator.hasNext) {
      acc.getField(0).asInstanceOf[T] // min
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: GenericRow, its: JIterable[GenericRow]): Unit = {
    val map = acc.getField(1).asInstanceOf[SortedMapView[T, JLong]]
    val retractMap = acc.getField(2).asInstanceOf[MapView[T, JLong]]
    var hasMin: Boolean = {
      val iterator = map.iterator
      iterator != null && iterator.hasNext
    }
    var iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      val otherMap = a.getField(1).asInstanceOf[SortedMapView[T, JLong]]
      val curAccItor = otherMap.iterator
      if (curAccItor != null && curAccItor.hasNext) {
        val accMin = acc.getField(0).asInstanceOf[T]
        val otherMin = a.getField(0).asInstanceOf[T]
        // set min element
        if (!hasMin || ord.compare(accMin, otherMin) > 0) {
          // update otherMin to acc
          acc.update(0, otherMin)
          hasMin = true
        }
        // merge the count for each key
        while (curAccItor.hasNext) {
          val entry = curAccItor.next()
          val key = entry.getKey
          val count = entry.getValue
          val oldCnt = map.get(key)
          if (oldCnt != null) {
            map.put(key, oldCnt + count)
          } else {
            map.put(key, count)
          }
        }
      }
    }

    var minChanged = false
    iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      // merge retract map
      val otherRetractMap = a.getField(2).asInstanceOf[MapView[T, JLong]]
      val retractIter = otherRetractMap.iterator
      if (retractIter != null) {
        while (retractIter.hasNext) {
          val entry = retractIter.next()
          val key = entry.getKey
          val retractCount = entry.getValue

          val count = map.get(key)
          if (count == null) {
            if (retractMap.contains(key)) {
              retractMap.put(key, retractMap.get(key) + retractCount)
            } else {
              retractMap.put(key, retractCount)
            }
          } else if (count > retractCount) {
            map.put(key, count - retractCount)
          } else {
            map.remove(key)
            // if key is the current min value, we have to iterate the map to find the 2nd smallest
            // value to replace key as the min value
            val min = acc.getField(0).asInstanceOf[T]
            if (ord.compare(min, key) == 0) {
              minChanged = true
            }

            if (count < retractCount) {
              retractMap.put(key, retractCount - count)
            }
          }
        }
      }
    }

    if (minChanged) {
      updateMin(acc, map)
    }
  }

  private def updateMin(acc: GenericRow, map: SortedMapView[T, JLong]): Unit = {
    // if the total count is 0, we could just simply set the f0(min) to the initial value
    val minEntry = map.firstEntry
    if (minEntry != null) {
      acc.update(0, minEntry.getKey)
    } else {
      acc.update(0, getInitValue)
    }
  }

  def resetAccumulator(acc: GenericRow): Unit = {
    acc.update(0, getInitValue) // min
    val map = acc.getField(1).asInstanceOf[SortedMapView[T, JLong]]
    val retractMap = acc.getField(2).asInstanceOf[MapView[T, JLong]]
    map.clear()
    retractMap.clear()
  }

  def getInitValue: T

  /**
    * DataTypes.createBaseRowType only accept InternalType, so we add the getInternalValueType
    * interface here
    */
  def getInternalValueType: InternalType

  def getValueType: DataType = getInternalValueType

  override def getResultType: DataType = getValueType

  override def getUserDefinedInputTypes(signature: Array[Class[_]]): Array[DataType] = {
    if (signature.length == 1) {
      Array[DataType](getValueType)
    } else {
      throw new UnsupportedOperationException
    }
  }

  override def getAccumulatorType: DataType = {
    val fieldTypes: Array[DataType] = Array(
      getInternalValueType,
      // it will be replaced to SortedMapViewType
      DataTypes.createGenericType(classOf[SortedMapView[_, _]]),
      // it will be replaced to MapViewType
      DataTypes.createGenericType(classOf[MapView[_, _]]))
    val fieldNames = Array("min", "map", "retractMap")
    new RowType(fieldTypes, fieldNames)
  }
}

/**
  * Built-in Byte Min with retraction aggregate function
  */
class ByteMinWithRetractAggFunction extends MinWithRetractAggFunction[JByte] {
  override def getInitValue: JByte = 0.toByte
  override def getInternalValueType: InternalType = DataTypes.BYTE
}

/**
  * Built-in Short Min with retraction aggregate function
  */
class ShortMinWithRetractAggFunction extends MinWithRetractAggFunction[JShort] {
  override def getInitValue: JShort = 0.toShort
  override def getInternalValueType: InternalType = DataTypes.SHORT
}

/**
  * Built-in Int Min with retraction aggregate function
  */
class IntMinWithRetractAggFunction extends MinWithRetractAggFunction[JInt] {
  override def getInitValue: JInt = 0
  override def getInternalValueType: InternalType = DataTypes.INT
}

/**
  * Built-in Long Min with retraction aggregate function
  */
class LongMinWithRetractAggFunction extends MinWithRetractAggFunction[JLong] {
  override def getInitValue: JLong = 0L
  override def getInternalValueType: InternalType = DataTypes.LONG
}

/**
  * Built-in Float Min with retraction aggregate function
  */
class FloatMinWithRetractAggFunction extends MinWithRetractAggFunction[JFloat] {
  override def getInitValue: JFloat = 0.0f
  override def getInternalValueType: InternalType = DataTypes.FLOAT
}

/**
  * Built-in Double Min with retraction aggregate function
  */
class DoubleMinWithRetractAggFunction extends MinWithRetractAggFunction[JDouble] {
  override def getInitValue: JDouble = 0.0d
  override def getInternalValueType: InternalType = DataTypes.DOUBLE
}

/**
  * Built-in Boolean Min with retraction aggregate function
  */
class BooleanMinWithRetractAggFunction extends MinWithRetractAggFunction[JBoolean] {
  override def getInitValue: JBoolean = false
  override def getInternalValueType: InternalType = DataTypes.BOOLEAN
}

/**
  * Built-in Big Decimal Min with retraction aggregate function
  */
class DecimalMinWithRetractAggFunction(decimalType: DecimalType)
  extends MinWithRetractAggFunction[Decimal] {
  override def getInitValue: Decimal = Decimal.zero(decimalType.precision(), decimalType.scale())
  override def getInternalValueType: InternalType = DataTypes.createGenericType(
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale()))
  override def getValueType: DataType =
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale())
}

/**
  * Built-in String Min with retraction aggregate function
  */
class StringMinWithRetractAggFunction extends MinWithRetractAggFunction[BinaryString] {
  override def getInitValue: BinaryString = BinaryString.fromString("")
  override def getInternalValueType: InternalType = BinaryStringTypeInfo.INSTANCE.toInternalType
  override def getValueType: DataType = BinaryStringTypeInfo.INSTANCE
}
