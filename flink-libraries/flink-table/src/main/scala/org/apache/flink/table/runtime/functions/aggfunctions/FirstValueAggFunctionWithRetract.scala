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
import java.util.{ArrayList => JArrayList, List => JList}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.dataview.{MapView, Order, SortedMapView}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.types.{DataType, DataTypes, DecimalType, InternalType, RowType, TypeConverters}
import org.apache.flink.table.dataformat.{BinaryString, Decimal, GenericRow}
import org.apache.flink.table.typeutils.{BinaryStringTypeInfo, DecimalTypeInfo}

/**
  * Base class for built-in first value with retraction aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class FirstValueWithRetractAggFunction[T]
  extends AggregateFunction[T, GenericRow] {

  def accumulate(acc: GenericRow, value: Any): Unit = {
    if (null != value) {
      val order = System.currentTimeMillis()
      val v = value.asInstanceOf[T]
      val dataMapView = acc.getField(2).asInstanceOf[MapView[T, JList[JLong]]]
      var dataMapList: JList[JLong] = dataMapView.get(v)
      if (null == dataMapList) {
        dataMapList = new JArrayList[JLong]
      }
      dataMapList.add(order)
      dataMapView.put(v, dataMapList)
      accumulate(acc, v, order)
    }
  }

  def accumulate(acc: GenericRow, value: Any, order: Long): Unit = {
    if (null != value) {
      val v = value.asInstanceOf[T]
      val prevOrder = acc.getField(1).asInstanceOf[JLong]
      if (prevOrder == null || prevOrder > order) {
        acc.update(0, v)      // acc.fistValue = v
        acc.update(1, order)  // acc.fistOrder = order
      }
      val sortedDataMapView = acc.getField(3).asInstanceOf[SortedMapView[JLong, JList[T]]]
      var sortedDataMapList: JList[T] = sortedDataMapView.get(order)
      if (null == sortedDataMapList) {
        sortedDataMapList = new JArrayList[T]
      }
      sortedDataMapList.add(v)
      sortedDataMapView.put(order, sortedDataMapList)
    }
  }

  def retract(acc: GenericRow, value: Any): Unit = {
    if (null != value) {
      val v = value.asInstanceOf[T]
      val dataMapView = acc.getField(2).asInstanceOf[MapView[T, JList[JLong]]]
      val dataMapList: JList[JLong] = dataMapView.get(v)
      if (null != dataMapList && dataMapList.size() > 0) {
        val order = dataMapList.get(0)
        dataMapList.remove(0)
        if (dataMapList.isEmpty) {
          dataMapView.remove(v)
        } else {
          dataMapView.put(v, dataMapList)
        }
        retract(acc, v, order)
      }
    }
  }

  def retract(acc: GenericRow, value: Any, order: Long): Unit = {
    if (null != value) {
      val v = value.asInstanceOf[T]
      val sortedDataMapView = acc.getField(3).asInstanceOf[SortedMapView[JLong, JList[T]]]
      val dataList = sortedDataMapView.get(order)
      if (null == dataList) {
        return
      }
      val index = dataList.indexOf(v)
      if (index >= 0) {
        dataList.remove(index)
        if (dataList.isEmpty) {
          sortedDataMapView.remove(order)
        } else {
          sortedDataMapView.put(order, dataList)
        }
      }
      if (v == acc.getField(0)) {  // v == acc.fistValue
        updateValue(acc, sortedDataMapView)
      }
    }
  }

  override def getValue(acc: GenericRow): T = {
    // get firstValue
    acc.getField(0).asInstanceOf[T]
  }

  private def updateValue(
    acc: GenericRow,
    sortedDataMapView: SortedMapView[JLong, JList[T]]): Unit = {
    val startKey = acc.getField(1).asInstanceOf[JLong]
    val itor = sortedDataMapView.tailEntries(startKey).iterator()

    val firstValue = if (!itor.hasNext) {
      acc.update(1, null)
      null.asInstanceOf[T]
    } else {
      val entry = itor.next()
      // set firstOrder
      acc.update(1, entry.getKey)
      entry.getValue.get(0)
    }
    // set firstValue
    acc.update(0, firstValue)
  }

  def resetAccumulator(acc: GenericRow): Unit = {
    acc.update(0, null)
    acc.update(1, null)
    val dataMapView = acc.getField(2).asInstanceOf[MapView[T, JList[JLong]]]
    dataMapView.clear()
    val sortedDataMapView = acc.getField(3).asInstanceOf[SortedMapView[JLong, JList[T]]]
    sortedDataMapView.clear()
  }

  def initDataMap: MapView[T, JList[JLong]] = {
    new MapView[T, JList[JLong]](getInternalValueType, new ListTypeInfo(Types.LONG))
  }

  /**
    * DataTypes.createBaseRowType only accept InternalType, so we add the getInternalValueType
    * interface here
    */
  def getInternalValueType: InternalType

  def getValueType: DataType = getInternalValueType

  override def getResultType(): DataType = getValueType

  override def getUserDefinedInputTypes(signature: Array[Class[_]]): Array[DataType] = {
    if (signature.length == 1) {
      Array[DataType](getValueType)
    } else if (signature.length == 2) {
      Array[DataType](getValueType, DataTypes.LONG)
    } else {
      throw new UnsupportedOperationException
    }
  }

  override def createAccumulator(): GenericRow = {
    // The accumulator schema:
    // firstValue: T
    // fistOrder: JLong
    // dataMap: MapView[T, JList[JLong]]
    // sortedDataMap: SortedMapView[JLong, JList[T]]
    val acc = new GenericRow(4)
    // field_0 is firstValue, field_1 is firstOrder, default are null
    acc.update(2, initDataMap)
    acc.update(3, new SortedMapView(
      Order.ASCENDING,
      DataTypes.LONG,
      new ListTypeInfo(TypeConverters.createExternalTypeInfoFromDataType(getValueType))
    ))
    acc
  }

  override def getAccumulatorType: DataType = {
    val fieldTypes: Array[DataType] = Array(
      getInternalValueType,
      DataTypes.LONG,
      // it will be replaced to MapViewType
      DataTypes.createGenericType(classOf[MapView[_, _]]),
      // it will be replaced to SortedMapViewType
      DataTypes.createGenericType(classOf[SortedMapView[_, _]]))
    val fieldNames = Array("firstValue", "firstOrder", "dataMap", "sortedDataMap")
    new RowType(fieldTypes, fieldNames)
  }
}

class ByteFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[JByte] {
  override def getInternalValueType: InternalType = DataTypes.BYTE
}

class ShortFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[JShort] {
  override def getInternalValueType: InternalType = DataTypes.SHORT
}

class IntFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[JInt] {
  override def getInternalValueType: InternalType = DataTypes.INT
}

class LongFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[JLong] {
  override def getInternalValueType: InternalType = DataTypes.LONG
}

class FloatFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[JFloat] {
  override def getInternalValueType: InternalType = DataTypes.FLOAT
}

class DoubleFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[JDouble] {
  override def getInternalValueType: InternalType = DataTypes.DOUBLE
}

class BooleanFirstValueWithRetractAggFunction extends FirstValueWithRetractAggFunction[JBoolean] {
  override def getInternalValueType: InternalType = DataTypes.BOOLEAN
}

class DecimalFirstValueWithRetractAggFunction(decimalType: DecimalType)
  extends FirstValueWithRetractAggFunction[Decimal] {
  override def getInternalValueType: InternalType = DataTypes.createGenericType(
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale()))
  override def getValueType: DataType =
    DecimalTypeInfo.of(decimalType.precision(), decimalType.scale())
}

class StringFirstValueWithRetractAggFunction
  extends FirstValueWithRetractAggFunction[BinaryString] {
  override def getInternalValueType: InternalType = DataTypes.createGenericType(
    BinaryStringTypeInfo.INSTANCE)
  override def getValueType: DataType = BinaryStringTypeInfo.INSTANCE
}
