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

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}
import org.apache.flink.table.api.dataview.ListView
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType}
import org.apache.flink.table.dataformat.{BinaryString, GenericRow}
import org.apache.flink.table.typeutils.BinaryStringTypeInfo

/**
  * built-in concat with separator aggregate function
  */
class ConcatWsAggFunction extends AggregateFunction[BinaryString, GenericRow] {

  override def createAccumulator(): GenericRow = {
    val acc = new GenericRow(3)
    // list
    acc.update(0, new ListView[BinaryString](BinaryStringTypeInfo.INSTANCE))
    // retractList
    acc.update(1, new ListView[BinaryString](BinaryStringTypeInfo.INSTANCE))
    // delimiter
    acc.update(2, BinaryString.fromString("\n"))
    acc
  }

  def accumulate(
      acc: GenericRow,
      lineDelimiter: BinaryString,
      value: BinaryString): Unit = {
    // ignore null value
    if (null != value) {
      acc.update(2, lineDelimiter)
      val list = acc.getField(0).asInstanceOf[ListView[BinaryString]]
      list.add(value)
    }
  }

  def retract(acc: GenericRow, lineDelimiter: BinaryString, value: BinaryString): Unit = {
    if (null != value) {
      acc.update(2, lineDelimiter)
      val list = acc.getField(0).asInstanceOf[ListView[BinaryString]]
      if (!list.remove(value)) {
        val retractList = acc.getField(1).asInstanceOf[ListView[BinaryString]]
        retractList.add(value)
      }
    }
  }

  override def getValue(acc: GenericRow): BinaryString = {
    val list = acc.getField(0).asInstanceOf[ListView[BinaryString]]
    val accList = list.get
    // UserFacingListState will always return a non null list
    if (accList == null || !accList.iterator().hasNext) {
      // return null when the list is empty
      null
    } else {
      val delimiter = acc.getField(2).asInstanceOf[BinaryString]
      val list = acc.getField(0).asInstanceOf[ListView[BinaryString]]
      BinaryString.concatWs(delimiter, list.get)
    }
  }

  def merge(acc: GenericRow, its: JIterable[GenericRow]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val otherAcc = iter.next()
      val thisAccList = acc.getField(0).asInstanceOf[ListView[BinaryString]]
      val otherAccList = otherAcc.getField(0).asInstanceOf[ListView[BinaryString]]
      val accList = otherAccList.get
      if (accList != null) {
        val listIter = accList.iterator()
        acc.update(2, otherAcc.getField(2)) // acc.delimiter = otherAcc.delimiter
        while (listIter.hasNext) {
          thisAccList.add(listIter.next())
        }
      }

      val thisAccRetractList = acc.getField(1).asInstanceOf[ListView[BinaryString]]
      val otherAccRetractList = otherAcc.getField(1).asInstanceOf[ListView[BinaryString]]
      val retractList = otherAccRetractList.get
      if (retractList != null) {
        val retractListIter = retractList.iterator()
        var buffer: JList[BinaryString] = null
        if (retractListIter.hasNext) {
          buffer = thisAccList.get.asInstanceOf[JList[BinaryString]]
        }
        var listChanged = false
        while (retractListIter.hasNext) {
          val element = retractListIter.next()
          if (buffer != null && buffer.remove(element)) {
            listChanged = true
          } else {
            thisAccRetractList.add(element)
          }
        }

        if (listChanged) {
          thisAccList.clear()
          thisAccList.addAll(buffer)
        }
      }
    }
  }

  def resetAccumulator(acc: GenericRow): Unit = {
    acc.update(0, BinaryString.fromString("\n"))
    val list = acc.getField(0).asInstanceOf[ListView[BinaryString]]
    list.clear()
  }

  override def getAccumulatorType: DataType = {
    val fieldTypes: Array[DataType] = Array(
      // it will be replaced to ListViewType
      DataTypes.createGenericType(classOf[ListView[_]]),
      // it will be replaced to ListViewType
      DataTypes.createGenericType(classOf[ListView[_]]),
      DataTypes.STRING)
    val fieldNames = Array("list", "retractList", "delimiter")
    new RowType(fieldTypes, fieldNames)
  }
}

