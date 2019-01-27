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
import org.apache.flink.table.runtime.functions.aggfunctions.ConcatAggFunction._
import org.apache.flink.table.dataformat.{BinaryString, GenericRow}
import org.apache.flink.table.typeutils.BinaryStringTypeInfo

/**
  * built-in concat aggregate function
  */
class ConcatAggFunction extends AggregateFunction[BinaryString, GenericRow] {

  def accumulate(acc: GenericRow, value: BinaryString): Unit = {
    if (null != value) {
      val list = acc.getField(0).asInstanceOf[ListView[BinaryString]]
      list.add(value)
    }
  }

  def retract(acc: GenericRow, value: BinaryString): Unit = {
    if (null != value) {
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
      BinaryString.concatWs(lineDelimiter, list.get)
    }
  }

  def merge(acc: GenericRow, its: JIterable[GenericRow]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val otherAcc = iter.next()
      val thisList = acc.getField(0).asInstanceOf[ListView[BinaryString]]
      val otherList = otherAcc.getField(0).asInstanceOf[ListView[BinaryString]]
      val accList = otherList.get
      if (accList != null) {
        val listIter = accList.iterator()
        while (listIter.hasNext) {
          thisList.add(listIter.next())
        }
      }

      val otherRetractList = otherAcc.getField(1).asInstanceOf[ListView[BinaryString]]
      val thisRetractList = acc.getField(1).asInstanceOf[ListView[BinaryString]]
      val retractList = otherRetractList.get
      if (retractList != null) {
        val retractListIter = retractList.iterator()
        var buffer: JList[BinaryString] = null
        if (retractListIter.hasNext) {
          buffer = thisList.get.asInstanceOf[JList[BinaryString]]
        }
        var listChanged = false
        while (retractListIter.hasNext) {
          val element = retractListIter.next()
          if (buffer != null && buffer.remove(element)) {
            listChanged = true
          } else {
            thisRetractList.add(element)
          }
        }

        if (listChanged) {
          thisList.clear()
          thisList.addAll(buffer)
        }
      }
    }
  }

  override def createAccumulator(): GenericRow = {
    val acc = new GenericRow(2)
    // list
    acc.update(0, new ListView[BinaryString](BinaryStringTypeInfo.INSTANCE))
    // retractList
    acc.update(1, new ListView[BinaryString](BinaryStringTypeInfo.INSTANCE))
    acc
  }

  def resetAccumulator(acc: GenericRow): Unit = {
    val list = acc.getField(0).asInstanceOf[ListView[BinaryString]]
    val retractList = acc.getField(1).asInstanceOf[ListView[BinaryString]]
    list.clear()
    retractList.clear()
  }

  override def getAccumulatorType: DataType = {
    val fieldTypes: Array[DataType] = Array(
      // it will be replaced to ListViewType
      DataTypes.createGenericType(classOf[ListView[_]]),
      // it will be replaced to ListViewType
      DataTypes.createGenericType(classOf[ListView[_]]))
    val fieldNames = Array("list", "retractList")
    new RowType(fieldTypes, fieldNames)
  }
}

object ConcatAggFunction {
  val lineDelimiter: BinaryString = BinaryString.fromString("\n")
}
