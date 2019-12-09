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

package org.apache.flink.table.runtime.types

import java.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{CompositeType, TypeComparator, TypeSerializer}
import org.apache.flink.api.common.typeutils.CompositeType.{FlatFieldDescriptor, TypeComparatorBuilder}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row

class CRowTypeInfo(val rowType: RowTypeInfo) extends CompositeType[CRow](classOf[CRow]) {

  override def getFieldNames: Array[String] = rowType.getFieldNames

  override def getFieldIndex(fieldName: String): Int = rowType.getFieldIndex(fieldName)

  override def getTypeAt[X](fieldExpression: String): TypeInformation[X] =
    rowType.getTypeAt(fieldExpression)

  override def getTypeAt[X](pos: Int): TypeInformation[X] =
    rowType.getTypeAt(pos)

  override def getFlatFields(
      fieldExpression: String,
      offset: Int,
      result: util.List[FlatFieldDescriptor]): Unit =
    rowType.getFlatFields(fieldExpression, offset, result)

  override def isBasicType: Boolean = rowType.isBasicType

  override def isTupleType: Boolean = rowType.isTupleType

  override def getArity: Int = rowType.getArity

  override def getTotalFields: Int = rowType.getTotalFields

  override def createSerializer(config: ExecutionConfig): TypeSerializer[CRow] =
    new CRowSerializer(rowType.createSerializer(config))

  // not implemented because we override createComparator
  override protected def createTypeComparatorBuilder(): TypeComparatorBuilder[CRow] = null

  override def createComparator(
      logicalKeyFields: Array[Int],
      orders: Array[Boolean],
      logicalFieldOffset: Int,
      config: ExecutionConfig): TypeComparator[CRow] = {

    val rowComparator = rowType.createComparator(
      logicalKeyFields,
      orders,
      logicalFieldOffset,
      config)

    new CRowComparator(rowComparator)
  }

  override def equals(obj: scala.Any): Boolean = {
    if (this.canEqual(obj)) {
      rowType.equals(obj.asInstanceOf[CRowTypeInfo].rowType)
    } else {
      false
    }
  }

  override def canEqual(obj: scala.Any): Boolean = obj.isInstanceOf[CRowTypeInfo]

}

object CRowTypeInfo {

  def apply(rowType: TypeInformation[Row]): CRowTypeInfo = {
    rowType match {
      case r: RowTypeInfo => new CRowTypeInfo(r)
    }
  }

}

