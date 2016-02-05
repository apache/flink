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
package org.apache.flink.api.table.typeinfo

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType.TypeComparatorBuilder
import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

import scala.collection.mutable.ArrayBuffer
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.table.Row

/**
 * TypeInformation for [[Row]].
 */
class RowTypeInfo(fieldTypes: Seq[TypeInformation[_]])
  extends CaseClassTypeInfo[Row](
    classOf[Row],
    Array(),
    fieldTypes,
    for (i <- fieldTypes.indices) yield "f" + i)
{

  /**
   * Temporary variable for directly passing orders to comparators.
   */
  var comparatorOrders: Option[Array[Boolean]] = None

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[Row] = {
    val fieldSerializers: Array[TypeSerializer[Any]] = new Array[TypeSerializer[Any]](getArity)
    for (i <- 0 until getArity) {
      fieldSerializers(i) = this.types(i).createSerializer(executionConfig)
        .asInstanceOf[TypeSerializer[Any]]
    }

    new RowSerializer(fieldSerializers)
  }

  override def createComparator(
      logicalKeyFields: Array[Int],
      orders: Array[Boolean],
      logicalFieldOffset: Int,
      config: ExecutionConfig)
    : TypeComparator[Row] = {
    // store the order information for the builder
    comparatorOrders = Some(orders)
    val comparator = super.createComparator(logicalKeyFields, orders, logicalFieldOffset, config)
    comparatorOrders = None
    comparator
  }

  override def createTypeComparatorBuilder(): TypeComparatorBuilder[Row] = {
    new RowTypeComparatorBuilder(comparatorOrders.getOrElse(
      throw new IllegalStateException("Cannot create comparator builder without orders.")))
  }

  private class RowTypeComparatorBuilder(
      comparatorOrders: Array[Boolean])
    extends TypeComparatorBuilder[Row] {

    val fieldComparators: ArrayBuffer[TypeComparator[_]] = new ArrayBuffer[TypeComparator[_]]()
    val logicalKeyFields: ArrayBuffer[Int] = new ArrayBuffer[Int]()

    override def initializeTypeComparatorBuilder(size: Int): Unit = {
      fieldComparators.sizeHint(size)
      logicalKeyFields.sizeHint(size)
    }

    override def addComparatorField(fieldId: Int, comparator: TypeComparator[_]): Unit = {
      fieldComparators += comparator
      logicalKeyFields += fieldId
    }

    override def createTypeComparator(config: ExecutionConfig): TypeComparator[Row] = {
      val maxIndex = logicalKeyFields.max

      new RowComparator(
        logicalKeyFields.toArray,
        fieldComparators.toArray.asInstanceOf[Array[TypeComparator[Any]]],
        types.take(maxIndex + 1).map(_.createSerializer(config).asInstanceOf[TypeSerializer[Any]]),
        comparatorOrders
      )
    }
  }
}

