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

package org.apache.flink.table.typeutils

import java.util.Objects

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.base.{IntComparator, IntSerializer, LongComparator, LongSerializer}
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo.instantiateComparator
import org.apache.flink.util.Preconditions._

/**
  * TypeInformation for SQL INTERVAL types.
  */
@SerialVersionUID(-1816179424364825258L)
class TimeIntervalTypeInfo[T](
    val clazz: Class[T],
    val serializer: TypeSerializer[T],
    val comparatorClass: Class[_ <: TypeComparator[T]])
  extends TypeInformation[T]
  with AtomicType[T] {

  checkNotNull(clazz)
  checkNotNull(serializer)
  checkNotNull(comparatorClass)

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1

  override def getTypeClass: Class[T] = clazz

  override def isKeyType: Boolean = true

  override def createSerializer(config: ExecutionConfig): TypeSerializer[T] = serializer

  override def createComparator(
      sortOrderAscending: Boolean,
      executionConfig: ExecutionConfig)
    : TypeComparator[T] = instantiateComparator(comparatorClass, sortOrderAscending)

  // ----------------------------------------------------------------------------------------------

  override def hashCode: Int = Objects.hash(clazz, serializer, comparatorClass)

  def canEqual(obj: Any): Boolean = obj.isInstanceOf[TimeIntervalTypeInfo[_]]

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: TimeIntervalTypeInfo[_] =>
        other.canEqual(this) &&
          (this.clazz eq other.clazz) &&
          serializer == other.serializer &&
          (this.comparatorClass eq other.comparatorClass)
      case _ =>
        false
    }
  }

  override def toString: String = s"TimeIntervalTypeInfo(${clazz.getSimpleName})"
}

object TimeIntervalTypeInfo {

  val INTERVAL_MONTHS = new TimeIntervalTypeInfo(
    classOf[java.lang.Integer],
    IntSerializer.INSTANCE,
    classOf[IntComparator])

  val INTERVAL_MILLIS = new TimeIntervalTypeInfo(
    classOf[java.lang.Long],
    LongSerializer.INSTANCE,
    classOf[LongComparator])

  // ----------------------------------------------------------------------------------------------

  private def instantiateComparator[X](
      comparatorClass: Class[_ <: TypeComparator[X]],
      ascendingOrder: java.lang.Boolean)
    : TypeComparator[X] = {
    try {
      val constructor = comparatorClass.getConstructor(java.lang.Boolean.TYPE)
      constructor.newInstance(ascendingOrder)
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Could not initialize comparator ${comparatorClass.getName}", e)
    }
  }

}
