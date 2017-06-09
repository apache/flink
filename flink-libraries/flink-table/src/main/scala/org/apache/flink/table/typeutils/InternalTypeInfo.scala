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
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.util.Preconditions._

/**
  * TypeInformation for internal types of the Table API that are for translation purposes only
  * and should not be contained in final plan.
  */
@SerialVersionUID(-13064574364925255L)
abstract class InternalTypeInfo[T](val clazz: Class[T])
  extends TypeInformation[T]
  with AtomicType[T] {

  checkNotNull(clazz)

  override def isBasicType: Boolean =
    throw new UnsupportedOperationException("This type is for internal use only.")

  override def isTupleType: Boolean =
    throw new UnsupportedOperationException("This type is for internal use only.")

  override def getArity: Int =
    throw new UnsupportedOperationException("This type is for internal use only.")

  override def getTotalFields: Int =
    throw new UnsupportedOperationException("This type is for internal use only.")

  override def getTypeClass: Class[T] = clazz

  override def isKeyType: Boolean =
    throw new UnsupportedOperationException("This type is for internal use only.")

  override def createSerializer(config: ExecutionConfig): TypeSerializer[T] =
    throw new UnsupportedOperationException("This type is for internal use only.")

  override def createComparator(
      sortOrderAscending: Boolean,
      executionConfig: ExecutionConfig)
    : TypeComparator[T] =
    throw new UnsupportedOperationException("This type is for internal use only.")

  // ----------------------------------------------------------------------------------------------

  override def hashCode: Int = Objects.hash(clazz)

  def canEqual(obj: Any): Boolean

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: InternalTypeInfo[_] =>
        other.canEqual(this) && (this.clazz eq other.clazz)
      case _ =>
        false
    }
  }

  override def toString: String = s"InternalTypeInfo"
}
