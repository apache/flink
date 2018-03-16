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

package org.apache.flink.table.dataview

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.ListSerializer
import org.apache.flink.table.api.dataview.ListView

/**
  * [[ListView]] type information.
  *
  * @param elementType element type information
  * @tparam T element type
  */
class  ListViewTypeInfo[T](val elementType: TypeInformation[T])
  extends TypeInformation[ListView[T]] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 1

  override def getTotalFields: Int = 1

  override def getTypeClass: Class[ListView[T]] = classOf[ListView[T]]

  override def isKeyType: Boolean = false

  override def createSerializer(config: ExecutionConfig): TypeSerializer[ListView[T]] = {
    val typeSer = elementType.createSerializer(config)
    new ListViewSerializer[T](new ListSerializer[T](typeSer))
  }

  override def canEqual(obj: scala.Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = 31 * elementType.hashCode

  override def equals(obj: Any): Boolean = canEqual(obj) && {
    obj match {
      case other: ListViewTypeInfo[T] =>
        elementType.equals(other.elementType)
      case _ => false
    }
  }

  override def toString: String = s"ListView<$elementType>"
}
