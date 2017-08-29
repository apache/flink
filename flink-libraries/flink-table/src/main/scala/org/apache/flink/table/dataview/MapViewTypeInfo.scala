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
import org.apache.flink.api.common.typeutils.base.MapSerializer
import org.apache.flink.table.api.dataview.MapView

/**
  * [[MapView]] type information.
  *
  * @param keyType key type information
  * @param valueType value type information
  * @tparam K key type
  * @tparam V value type
  */
class  MapViewTypeInfo[K, V](
    val keyType: TypeInformation[K],
    val valueType: TypeInformation[V])
  extends TypeInformation[MapView[K, V]] {

  override def isBasicType = false

  override def isTupleType = false

  override def getArity = 1

  override def getTotalFields = 1

  override def getTypeClass: Class[MapView[K, V]] = classOf[MapView[K, V]]

  override def isKeyType: Boolean = false

  override def createSerializer(config: ExecutionConfig): TypeSerializer[MapView[K, V]] = {
    val keySer = keyType.createSerializer(config)
    val valueSer = valueType.createSerializer(config)
    new MapViewSerializer[K, V](new MapSerializer[K, V](keySer, valueSer))
  }

  override def canEqual(obj: scala.Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = 31 * keyType.hashCode + valueType.hashCode

  override def equals(obj: Any): Boolean = canEqual(obj) && {
    obj match {
      case other: MapViewTypeInfo[_, _] =>
        keyType.equals(other.keyType) &&
          valueType.equals(other.valueType)
      case _ => false
    }
  }

  override def toString: String = s"MapView<$keyType, $valueType>"
}
