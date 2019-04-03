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

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
  * The [[NullAwareMapSerializer]] is similar to MapSerializer, the only difference is that
  * the [[NullAwareMapSerializer]] can handle null keys.
  */
class NullAwareMapSerializer[K, V](
  val keySerializer: TypeSerializer[K],
  val valueSerializer: TypeSerializer[V])
  extends TypeSerializer[JMap[K, V]]{

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[JMap[K, V]] = {
    val keySerCopy = keySerializer.duplicate()
    val valueSerCopy = valueSerializer.duplicate()
    new NullAwareMapSerializer(keySerCopy, valueSerCopy)
  }

  override def createInstance(): JMap[K, V] = new JHashMap[K, V]()

  override def copy(from: JMap[K, V]): JMap[K, V] = {
    val newMap = createInstance()
    val fromIter = from.entrySet().iterator()
    while (fromIter.hasNext) {
      val entry = fromIter.next()
      val key = entry.getKey
      val value = entry.getValue
      val newKey = if (key == null) {
        null.asInstanceOf[K]
      } else {
        keySerializer.copy(key)
      }
      val newValue = if (value == null) {
        null.asInstanceOf[V]
      } else {
        valueSerializer.copy(value)
      }
      newMap.put(newKey, newValue)
    }
    newMap
  }

  override def copy(from: JMap[K, V], reuse: JMap[K, V]): JMap[K, V] = copy(from)

  override def getLength: Int = -1 // var length

  override def serialize(map: JMap[K, V], target: DataOutputView): Unit = {
    target.writeInt(map.size())

    val iter = map.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry.getKey == null) {
        target.writeBoolean(true)
      } else {
        target.writeBoolean(false)
        keySerializer.serialize(entry.getKey, target)
      }

      if (entry.getValue == null) {
        target.writeBoolean(true)
      } else {
        target.writeBoolean(false)
        valueSerializer.serialize(entry.getValue, target)
      }
    }
  }

  override def deserialize(source: DataInputView): JMap[K, V] = {
    val size = source.readInt()
    val map = createInstance()

    for (i <- 0 until size) {
      val keyIsNull = source.readBoolean()
      val key = if (keyIsNull) {
        null.asInstanceOf[K]
      } else {
        keySerializer.deserialize(source)
      }

      val valueIsNull = source.readBoolean()
      val value = if (valueIsNull) {
        null.asInstanceOf[V]
      } else {
        valueSerializer.deserialize(source)
      }
      map.put(key, value)
    }

    map
  }

  override def deserialize(
    reuse: JMap[K, V],
    source: DataInputView): JMap[K, V] = deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val size = source.readInt()
    target.writeInt(size)

    for (i <- 0 until size) {
      val keyIsNull = source.readBoolean()
      target.writeBoolean(keyIsNull)
      if (!keyIsNull) {
        keySerializer.copy(source, target)
      }

      val valueIsNull = source.readBoolean()
      target.writeBoolean(valueIsNull)
      if (!valueIsNull) {
        valueSerializer.copy(source, target)
      }
    }
  }

  override def toString: String = {
    "NullAwareMapSerializer{keySerializer=" + keySerializer +
      ", valueSerializer=" + valueSerializer + "}"
  }

  override def equals(o: Any): Boolean = {
    if (this eq o.asInstanceOf[AnyRef]) return true
    if (o == null || (getClass ne o.getClass)) return false
    val that = o.asInstanceOf[NullAwareMapSerializer[K, V]]
    this.keySerializer == that.keySerializer && this.valueSerializer == that.valueSerializer
  }

  override def hashCode: Int = {
    var result = keySerializer.hashCode
    result = 31 * result + valueSerializer.hashCode
    result
  }

  override def snapshotConfiguration() = throw new UnsupportedOperationException
}
