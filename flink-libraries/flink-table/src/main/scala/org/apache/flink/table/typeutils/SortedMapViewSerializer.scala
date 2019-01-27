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

import java.util

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.common.typeutils.base.{MapSerializerConfigSnapshot, SortedMapSerializer}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.table.api.dataview.{Order, SortedMapView}

/**
  * A serializer for [[SortedMapView]]. The serializer relies on a key serializer and a value
  * serializer for the serialization of the map's key-value pairs.
  *
  * The serialization format for the map is as follows: four bytes for the length of the map,
  * followed by the serialized representation of each key-value pair. To allow null values,
  * each value is prefixed by a null marker.
  *
  * @param sortedMapSerializer Map serializer.
  * @tparam K The type of the keys in the map.
  * @tparam V The type of the values in the map.
  */
@Internal
class SortedMapViewSerializer[K, V](val sortedMapSerializer: SortedMapSerializer[K, V])
  extends TypeSerializer[SortedMapView[K, V]] {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[SortedMapView[K, V]] =
    new SortedMapViewSerializer[K, V](
      sortedMapSerializer.duplicate().asInstanceOf[SortedMapSerializer[K, V]])

  override def createInstance(): SortedMapView[K, V] = {
    new SortedMapView[K, V](Order.ASCENDING, null, null)
  }

  override def copy(from: SortedMapView[K, V]): SortedMapView[K, V] = {
    new SortedMapView[K, V](Order.ASCENDING, null, null,
      sortedMapSerializer.copy(from.map).asInstanceOf[util.TreeMap[K, V]])
  }

  override def copy(from: SortedMapView[K, V], reuse: SortedMapView[K, V]): SortedMapView[K, V] =
    copy(from)

  override def getLength: Int = -1  // var length

  override def serialize(record: SortedMapView[K, V], target: DataOutputView): Unit = {
    sortedMapSerializer.serialize(record.map, target)
  }

  override def deserialize(source: DataInputView): SortedMapView[K, V] =
    new SortedMapView[K, V](Order.ASCENDING, null, null,
      sortedMapSerializer.deserialize(source).asInstanceOf[util.TreeMap[K, V]])

  override def deserialize(reuse: SortedMapView[K, V], source: DataInputView): SortedMapView[K, V]
    = deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    sortedMapSerializer.copy(source, target)

  override def canEqual(obj: Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = sortedMapSerializer.hashCode()

  override def equals(obj: Any): Boolean = canEqual(this) &&
    sortedMapSerializer.equals(obj.asInstanceOf[SortedMapViewSerializer[_, _]].sortedMapSerializer)

  override def snapshotConfiguration(): TypeSerializerConfigSnapshot =
    sortedMapSerializer.snapshotConfiguration()

  // copy and modified from MapSerializer.ensureCompatibility
  override def ensureCompatibility(configSnapshot: TypeSerializerConfigSnapshot)
  : CompatibilityResult[SortedMapView[K, V]] = {

    configSnapshot match {
      case snapshot: MapSerializerConfigSnapshot[_, _] =>
        val previousKvSerializersAndConfigs = snapshot.getNestedSerializersAndConfigs

        val keyCompatResult = CompatibilityUtil.resolveCompatibilityResult(
          previousKvSerializersAndConfigs.get(0).f0,
          classOf[UnloadableDummyTypeSerializer[_]],
          previousKvSerializersAndConfigs.get(0).f1,
          sortedMapSerializer.getKeySerializer)

        val valueCompatResult = CompatibilityUtil.resolveCompatibilityResult(
          previousKvSerializersAndConfigs.get(1).f0,
          classOf[UnloadableDummyTypeSerializer[_]],
          previousKvSerializersAndConfigs.get(1).f1,
          sortedMapSerializer.getValueSerializer)

        if (!keyCompatResult.isRequiresMigration && !valueCompatResult.isRequiresMigration) {
          CompatibilityResult.compatible[SortedMapView[K, V]]
        } else if (keyCompatResult.getConvertDeserializer != null
            && valueCompatResult.getConvertDeserializer != null) {
          CompatibilityResult.requiresMigration[SortedMapView[K, V]](
            new SortedMapViewSerializer[K, V](
              new SortedMapSerializer[K, V](
                sortedMapSerializer.getComparator,
                new TypeDeserializerAdapter[K](keyCompatResult.getConvertDeserializer),
                new TypeDeserializerAdapter[V](valueCompatResult.getConvertDeserializer))
            )
          )
        } else {
          CompatibilityResult.requiresMigration[SortedMapView[K, V]]
        }

      case _ => CompatibilityResult.requiresMigration[SortedMapView[K, V]]
    }
  }
}
