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

import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.common.typeutils.base.{MapSerializer, MapSerializerConfigSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.util.Preconditions

/**
  * A serializer for [[MapView]]. The serializer relies on a key serializer and a value
  * serializer for the serialization of the map's key-value pairs.
  *
  * The serialization format for the map is as follows: four bytes for the length of the map,
  * followed by the serialized representation of each key-value pair. To allow null values,
  * each value is prefixed by a null marker.
  *
  * @param mapSerializer Map serializer.
  * @tparam K The type of the keys in the map.
  * @tparam V The type of the values in the map.
  */
class MapViewSerializer[K, V](val mapSerializer: MapSerializer[K, V])
  extends CompositeTypeSerializer[MapView[K, V]](
    new MapViewSerializerConfigSnapshot[K, V](Preconditions.checkNotNull(mapSerializer)),
    mapSerializer) {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[MapView[K, V]] =
    new MapViewSerializer[K, V](
      mapSerializer.duplicate().asInstanceOf[MapSerializer[K, V]])

  override def createInstance(): MapView[K, V] = {
    new MapView[K, V]()
  }

  override def copy(from: MapView[K, V]): MapView[K, V] = {
    new MapView[K, V](null, null, mapSerializer.copy(from.map))
  }

  override def copy(from: MapView[K, V], reuse: MapView[K, V]): MapView[K, V] = copy(from)

  override def getLength: Int = -1  // var length

  override def serialize(record: MapView[K, V], target: DataOutputView): Unit = {
    mapSerializer.serialize(record.map, target)
  }

  override def deserialize(source: DataInputView): MapView[K, V] = {
    new MapView[K, V](null, null, mapSerializer.deserialize(source))
  }

  override def deserialize(reuse: MapView[K, V], source: DataInputView): MapView[K, V] =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    mapSerializer.copy(source, target)

  override def canEqual(obj: Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = mapSerializer.hashCode()

  override def equals(obj: Any): Boolean = canEqual(this) &&
    mapSerializer.equals(obj.asInstanceOf[MapViewSerializer[_, _]].mapSerializer)

  override def isComparableSnapshot(
      configSnapshot: TypeSerializerConfigSnapshot[_]): Boolean = {

    configSnapshot.isInstanceOf[MapViewSerializerConfigSnapshot[K, V]] ||
      // backwards compatibility path;
      // Flink versions older or equal to 1.5.x returns a
      // MapSerializerConfigSnapshot as the snapshot
      configSnapshot.isInstanceOf[MapSerializerConfigSnapshot[K, V]]
  }
}
