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
import org.apache.flink.api.common.typeutils.base.{CollectionSerializerConfigSnapshot, ListSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.table.api.dataview.ListView

/**
  * A serializer for [[ListView]]. The serializer relies on an element
  * serializer for the serialization of the list's elements.
  *
  * The serialization format for the list is as follows: four bytes for the length of the list,
  * followed by the serialized representation of each element.
  *
  * @param listSerializer List serializer.
  * @tparam T The type of element in the list.
  */
@SerialVersionUID(-2030398712359267867L)
class ListViewSerializer[T](val listSerializer: TypeSerializer[java.util.List[T]])
  extends TypeSerializer[ListView[T]]
  with LegacySerializerSnapshotTransformer[ListView[T]] {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[ListView[T]] = {
    new ListViewSerializer[T](listSerializer.duplicate())
  }

  override def createInstance(): ListView[T] = {
    new ListView[T]
  }

  override def copy(from: ListView[T]): ListView[T] = {
    new ListView[T](null, listSerializer.copy(from.list))
  }

  override def copy(from: ListView[T], reuse: ListView[T]): ListView[T] = copy(from)

  override def getLength: Int = -1

  override def serialize(record: ListView[T], target: DataOutputView): Unit = {
    listSerializer.serialize(record.list, target)
  }

  override def deserialize(source: DataInputView): ListView[T] = {
    new ListView[T](null, listSerializer.deserialize(source))
  }

  override def deserialize(reuse: ListView[T], source: DataInputView): ListView[T] =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    listSerializer.copy(source, target)

  override def hashCode(): Int = listSerializer.hashCode()

  override def equals(obj: Any): Boolean =
    listSerializer.equals(obj.asInstanceOf[ListViewSerializer[_]].listSerializer)

  override def snapshotConfiguration(): ListViewSerializerSnapshot[T] =
    new ListViewSerializerSnapshot[T](this)

  /**
    * We need to override this as a [[LegacySerializerSnapshotTransformer]]
    * because in Flink 1.6.x and below, this serializer was incorrectly returning
    * directly the snapshot of the nested list serializer as its own snapshot.
    *
    * <p>This method transforms the incorrect list serializer snapshot
    * to be a proper [[ListViewSerializerSnapshot]].
    */
  override def transformLegacySerializerSnapshot[U](
      legacySnapshot: TypeSerializerSnapshot[U]
  ): TypeSerializerSnapshot[ListView[T]] = {

    legacySnapshot match {
      case correctSnapshot: ListViewSerializerSnapshot[T] =>
        correctSnapshot

      case legacySnapshot: CollectionSerializerConfigSnapshot[java.util.List[T], T] =>
        // first, transform the incorrect list serializer's snapshot
        // into a proper ListSerializerSnapshot
        val transformedNestedListSerializerSnapshot = new ListSerializerSnapshot[T]
        CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
          transformedNestedListSerializerSnapshot,
          legacySnapshot.getSingleNestedSerializerAndConfig.f1)

        // then, wrap the transformed ListSerializerSnapshot
        // as a nested snapshot in the final resulting ListViewSerializerSnapshot
        val transformedListViewSerializerSnapshot = new ListViewSerializerSnapshot[T]()
        CompositeTypeSerializerUtil.setNestedSerializersSnapshots(
          transformedListViewSerializerSnapshot,
          transformedNestedListSerializerSnapshot)

        transformedListViewSerializerSnapshot
    }
  }

  def getListSerializer: TypeSerializer[java.util.List[T]] = listSerializer
}
