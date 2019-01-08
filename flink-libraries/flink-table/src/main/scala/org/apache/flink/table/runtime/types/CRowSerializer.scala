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

import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.Row

class CRowSerializer(val rowSerializer: TypeSerializer[Row]) extends TypeSerializer[CRow] {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[CRow] = new CRowSerializer(rowSerializer.duplicate())

  override def createInstance(): CRow = new CRow(rowSerializer.createInstance(), true)

  override def copy(from: CRow): CRow = new CRow(rowSerializer.copy(from.row), from.change)

  override def copy(from: CRow, reuse: CRow): CRow = {
    rowSerializer.copy(from.row, reuse.row)
    reuse.change = from.change
    reuse
  }

  override def getLength: Int = -1

  override def serialize(record: CRow, target: DataOutputView): Unit = {
    rowSerializer.serialize(record.row, target)
    target.writeBoolean(record.change)
  }

  override def deserialize(source: DataInputView): CRow = {
    val row = rowSerializer.deserialize(source)
    val change = source.readBoolean()
    new CRow(row, change)
  }

  override def deserialize(reuse: CRow, source: DataInputView): CRow = {
    rowSerializer.deserialize(reuse.row, source)
    reuse.change = source.readBoolean()
    reuse
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    rowSerializer.copy(source, target)
    target.writeBoolean(source.readBoolean())
  }

  override def canEqual(obj: Any): Boolean = obj.isInstanceOf[CRowSerializer]

  override def equals(obj: Any): Boolean = {

    if (canEqual(obj)) {
      val other = obj.asInstanceOf[CRowSerializer]
      rowSerializer.equals(other.rowSerializer)
    } else {
      false
    }
  }

  override def hashCode: Int = rowSerializer.hashCode() * 13

  // --------------------------------------------------------------------------------------------
  // Serializer configuration snapshotting & compatibility
  // --------------------------------------------------------------------------------------------

  override def snapshotConfiguration(): TypeSerializerConfigSnapshot = {
    new CRowSerializer.CRowSerializerConfigSnapshot(rowSerializer)
  }

  override def ensureCompatibility(
      configSnapshot: TypeSerializerConfigSnapshot): CompatibilityResult[CRow] = {

    configSnapshot match {
      case crowSerializerConfigSnapshot: CRowSerializer.CRowSerializerConfigSnapshot =>
        val compatResult = CompatibilityUtil.resolveCompatibilityResult(
          crowSerializerConfigSnapshot.getSingleNestedSerializerAndConfig.f0,
          classOf[UnloadableDummyTypeSerializer[_]],
          crowSerializerConfigSnapshot.getSingleNestedSerializerAndConfig.f1,
          rowSerializer)

        if (compatResult.isRequiresMigration) {
          if (compatResult.getConvertDeserializer != null) {
            CompatibilityResult.requiresMigration(
              new CRowSerializer(
                new TypeDeserializerAdapter(compatResult.getConvertDeserializer))
            )
          } else {
            CompatibilityResult.requiresMigration()
          }
        } else {
          CompatibilityResult.compatible()
        }

      case _ => CompatibilityResult.requiresMigration()
    }
  }
}

object CRowSerializer {

  class CRowSerializerConfigSnapshot(rowSerializers: TypeSerializer[Row]*)
    extends CompositeTypeSerializerConfigSnapshot(rowSerializers: _*) {

    override def getVersion: Int = CRowSerializerConfigSnapshot.VERSION
  }

  object CRowSerializerConfigSnapshot {
    val VERSION = 1
  }

}
