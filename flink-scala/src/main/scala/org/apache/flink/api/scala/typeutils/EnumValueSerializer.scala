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
package org.apache.flink.api.scala.typeutils

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/** Serializer for [[Enumeration]] values. */
@Internal
@SerialVersionUID(-2403076635594572920L)
class EnumValueSerializer[E <: Enumeration](val enum: E) extends TypeSerializer[E#Value] {

  type T = E#Value

  override def duplicate: EnumValueSerializer[E] = this

  override def createInstance: T = enum(0)

  override def isImmutableType: Boolean = true

  override def getLength: Int = 4

  override def copy(from: T): T = enum.apply(from.id)

  override def copy(from: T, reuse: T): T = copy(from)

  override def copy(src: DataInputView, tgt: DataOutputView): Unit = {
    tgt.writeInt(src.readInt())
  }

  override def serialize(v: T, tgt: DataOutputView): Unit = tgt.writeInt(v.id)

  override def deserialize(source: DataInputView): T = enum(source.readInt())

  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)

  override def equals(obj: Any): Boolean = {
    obj match {
      case enumValueSerializer: EnumValueSerializer[_] =>
        enum == enumValueSerializer.enum
      case _ => false
    }
  }

  override def hashCode(): Int = {
    enum.hashCode()
  }

  // --------------------------------------------------------------------------------------------
  // Serializer configuration snapshotting & compatibility
  // --------------------------------------------------------------------------------------------

  override def snapshotConfiguration(): ScalaEnumSerializerSnapshot[E] = {
    new ScalaEnumSerializerSnapshot[E](enum)
  }
}
