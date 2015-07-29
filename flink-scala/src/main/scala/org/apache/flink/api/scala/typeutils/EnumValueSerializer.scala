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

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
 * Serializer for [[Enumeration]] values.
 */
class EnumValueSerializer[E <: Enumeration](val enum: E) extends TypeSerializer[E#Value] {

  type T = E#Value

  val intSerializer = new IntSerializer()

  override def duplicate: EnumValueSerializer[E] = this

  override def createInstance: T = enum(0)

  override def isImmutableType: Boolean = true

  override def getLength: Int = intSerializer.getLength

  override def copy(from: T): T = enum.apply(from.id)

  override def copy(from: T, reuse: T): T = copy(from)

  override def copy(src: DataInputView, tgt: DataOutputView): Unit = intSerializer.copy(src, tgt)

  override def serialize(v: T, tgt: DataOutputView): Unit = intSerializer.serialize(v.id, tgt)

  override def deserialize(source: DataInputView): T = enum(intSerializer.deserialize(source))

  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)

  override def equals(obj: Any): Boolean = {
    if (obj != null && obj.isInstanceOf[EnumValueSerializer[_]]) {
      val other = obj.asInstanceOf[EnumValueSerializer[_]]
      this.enum == other.enum
    } else {
      false
    }
  }
}
