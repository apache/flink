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

import java.io.ObjectInputStream

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataOutputView, DataInputView}

import scala.collection.generic.CanBuildFrom
;

/**
 * Serializer for Scala Collections.
 */
abstract class TraversableSerializer[T <: TraversableOnce[E], E](
    val elementSerializer: TypeSerializer[E])
  extends TypeSerializer[T] {

  def getCbf: CanBuildFrom[T, E, T]

  @transient var cbf: CanBuildFrom[T, E, T] = getCbf

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    cbf = getCbf
  }

  override def createInstance: T = {
    cbf().result()
  }

  override def isImmutableType: Boolean = true

  override def getLength: Int = -1

  override def copy(from: T): T = {
    val builder = cbf()
    builder.sizeHint(from.size)
    from foreach { e => builder += e }
    builder.result()
  }

  override def copy(from: T, reuse: T): T = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val len = source.readInt()
    target.writeInt(len)

    var i = 0
    while (i < len) {
      val isNonNull = source.readBoolean()
      target.writeBoolean(isNonNull)
      if (isNonNull) {
        elementSerializer.copy(source, target)
      }
      i += 1
    }
  }

  override def serialize(coll: T, target: DataOutputView): Unit = {
    val len = coll.size
    target.writeInt(len)
    coll foreach { e =>
      if (e == null) {
        target.writeBoolean(false)
      } else {
        target.writeBoolean(true)
        elementSerializer.serialize(e, target)
      }
    }
  }

  override def isStateful: Boolean = false

  override def deserialize(source: DataInputView): T = {
    val len = source.readInt()
    val builder = cbf()

    var i = 0
    while (i < len) {
      val isNonNull = source.readBoolean()
      if (isNonNull) {
        builder += elementSerializer.deserialize(source)
      } else {
        builder += null.asInstanceOf[E]
      }
      i += 1
    }

    builder.result()
  }

  override def deserialize(reuse: T, source: DataInputView): T = {
    val len = source.readInt()
    val builder = cbf()

    var i = 0
    while (i < len) {
      val isNonNull = source.readBoolean()
      if (isNonNull) {
        builder += elementSerializer.deserialize(source)
      } else {
        builder += null.asInstanceOf[E]
      }
      i += 1
    }

    builder.result()
  }

  override def equals(obj: Any): Boolean = {
    if (obj != null && obj.isInstanceOf[TraversableSerializer[_, _]]) {
      val other = obj.asInstanceOf[TraversableSerializer[_, _]]
      other.elementSerializer.equals(elementSerializer)
    } else {
      false
    }
  }
}
