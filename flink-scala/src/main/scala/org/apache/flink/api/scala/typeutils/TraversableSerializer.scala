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

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.collection.generic.CanBuildFrom

/**
 * Serializer for Scala Collections.
 */
@Internal
@SerialVersionUID(7522917416391312410L)
abstract class TraversableSerializer[T <: TraversableOnce[E], E](
    var elementSerializer: TypeSerializer[E])
  extends TypeSerializer[T] with Cloneable {

  def getCbf: CanBuildFrom[T, E, T]

  @transient var cbf: CanBuildFrom[T, E, T] = getCbf

  override def duplicate = {
    val duplicateElementSerializer = elementSerializer.duplicate()
    if (duplicateElementSerializer == elementSerializer) {
      // is not stateful, so return ourselves
      this
    } else {
      val result = this.clone().asInstanceOf[TraversableSerializer[T, E]]
      result.elementSerializer = elementSerializer.duplicate()
      result
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    cbf = getCbf
  }

  override def createInstance: T = {
    cbf().result()
  }

  override def isImmutableType: Boolean = false

  override def getLength: Int = -1

  override def copy(from: T): T = {
    val builder = cbf()
    builder.sizeHint(from.size)
    from foreach { e => builder += elementSerializer.copy(e) }
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
    obj match {
      case other: TraversableSerializer[_, _] =>
        other.canEqual(this) && elementSerializer.equals(other.elementSerializer)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    elementSerializer.hashCode()
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[TraversableSerializer[_, _]]
  }

  override def snapshotConfiguration(): TraversableSerializerConfigSnapshot[E] = {
    new TraversableSerializerConfigSnapshot[E](elementSerializer)
  }

  override def ensureCompatibility(
      configSnapshot: TypeSerializerConfigSnapshot): CompatibilityResult[T] = {

    configSnapshot match {
      case traversableSerializerConfigSnapshot
          : TraversableSerializerConfigSnapshot[E] =>

        val elemCompatRes = CompatibilityUtil.resolveCompatibilityResult(
          traversableSerializerConfigSnapshot.getSingleNestedSerializerAndConfig.f0,
          classOf[UnloadableDummyTypeSerializer[_]],
          traversableSerializerConfigSnapshot.getSingleNestedSerializerAndConfig.f1,
          elementSerializer)

        if (elemCompatRes.isRequiresMigration) {
          CompatibilityResult.requiresMigration()
        } else {
          CompatibilityResult.compatible()
        }

      case _ => CompatibilityResult.requiresMigration()
    }
  }
}
