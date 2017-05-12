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

import java.io.IOException

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.{CompatibilityResult, TypeSerializer, TypeSerializerConfigSnapshot}
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flink.api.java.typeutils.runtime.{DataInputViewStream, DataOutputViewStream}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

/**
 * Serializer for [[Enumeration]] values.
 */
@Internal
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
    obj match {
      case enumValueSerializer: EnumValueSerializer[_] =>
        enumValueSerializer.canEqual(this) && enum == enumValueSerializer.enum
      case _ => false
    }
  }

  override def hashCode(): Int = {
    enum.hashCode()
  }

  override def canEqual(obj: scala.Any): Boolean = {
    obj.isInstanceOf[EnumValueSerializer[_]]
  }

  // --------------------------------------------------------------------------------------------
  // Serializer configuration snapshotting & compatibility
  // --------------------------------------------------------------------------------------------

  override def snapshotConfiguration(): EnumValueSerializer.ScalaEnumSerializerConfigSnapshot[E] = {
    new EnumValueSerializer.ScalaEnumSerializerConfigSnapshot[E](
      enum.getClass.asInstanceOf[Class[E]])
  }

  override def ensureCompatibility(
      configSnapshot: TypeSerializerConfigSnapshot): CompatibilityResult[E#Value] = {

    configSnapshot match {
      case enumSerializerConfigSnapshot: EnumValueSerializer.ScalaEnumSerializerConfigSnapshot[_] =>
        val enumClass = enum.getClass.asInstanceOf[Class[E]]
        if (enumClass.equals(enumSerializerConfigSnapshot.getEnumClass)) {
          val currentEnumConstants = enumSerializerConfigSnapshot.getEnumClass.getEnumConstants

          for ( i <- 0 to currentEnumConstants.length) {
            // compatible only if new enum constants are only appended,
            // and original constants must be in the exact same order

            if (currentEnumConstants(i) != enumSerializerConfigSnapshot.getEnumConstants(i)) {
              return CompatibilityResult.requiresMigration()
            }
          }

          CompatibilityResult.compatible()
        } else {
          CompatibilityResult.requiresMigration()
        }

      case _ => CompatibilityResult.requiresMigration()
    }
  }
}

object EnumValueSerializer {

  class ScalaEnumSerializerConfigSnapshot[E <: Enumeration](private var enumClass: Class[E])
      extends TypeSerializerConfigSnapshot {

    var enumConstants: Array[E] = enumClass.getEnumConstants

    /** This empty nullary constructor is required for deserializing the configuration. */
    def this() = this(null)

    override def write(out: DataOutputView): Unit = {
      super.write(out)

      try {
        val outViewWrapper = new DataOutputViewStream(out)
        try {
          InstantiationUtil.serializeObject(outViewWrapper, enumClass)
          InstantiationUtil.serializeObject(outViewWrapper, enumConstants)
        } finally if (outViewWrapper != null) outViewWrapper.close()
      }
    }

    override def read(in: DataInputView): Unit = {
      super.read(in)

      try {
        val inViewWrapper = new DataInputViewStream(in)
        try
          try {
            enumClass = InstantiationUtil.deserializeObject(
              inViewWrapper, getUserCodeClassLoader)

            enumConstants = InstantiationUtil.deserializeObject(
              inViewWrapper, getUserCodeClassLoader)
          } catch {
            case e: ClassNotFoundException =>
              throw new IOException("The requested enum class cannot be found in classpath.", e)
          }
          finally if (inViewWrapper != null) inViewWrapper.close()
      }
    }

    override def getVersion: Int = ScalaEnumSerializerConfigSnapshot.VERSION

    def getEnumClass: Class[E] = enumClass

    def getEnumConstants: Array[E] = enumConstants

    override def equals(obj: scala.Any): Boolean = {
      if (obj == this) {
        return true
      }

      if (obj == null) {
        return false
      }

      obj.isInstanceOf[ScalaEnumSerializerConfigSnapshot[E]] &&
        enumClass.equals(obj.asInstanceOf[ScalaEnumSerializerConfigSnapshot[E]].enumClass) &&
        enumConstants.sameElements(
          obj.asInstanceOf[ScalaEnumSerializerConfigSnapshot[E]].enumConstants)
    }

    override def hashCode(): Int = {
      enumClass.hashCode() * 31 + enumConstants.toSeq.hashCode()
    }
  }

  object ScalaEnumSerializerConfigSnapshot {
    val VERSION = 1
  }

}
