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
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerConfigSnapshot, TypeSerializerSchemaCompatibility}
import org.apache.flink.api.java.typeutils.runtime.{DataInputViewStream, DataOutputViewStream}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.{InstantiationUtil, Preconditions}

import scala.collection.mutable.ListBuffer

/**
 * Serializer for [[Enumeration]] values.
 */
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

object EnumValueSerializer {

  class ScalaEnumSerializerConfigSnapshot[E <: Enumeration]
      extends TypeSerializerConfigSnapshot[E#Value] {

    var enumClass: Class[E] = _
    var enumConstants: List[(String, Int)] = _

    def this(enum: E) = {
      this()
      this.enumClass = Preconditions.checkNotNull(enum).getClass.asInstanceOf[Class[E]]
      this.enumConstants = enum.values.toList.map(x => (x.toString, x.id))
    }

    override def write(out: DataOutputView): Unit = {
      super.write(out)

      val outViewWrapper = new DataOutputViewStream(out)
      try {
        out.writeUTF(enumClass.getName)

        out.writeInt(enumConstants.length)
        for ((name, idx) <- enumConstants) {
          out.writeUTF(name)
          out.writeInt(idx)
        }
      } finally if (outViewWrapper != null) outViewWrapper.close()
    }

    override def read(in: DataInputView): Unit = {
      super.read(in)

      val inViewWrapper = new DataInputViewStream(in)
      try {
        if (getReadVersion == 1) {
          enumClass = InstantiationUtil.deserializeObject(
            inViewWrapper, getUserCodeClassLoader)

          // read null from input stream
          InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader)
          enumConstants = List()
        } else if (getReadVersion == ScalaEnumSerializerConfigSnapshot.VERSION) {
          enumClass = Class.forName(
            in.readUTF(), true, getUserCodeClassLoader).asInstanceOf[Class[E]]

          val length = in.readInt()
          val listBuffer = ListBuffer[(String, Int)]()

          for (_ <- 0 until length) {
            val name = in.readUTF()
            val idx = in.readInt()
            listBuffer += ((name, idx))
          }

          enumConstants = listBuffer.toList
        } else {
          throw new IOException(
            s"Cannot deserialize ${getClass.getSimpleName} with version $getReadVersion.")
        }
      } catch {
        case e: ClassNotFoundException =>
          throw new IOException("The requested enum class cannot be found in classpath.", e)
      }
      finally if (inViewWrapper != null) inViewWrapper.close()
    }

    override def resolveSchemaCompatibility(
      newSerializer: TypeSerializer[E#Value]): TypeSerializerSchemaCompatibility[E#Value] = {
      val serializerSnapshot = new ScalaEnumSerializerSnapshot(enumClass, enumConstants)
      serializerSnapshot.resolveSchemaCompatibility(newSerializer)
    }

    override def getVersion: Int = ScalaEnumSerializerConfigSnapshot.VERSION

    def getEnumClass: Class[E] = enumClass

    def getEnumConstants: List[(String, Int)] = enumConstants

    override def equals(obj: scala.Any): Boolean = {
      if (obj == this) {
        return true
      }

      if (obj == null) {
        return false
      }

      obj.isInstanceOf[ScalaEnumSerializerConfigSnapshot[E]] &&
        enumClass.equals(obj.asInstanceOf[ScalaEnumSerializerConfigSnapshot[E]].enumClass) &&
        enumConstants.equals(
          obj.asInstanceOf[ScalaEnumSerializerConfigSnapshot[E]].enumConstants)
    }

    override def hashCode(): Int = {
      enumClass.hashCode() * 31 + enumConstants.hashCode()
    }

    override def getCompatibleVersions: Array[Int] = {
      Array(ScalaEnumSerializerConfigSnapshot.VERSION, 1)
    }
  }

  object ScalaEnumSerializerConfigSnapshot {
    val VERSION = 2
  }
}
