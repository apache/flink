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

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.api.java.typeutils.runtime.{DataInputViewStream, DataOutputViewStream}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.{InstantiationUtil, Preconditions}

import scala.collection.mutable.ListBuffer

class ScalaEnumSerializerSnapshot[E <: Enumeration]
  extends TypeSerializerSnapshot[E#Value] {

  var enumClass: Class[E] = _
  var previousEnumConstants: List[(String, Int)] = _

  def this(enum: E) = {
    this()
    this.enumClass = Preconditions.checkNotNull(enum).getClass.asInstanceOf[Class[E]]
    this.previousEnumConstants = enum.values.toList.map(x => (x.toString, x.id))
  }

  def this(enumClass: Class[E], previousEnumConstants: List[(String, Int)]) = {
    this()
    this.enumClass = Preconditions.checkNotNull(enumClass)
    this.previousEnumConstants = Preconditions.checkNotNull(previousEnumConstants)
  }

  override def getCurrentVersion: Int = ScalaEnumSerializerSnapshot.VERSION

  override def writeSnapshot(out: DataOutputView): Unit = {
    val outViewWrapper = new DataOutputViewStream(out)
    try {
      out.writeUTF(enumClass.getName)

      out.writeInt(previousEnumConstants.length)
      for ((name, idx) <- previousEnumConstants) {
        out.writeUTF(name)
        out.writeInt(idx)
      }
    } finally if (outViewWrapper != null) outViewWrapper.close()
  }

  override def readSnapshot(
    readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {
    val inViewWrapper = new DataInputViewStream(in)
    try {
      if (readVersion == 1) {
        enumClass = InstantiationUtil.deserializeObject(
          inViewWrapper, userCodeClassLoader)

        // read null from input stream
        InstantiationUtil.deserializeObject(inViewWrapper, userCodeClassLoader)
        previousEnumConstants = List()
      } else if (readVersion >= 2) {
        enumClass = Class.forName(
          in.readUTF(), true, userCodeClassLoader).asInstanceOf[Class[E]]

        val length = in.readInt()
        val listBuffer = ListBuffer[(String, Int)]()

        for (_ <- 0 until length) {
          val name = in.readUTF()
          val idx = in.readInt()
          listBuffer += ((name, idx))
        }

        previousEnumConstants = listBuffer.toList
      } else {
        throw new IOException(
          s"Cannot deserialize ${getClass.getSimpleName} with version $readVersion.")
      }
    } catch {
      case e: ClassNotFoundException =>
        throw new IOException("The requested enum class cannot be found in classpath.", e)
    }
    finally if (inViewWrapper != null) inViewWrapper.close()
  }

  override def restoreSerializer(): TypeSerializer[E#Value] = {
    enumClass.newInstance().asInstanceOf[TypeSerializer[E#Value]]
  }

  override def resolveSchemaCompatibility(
    newSerializer: TypeSerializer[E#Value]): TypeSerializerSchemaCompatibility[E#Value] = {
    newSerializer match {
      case newEnumSerializer: EnumValueSerializer[_] => {
        if (enumClass.equals(newEnumSerializer.enum.getClass)) {
          for ((previousEnumConstant, idx) <- previousEnumConstants) {
            val enumValue = try {
              newEnumSerializer.enum(idx)
            } catch {
              case _: NoSuchElementException =>
                // couldn't find an enum value for the given index
                return TypeSerializerSchemaCompatibility.incompatible()
            }

            if (!previousEnumConstant.equals(enumValue.toString)) {
              // compatible only if new enum constants are only appended,
              // and original constants must be in the exact same order
              return TypeSerializerSchemaCompatibility.compatibleAfterMigration()
            }
          }

          TypeSerializerSchemaCompatibility.compatibleAsIs()
        } else {
          TypeSerializerSchemaCompatibility.incompatible()
        }
      }
      case _ => TypeSerializerSchemaCompatibility.incompatible()
    }
  }
}

object ScalaEnumSerializerSnapshot {
  val VERSION = 3
}
