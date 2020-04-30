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


import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
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
    Preconditions.checkState(enumClass != null)
    Preconditions.checkState(previousEnumConstants != null)
    
    out.writeUTF(enumClass.getName)

    out.writeInt(previousEnumConstants.length)
    for ((name, idx) <- previousEnumConstants) {
      out.writeUTF(name)
      out.writeInt(idx)
    }
  }

  override def readSnapshot(
      readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit = {

    enumClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader)

    val length = in.readInt()
    val listBuffer = ListBuffer[(String, Int)]()

    for (_ <- 0 until length) {
      val name = in.readUTF()
      val idx = in.readInt()
      listBuffer += ((name, idx))
    }

    previousEnumConstants = listBuffer.toList
  }

  override def restoreSerializer(): TypeSerializer[E#Value] = {
    Preconditions.checkState(enumClass != null)
    val enumObject = enumClass.getField("MODULE$").get(null).asInstanceOf[E]
    new EnumValueSerializer(enumObject)
  }

  override def resolveSchemaCompatibility(
    newSerializer: TypeSerializer[E#Value]): TypeSerializerSchemaCompatibility[E#Value] = {

    Preconditions.checkState(enumClass != null)
    Preconditions.checkState(previousEnumConstants != null)

    if (!newSerializer.isInstanceOf[EnumValueSerializer[E]]) {
      return TypeSerializerSchemaCompatibility.incompatible()
    }

    val newEnumSerializer = newSerializer.asInstanceOf[EnumValueSerializer[E]]
    if (!enumClass.equals(newEnumSerializer.enum.getClass)) {
      return TypeSerializerSchemaCompatibility.incompatible()
    }

    for ((previousEnumName, index) <- previousEnumConstants) {
      try {
        val newEnumName = newEnumSerializer.enum(index).toString
        if (previousEnumName != newEnumName) {
          return TypeSerializerSchemaCompatibility.incompatible()
        }
      } catch {
        case _: NoSuchElementException =>
          return TypeSerializerSchemaCompatibility.incompatible()
      }
    }

    TypeSerializerSchemaCompatibility.compatibleAsIs()
  }
}

object ScalaEnumSerializerSnapshot {
  val VERSION = 3
}
