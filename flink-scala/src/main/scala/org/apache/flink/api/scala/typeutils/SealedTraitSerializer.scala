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

import java.util

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

class SealedTraitSerializer[T](
        val subtypeClasses: Array[Class[_]],
        val subtypeSerializers: Array[TypeSerializer[_]]) extends TypeSerializer[T] {


  override def duplicate(): TypeSerializer[T] = new SealedTraitSerializer[T](
    subtypeClasses,
    subtypeSerializers.map(_.duplicate())
  )

  override def createInstance(): T = subtypeSerializers(0).createInstance().asInstanceOf[T]

  override def isImmutableType: Boolean = subtypeSerializers.forall(_.isImmutableType)

  override def getLength: Int = -1

  override def copy(from: T): T = {
    subtypeSerializers(subtypeIndex(from)).asInstanceOf[TypeSerializer[T]].copy(from)
  }

  override def copy(from: T, reuse: T): T = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    serialize(deserialize(source), target)

  override def serialize(record: T, target: DataOutputView): Unit = {
    val index = subtypeIndex(record)
    target.writeInt(index)
    subtypeSerializers(index).asInstanceOf[TypeSerializer[T]].serialize(record, target)
  }

  override def deserialize(source: DataInputView): T = {
    val index = source.readInt()
    subtypeSerializers(index).asInstanceOf[TypeSerializer[T]].deserialize(source)
  }

  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaSealedTraitSerializerSnapshot[T](this)

  override def equals(obj: Any): Boolean = obj match {
    case st: SealedTraitSerializer[_] => st.subtypeClasses.sameElements(subtypeClasses)
    case _ => false
  }

  override def hashCode(): Int = util.Arrays.hashCode(subtypeClasses.asInstanceOf[Array[AnyRef]])


  private def subtypeIndex(subtype: T): Int = {
    var i = 0
    while ((i < subtypeClasses.length) && !subtypeClasses(i).isInstance(subtype)) { i += 1 }
    if (i == subtypeClasses.length) {
      throw new UnsupportedOperationException(
        s"cannot dispatch subtype ${subtype} over a list of known subtypes ${subtypeClasses.toList}"
      )
    } else {
      i
    }
  }

}

