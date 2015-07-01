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

import java.util.BitSet

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
 * Serializer for Case Classes. Creation and access is different from
 * our Java Tuples so we have to treat them differently.
 */
abstract class CaseClassSerializer[T <: Product](
    clazz: Class[T],
    scalaFieldSerializers: Array[TypeSerializer[_]])
  extends TupleSerializerBase[T](clazz, scalaFieldSerializers) with Cloneable {

  @transient var fields : Array[AnyRef] = _

  @transient var instanceCreationFailed : Boolean = false

  override def duplicate = {
    val result = this.clone().asInstanceOf[CaseClassSerializer[T]]

    // set transient fields to null and make copy of serializers
    result.fields = null
    result.instanceCreationFailed = false
    result.fieldSerializers = fieldSerializers.map(_.duplicate())

    result
  }

  def createInstance: T = {
    if (instanceCreationFailed) {
      null.asInstanceOf[T]
    }
    else {
      initArray()
      try {
        var i = 0
        while (i < arity) {
          fields(i) = fieldSerializers(i).createInstance()
          i += 1
        }
        createInstance(fields)
      }
      catch {
        case t: Throwable =>
          instanceCreationFailed = true
          null.asInstanceOf[T]
      }
    }
  }

  def copy(from: T, reuse: T): T = {
    copy(from)
  }

  def copy(from: T): T = {
    initArray()
    var i = 0
    while (i < arity) {
      val fieldValue: AnyRef = from.productElement(i).asInstanceOf[AnyRef]
      if(fieldValue != null){
        fields(i) = fieldSerializers(i).copy(fieldValue)
      } else {
        fields(i) = null
      }
      i += 1
    }
    createInstance(fields)
  }

  def serialize(value: T, target: DataOutputView) {
    val bitIndicator: BitSet = new BitSet(arity)
    var i: Int = 0
    while (i < arity) {
      {
        val element: Any = value.productElement(i)
        bitIndicator.set(i, element != null)
      }
      i += 1
    }

    target.write(bitIndicator.toByteArray)

    i = 0
    while (i < arity) {
      val serializer = fieldSerializers(i).asInstanceOf[TypeSerializer[Any]]
      val element: Any = value.productElement(i)
      if (element != null) {
        serializer.serialize(element, target)
      }
      i += 1
    }
  }

  def deserialize(reuse: T, source: DataInputView): T = {
    deserialize(source)
  }

  def deserialize(source: DataInputView): T = {
    initArray()

    val bitSetSize: Int = (arity / 8) + 1
    val buffer: Array[Byte] = new Array[Byte](bitSetSize)
    source.read(buffer)
    val bitIndicator: BitSet = BitSet.valueOf(buffer)

    var i = 0
    while (i < arity) {
      if(bitIndicator.get(i)){
        fields(i) = fieldSerializers(i).deserialize(source)
      } else {
        fields(i) = null
      }
      i += 1
    }
    createInstance(fields)
  }

  def initArray() = {
    if (fields == null) {
      fields = new Array[AnyRef](arity)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val bitSetSize: Int = (arity / 8) + 1
    val buffer: Array[Byte] = new Array[Byte](bitSetSize)
    source.read(buffer)
    val bitIndicator: BitSet = BitSet.valueOf(buffer)

    target.write(bitIndicator.toByteArray)

    var i: Int = 0
    while (i < arity) {
      if (bitIndicator.get(i)) {
        fieldSerializers(i).copy(source, target)
      }
      i += 1
    }
  }
}
