/**
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
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.core.memory.{DataOutputView, DataInputView}
;

/**
 * Serializer for Scala Tuples. Creation and access is different from
 * our Java Tuples so we have to treat them differently.
 */
abstract class ScalaTupleSerializer[T <: Product](
    tupleClass: Class[T],
    scalaFieldSerializers: Array[TypeSerializer[_]])
  extends TupleSerializerBase[T](tupleClass, scalaFieldSerializers) {

  def createInstance: T = {
    val fields: Array[AnyRef] = new Array(arity)
    for (i <- 0 until arity) {
      fields(i) = fieldSerializers(i).createInstance()
    }
    createInstance(fields)
  }

  def copy(from: T, reuse: T): T = {
    val fields: Array[AnyRef] = new Array(arity)
    for (i <- 0 until arity) {
      fields(i) = from.productElement(i).asInstanceOf[AnyRef]
    }
    createInstance(fields)
  }

  def serialize(value: T, target: DataOutputView) {
    for (i <- 0 until arity) {
      val serializer = fieldSerializers(i).asInstanceOf[TypeSerializer[Any]]
      serializer.serialize(value.productElement(i), target)
    }
  }

  def deserialize(reuse: T, source: DataInputView): T = {
    val fields: Array[AnyRef] = new Array(arity)
    for (i <- 0 until arity) {
      val field = reuse.productElement(i).asInstanceOf[AnyRef]
      fields(i) = fieldSerializers(i).deserialize(field, source)
    }
    createInstance(fields)
  }
}
