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
package org.apache.flink.runtime.types.chillkryo5

import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.Serializer
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.reflect.ClassTag

class WrappedArraySerializer[T] extends Serializer[WrappedArray[T]] {
  override def write(kryo: Kryo, output: Output, obj: mutable.WrappedArray[T]): Unit = {
    // Write the class-manifest, we don't use writeClass because it
    // uses the registration system, and this class might not be registered
    kryo.writeObject(output, obj.elemManifest.runtimeClass)
    kryo.writeClassAndObject(output, obj.array)
  }

  override def read(
      kryo: Kryo,
      input: Input,
      cls: Class[_ <: mutable.WrappedArray[T]]): mutable.WrappedArray[T] = {
    // Write the class-manifest, we don't use writeClass because it
    // uses the registration system, and this class might not be registered
    val clazz = kryo.readObject(input, classOf[Class[T]])
    val array = kryo.readClassAndObject(input).asInstanceOf[Array[T]]
    val bldr = new mutable.WrappedArrayBuilder[T](ClassTag[T](clazz))
    bldr.sizeHint(array.size)
    bldr ++= array
    bldr.result()
  }
}
