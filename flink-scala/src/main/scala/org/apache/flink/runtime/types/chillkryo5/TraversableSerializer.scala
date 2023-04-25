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

import com.esotericsoftware.kryo.kryo5
import com.esotericsoftware.kryo.kryo5.{io, Serializer}

import scala.collection.generic.CanBuildFrom

class TraversableSerializer[T, C <: Traversable[T]](override val isImmutable: Boolean = true)(
    implicit cbf: CanBuildFrom[C, T, C])
  extends Serializer[C] {

  override def write(kryo: kryo5.Kryo, output: io.Output, obj: C): Unit = {
    // Write the size:
    output.writeInt(obj.size, true)
    obj.foreach {
      t =>
        val tRef = t.asInstanceOf[AnyRef]
        kryo.writeClassAndObject(output, tRef)
        // After each intermediate object, flush
        output.flush()
    }
  }

  override def read(kryo: kryo5.Kryo, input: io.Input, cls: Class[_ <: C]): C = {
    val size = input.readInt(true)
    // Go ahead and be faster, and not as functional cool, and be mutable in here
    var idx = 0
    val builder = cbf()
    builder.sizeHint(size)

    while (idx < size) {
      val item = kryo.readClassAndObject(input).asInstanceOf[T]
      builder += item
      idx += 1
    }
    builder.result()
  }
}
