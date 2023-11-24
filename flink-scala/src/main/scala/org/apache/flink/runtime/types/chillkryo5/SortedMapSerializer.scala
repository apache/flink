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

import com.esotericsoftware.kryo.kryo5.{Kryo, Serializer}
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}

import scala.collection.immutable.SortedMap

class SortedMapSerializer[A, B] extends Serializer[SortedMap[A, B]] {
  type M = SortedMap[A, B]

  override def write(kryo: Kryo, output: Output, map: SortedMap[A, B]): Unit = {
    // Write the size
    output.writeInt(map.size, true)

    // Write the ordering
    kryo.writeClassAndObject(output, map.ordering.asInstanceOf[AnyRef])
    map.foreach {
      t =>
        val tRef = t.asInstanceOf[AnyRef]
        kryo.writeClassAndObject(output, tRef)
        // After each intermediate object, flush
        output.flush()
    }
  }

  override def read(kryo: Kryo, input: Input, cls: Class[_ <: SortedMap[A, B]]): SortedMap[A, B] = {
    val size = input.readInt(true)
    val ordering = kryo.readClassAndObject(input).asInstanceOf[Ordering[A]]

    // Go ahead and be faster, and not as functional cool, and be mutable in here
    var idx = 0
    val builder = SortedMap.canBuildFrom[A, B](ordering)()
    builder.sizeHint(size)

    while (idx < size) {
      val item = kryo.readClassAndObject(input).asInstanceOf[(A, B)]
      builder += item
      idx += 1
    }
    builder.result()
  }
}
