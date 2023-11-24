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

import scala.collection.immutable.BitSet

class ScalaBitSetSerializer extends Serializer[BitSet] {
  override def write(kryo: Kryo, o: Output, v: BitSet): Unit = {
    val size = v.size
    o.writeInt(size, true)
    // Duplicates some data, but helps size on the other end:
    if (size > 0) { o.writeInt(v.max, true) }
    var previous: Int = -1
    v.foreach {
      vi =>
        if (previous >= 0) {
          o.writeInt(vi - previous, true)
        } else {
          o.writeInt(vi, true) // first item
        }
        previous = vi
    }
  }
  override def read(kryo: Kryo, input: Input, c: Class[_ <: BitSet]): BitSet = {
    val size = input.readInt(true)
    if (size == 0) {
      BitSet.empty
    } else {
      var sum = 0
      val bits = new Array[Long](input.readInt(true) / 64 + 1)
      (0 until size).foreach {
        step =>
          sum += input.readInt(true)
          bits(sum / 64) |= 1L << (sum % 64)
      }
      BitSet.fromBitMask(bits)
    }
  }
}
