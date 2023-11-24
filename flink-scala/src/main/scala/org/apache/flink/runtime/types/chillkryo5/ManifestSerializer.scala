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

class ManifestSerializer[T] extends Serializer[Manifest[T]] {
  val singletons: IndexedSeq[Manifest[_]] = IndexedSeq(
    Manifest.Any,
    Manifest.AnyVal,
    Manifest.Boolean,
    Manifest.Byte,
    Manifest.Char,
    Manifest.Double,
    Manifest.Float,
    Manifest.Int,
    Manifest.Long,
    Manifest.Nothing,
    Manifest.Null,
    Manifest.Object,
    Manifest.Short,
    Manifest.Unit
  )

  val singletonToIdx = singletons.zipWithIndex.toMap

  private def writeInternal(kser: Kryo, out: Output, obj: Manifest[_]) {
    val idxOpt = singletonToIdx.get(obj)
    if (idxOpt.isDefined) {
      // We offset by 1 to keep positive and save space
      out.writeInt(idxOpt.get + 1, true)
    } else {
      out.writeInt(0, true)
      kser.writeObject(out, obj.erasure)
      // write the type arguments:
      val targs = obj.typeArguments
      out.writeInt(targs.size, true)
      out.flush
      targs.foreach {
        writeInternal(kser, out, _)
      }
    }
  }

  override def write(kryo: Kryo, output: Output, obj: Manifest[T]): Unit = {
    writeInternal(kryo, output, obj)
  }

  override def read(kryo: Kryo, input: Input, cls: Class[_ <: Manifest[T]]): Manifest[T] = {
    val sidx = input.readInt(true)
    if (sidx == 0) {
      val clazz = kryo.readObject(input, classOf[Class[T]]).asInstanceOf[Class[T]]
      val targsCnt = input.readInt(true)
      if (targsCnt == 0) {
        Manifest.classType(clazz)
      } else {
        // We don't need to know the cls:
        val typeArgs = (0 until targsCnt).map(_ => read(kryo, input, null))
        Manifest.classType(clazz, typeArgs.head, typeArgs.tail: _*)
      }
    } else {
      singletons(sidx - 1).asInstanceOf[Manifest[T]]
    }
  }
}
