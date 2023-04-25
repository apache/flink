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

import scala.collection.mutable.{Map => MMap}

class EnumerationSerializer extends Serializer[Enumeration#Value] {
  private val enumMethod = "scala$Enumeration$$outerEnum"
  private val outerMethod = classOf[Enumeration#Value].getMethod(enumMethod)
  // Cache the enum lookup:
  private val enumMap = MMap[Enumeration#Value, Enumeration]()

  private def enumOf(v: Enumeration#Value): Enumeration =
    enumMap.synchronized {
      // TODO: hacky, but not clear how to fix:
      enumMap.getOrElseUpdate(
        v,
        outerMethod
          .invoke(v)
          .asInstanceOf[scala.Enumeration])
    }

  override def write(kryo: Kryo, output: Output, obj: Enumeration#Value): Unit = {
    val enum = enumOf(obj)
    // Note due to the ObjectSerializer, this only really writes the class.
    kryo.writeClassAndObject(output, enum)
    // Now, we just write the ID:
    output.writeInt(obj.id)
  }

  override def read(
      kryo: Kryo,
      input: Input,
      cls: Class[_ <: Enumeration#Value]): Enumeration#Value = {
    // Note due to the ObjectSerializer, this only really writes the class.
    val enum = kryo.readClassAndObject(input).asInstanceOf[Enumeration]
    enum(input.readInt).asInstanceOf[Enumeration#Value]
  }
}
