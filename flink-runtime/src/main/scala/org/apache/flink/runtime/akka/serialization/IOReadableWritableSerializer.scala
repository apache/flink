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

package org.apache.flink.runtime.akka.serialization

import akka.serialization.JSerializer
import org.apache.flink.core.io.IOReadableWritable
import org.apache.flink.runtime.util.{DataOutputSerializer, DataInputDeserializer}

import org.apache.flink.util.InstantiationUtil

class IOReadableWritableSerializer extends JSerializer {
  val INITIAL_BUFFER_SIZE = 256

  override protected def fromBinaryJava(bytes: Array[Byte], manifest: Class[_]): AnyRef = {
    val in = new DataInputDeserializer(bytes, 0, bytes.length)

    val instance = InstantiationUtil.instantiate(manifest)

    if(!instance.isInstanceOf[IOReadableWritable]){
      throw new RuntimeException(s"Class $manifest is not of type IOReadableWritable.")
    }

    val ioRW = instance.asInstanceOf[IOReadableWritable]

    ioRW.read(in)

    ioRW
  }

  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = {
    if(!o.isInstanceOf[IOReadableWritable]){
      throw new RuntimeException("Object is not of type IOReadableWritable.")
    }

    val ioRW = o.asInstanceOf[IOReadableWritable]

    val out = new DataOutputSerializer(INITIAL_BUFFER_SIZE)
    ioRW.write(out)

    out.wrapAsByteBuffer().array()
  }

  override def identifier: Int = 1337
}
