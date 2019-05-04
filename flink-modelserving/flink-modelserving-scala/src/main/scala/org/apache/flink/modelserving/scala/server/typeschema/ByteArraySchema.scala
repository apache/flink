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

package org.apache.flink.modelserving.scala.server.typeschema

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

/**
  * Byte Array Schema - used for Kafka byte array serialization/deserialization.
  */
class ByteArraySchema extends DeserializationSchema[Array[Byte]]
  with SerializationSchema[Array[Byte]] {

  private val serialVersionUID = 1234567L

  /**
    * Deserialize byte array message.
    *
    * @param message Byte array message.
    * @return deserialized message.
    */
  override def isEndOfStream(nextElement: Array[Byte]) = false

  /**
    * Check whether end of file is reached.
    *
    * @param nextElement pointer to the next element.
    * @return boolean specifying wheather end of stream is reached.
    */
  override def deserialize(message: Array[Byte]): Array[Byte] = message

  /**
    * Serialize byte array message.
    *
    * @param element Byte array for next element.
    * @return serialized message.
    */
  override def serialize(element: Array[Byte]): Array[Byte] = element

  /**
    * Get data type.
    *
    * @return data type.
    */
  override def getProducedType: TypeInformation[Array[Byte]] =
    TypeExtractor.getForClass(classOf[Array[Byte]])
}
