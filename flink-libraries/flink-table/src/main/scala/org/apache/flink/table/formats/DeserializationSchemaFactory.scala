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

package org.apache.flink.table.formats

import java.util

import org.apache.flink.api.common.serialization.DeserializationSchema

/**
  * Factory for creating configured instances of [[DeserializationSchema]].
  *
  * @tparam T record type that the format produces or consumes
  */
trait DeserializationSchemaFactory[T] extends TableFormatFactory[T] {

  /**
    * Creates and configures a [[DeserializationSchema]] using the given properties.
    *
    * @param properties normalized properties describing the format
    * @return the configured serialization schema or null if the factory cannot provide an
    *         instance of this class
    */
  def createDeserializationSchema(properties: util.Map[String, String]): DeserializationSchema[T]

}
