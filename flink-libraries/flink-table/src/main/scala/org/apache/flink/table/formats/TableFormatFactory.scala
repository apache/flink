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

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}

/**
  * A factory to create different table format instances. This factory is used with Java's Service
  * Provider Interfaces (SPI) for discovering. A factory is called with a set of normalized
  * properties that describe the desired format. The factory allows for matching to the given set of
  * properties. See also [[SerializationSchemaFactory]] and [[DeserializationSchemaFactory]] for
  * creating configured instances of format classes accordingly.
  *
  * Classes that implement this interface need to be added to the
  * "META_INF/services/org.apache.flink.table.formats.TableFormatFactory' file of a JAR file in
  * the current classpath to be found.
  *
  * @tparam T record type that the format produces or consumes
  */
trait TableFormatFactory[T] {

  /**
    * Specifies the context that this factory has been implemented for. The framework guarantees
    * to only use the factory if the specified set of properties and values are met.
    *
    * Typical properties might be:
    *   - format.type
    *   - format.version
    *
    * Specified property versions allow the framework to provide backwards compatible properties
    * in case of string format changes:
    *   - format.property-version
    *
    * An empty context means that the factory matches for all requests.
    */
  def requiredContext(): util.Map[String, String]

  /**
    * Flag to indicate if the given format supports deriving information from a schema. If the
    * format can handle schema information, those properties must be added to the list of
    * supported properties.
    */
  def supportsSchemaDerivation(): Boolean

  /**
    * List of format property keys that this factory can handle. This method will be used for
    * validation. If a property is passed that this factory cannot handle, an exception will be
    * thrown. The list must not contain the keys that are specified by the context.
    *
    * Example format properties might be:
    *   - format.line-delimiter
    *   - format.ignore-parse-errors
    *   - format.fields.#.type
    *   - format.fields.#.name
    *
    * If schema derivation is enabled, the list must include schema properties:
    *   - schema.#.name
    *   - schema.#.type
    *
    * Note: Supported format properties must be prefixed with "format.". If schema derivation is
    * enabled, also properties with "schema." prefix can be used. Use "#" to denote an array of
    * values where "#" represents one or more digits. Property versions like
    * "format.property-version" must not be part of the supported properties.
    */
  def supportedProperties(): util.List[String]

}
