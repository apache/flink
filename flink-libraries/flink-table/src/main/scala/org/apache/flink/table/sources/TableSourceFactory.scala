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

package org.apache.flink.table.sources

import java.util

/**
  * A factory to create a [[TableSource]]. This factory is used with Java's Service Provider
  * Interfaces (SPI) for discovering. A factory is called with a set of normalized properties that
  * describe the desired table source. The factory allows for matching to the given set of
  * properties and creating a configured [[TableSource]] accordingly.
  *
  * Classes that implement this interface need to be added to the
  * "META_INF/services/org.apache.flink.table.sources.TableSourceFactory' file of a JAR file in
  * the current classpath to be found.
  */
trait TableSourceFactory[T] {

  /**
    * Specifies the context that this factory has been implemented for. The framework guarantees
    * to only call the [[create()]] method of the factory if the specified set of properties and
    * values are met.
    *
    * Typical properties might be:
    *   - connector.type
    *   - format.type
    *
    * Specified property versions allow the framework to provide backwards compatible properties
    * in case of string format changes:
    *   - connector.property-version
    *   - format.property-version
    *
    * An empty context means that the factory matches for all requests.
    */
  def requiredContext(): util.Map[String, String]

  /**
    * List of property keys that this factory can handle. This method will be used for validation.
    * If a property is passed that this factory cannot handle, an exception will be thrown. The
    * list must not contain the keys that are specified by the context.
    *
    * Example properties might be:
    *   - format.line-delimiter
    *   - format.ignore-parse-errors
    *   - format.fields.#.type
    *   - format.fields.#.name
    *
    * Note: Use "#" to denote an array of values where "#" represents one or more digits. Property
    * versions like "format.property-version" must not be part of the supported properties.
    */
  def supportedProperties(): util.List[String]

  /**
    * Creates and configures a [[TableSource]] using the given properties.
    *
    * @param properties normalized properties describing a table source
    * @return the configured table source
    */
  def create(properties: util.Map[String, String]): TableSource[T]

}
