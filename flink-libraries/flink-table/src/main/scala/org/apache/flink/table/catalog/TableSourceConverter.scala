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

package org.apache.flink.table.catalog

import java.util.{Set => JSet}

import org.apache.flink.table.sources.TableSource

/** Creates a [[org.apache.flink.table.sources.TableSource]] from the properties of an
  * [[ExternalCatalogTable]].
  *
  * The [[org.apache.flink.table.annotation.TableType]] annotation defines which type of external
  * table is supported.
  *
  * @tparam T The [[TableSource]] to be created by this converter.
  *
  * @deprecated Use the more generic [[org.apache.flink.table.sources.TableSourceFactory]] instead.
  */
@Deprecated
@deprecated("Use the more generic table source factories instead.")
trait TableSourceConverter[T <: TableSource[_]] {

  /**
    * Defines the properties that need to be provided by the [[ExternalCatalogTable]] to create
    * the [[TableSource]].
    *
    * @return The required properties.
    */
  def requiredProperties: JSet[String]

  /**
    * Creates a [[TableSource]] for the given [[ExternalCatalogTable]].
    *
    * @param externalCatalogTable ExternalCatalogTable to create a TableSource from.
    * @return The created TableSource.
    */
  def fromExternalCatalogTable(externalCatalogTable: ExternalCatalogTable): T

}
