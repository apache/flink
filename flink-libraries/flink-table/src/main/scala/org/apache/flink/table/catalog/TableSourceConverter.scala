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

/** Defines a converter used to convert [[org.apache.flink.table.sources.TableSource]] to
  * or from [[ExternalCatalogTable]].
  *
  * @tparam T The tableSource which to do convert operation on.
  */
trait TableSourceConverter[T <: TableSource[_]] {

  /**
    * Defines the required properties that must exists in the properties of an ExternalCatalogTable
    * to ensure the input ExternalCatalogTable is compatible with the requirements of
    * current converter.
    * @return the required properties.
    */
  def requiredProperties: JSet[String]

  /**
    * Converts the input external catalog table instance to a tableSource instance.
    *
    * @param externalCatalogTable input external catalog table instance to convert
    * @return converted tableSource instance from input external catalog table.
    */
  def fromExternalCatalogTable(externalCatalogTable: ExternalCatalogTable): T

}
