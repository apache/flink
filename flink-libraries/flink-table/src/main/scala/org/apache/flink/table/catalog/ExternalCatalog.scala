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

import java.util.{LinkedHashMap => JLinkedHashMap, List => JList}

import org.apache.flink.table.api.{PartitionNotExistException, _}

/**
  * An [[ExternalCatalog]] is the connector between an external database catalog and Flink's
  * Table API.
  *
  * It provides information about catalogs, databases and tables such as names, schema, statistics,
  * and access information.
  */
trait ExternalCatalog {

  /**
    * Gets the partition from external Catalog
    *
    * @param tableName table name
    * @param partSpec  partition specification
    * @throws TableNotExistException     if table does not exist in the catalog yet
    * @throws PartitionNotExistException if partition does not exist in the catalog yet
    * @return found partition
    */
  @throws[TableNotExistException]
  @throws[PartitionNotExistException]
  def getPartition(
    tableName: String,
    partSpec: JLinkedHashMap[String, String]): ExternalCatalogPartition

  /**
    * Gets the partition specification list of a table from external catalog
    *
    * @param tableName table name
    * @throws CatalogNotExistException  if database does not exist in the catalog yet
    * @throws TableNotExistException    if table does not exist in the catalog yet
    * @return list of partition spec
    */
  @throws[TableNotExistException]
  def listPartitions(tableName: String): JList[JLinkedHashMap[String, String]]

  /**
    * Get a table from this catalog.
    *
    * @param tableName The name of the table.
    * @throws TableNotExistException    thrown if the table does not exist in the catalog.
    * @return The requested table.
    */
  @throws[TableNotExistException]
  def getTable(tableName: String): ExternalCatalogTable

  /**
    * Gets the names of all tables registered in this catalog.
    *
    * @return A list of the names of all registered tables.
    */
  def listTables(): JList[String]

  /**
    * Gets a sub catalog from this catalog.
    *
    * @return The requested sub catalog.
    */
  @throws[CatalogNotExistException]
  def getSubCatalog(dbName: String): ExternalCatalog

  /**
    * Gets the names of all sub catalogs registered in this catalog.
    *
    * @return The list of the names of all registered sub catalogs.
    */
  def listSubCatalogs(): JList[String]

}
