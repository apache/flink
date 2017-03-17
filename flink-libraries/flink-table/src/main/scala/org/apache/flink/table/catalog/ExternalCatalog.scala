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

import java.util.{List => JList}

import org.apache.flink.table.api._

/**
  * This class is responsible for read table/database from external catalog.
  * Its main responsibilities is provide tables for calcite catalog, it looks up databases or tables
  * in the external catalog.
  */
trait ExternalCatalog {

  /**
    * Gets table from external Catalog
    *
    * @param dbName    database name
    * @param tableName table name
    * @throws DatabaseNotExistException if database does not exist in the catalog yet
    * @throws TableNotExistException    if table does not exist in the catalog yet
    * @return found table
    */
  @throws[DatabaseNotExistException]
  @throws[TableNotExistException]
  def getTable(dbName: String, tableName: String): ExternalCatalogTable

  /**
    * Gets the table name lists from current external Catalog
    *
    * @param dbName database name
    * @throws DatabaseNotExistException if database does not exist in the catalog yet
    * @return lists of table name
    */
  @throws[DatabaseNotExistException]
  def listTables(dbName: String): JList[String]

  /**
    * Gets database from external Catalog
    *
    * @param dbName database name
    * @throws DatabaseNotExistException if database does not exist in the catalog yet
    * @return found database
    */
  @throws[DatabaseNotExistException]
  def getDatabase(dbName: String): ExternalCatalogDatabase

  /**
    * Gets the database name lists from current external Catalog
    *
    * @return list of database names
    */
  def listDatabases(): JList[String]

}
