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
  * An [[ExternalCatalog]] is the connector between an external database catalog and Flink's
  * Table API.
  *
  * It provides information about databases and tables such as names, schema, statistics, and
  * access information.
  */
trait ExternalCatalog {

  /**
    * Get a table from the catalog
    *
    * @param dbName    The name of the table's database.
    * @param tableName The name of the table.
    * @throws DatabaseNotExistException thrown if the database does not exist in the catalog.
    * @throws TableNotExistException    thrown if the table does not exist in the catalog.
    * @return the requested table
    */
  @throws[DatabaseNotExistException]
  @throws[TableNotExistException]
  def getTable(dbName: String, tableName: String): ExternalCatalogTable

  /**
    * Get a list of all table names of a database in the catalog.
    *
    * @param dbName The name of the database.
    * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
    * @return The list of table names
    */
  @throws[DatabaseNotExistException]
  def listTables(dbName: String): JList[String]

  /**
    * Gets a database from the catalog.
    *
    * @param dbName The name of the database.
    * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
    * @return The requested database
    */
  @throws[DatabaseNotExistException]
  def getDatabase(dbName: String): ExternalCatalogDatabase

  /**
    * Gets a list of all databases in the catalog.
    *
    * @return The list of database names
    */
  def listDatabases(): JList[String]

}
