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

import org.apache.flink.table.api._

/**
  * The CrudExternalCatalog provides methods to create, drop, and alter databases or tables.
  */
trait CrudExternalCatalog extends ExternalCatalog {

  /**
    * Adds a table to the catalog.
    *
    * @param table          Description of the table to add
    * @param ignoreIfExists Flag to specify behavior if a table with the given name already exists:
    *                       if set to false, it throws a TableAlreadyExistException,
    *                       if set to true, nothing happens.
    * @throws DatabaseNotExistException  thrown if database does not exist
    * @throws TableAlreadyExistException thrown if table already exists and ignoreIfExists is false
    */
  @throws[DatabaseNotExistException]
  @throws[TableAlreadyExistException]
  def createTable(table: ExternalCatalogTable, ignoreIfExists: Boolean): Unit

  /**
    * Deletes table from a database of the catalog.
    *
    * @param dbName            Name of the database
    * @param tableName         Name of the table
    * @param ignoreIfNotExists Flag to specify behavior if the table or database does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
    * @throws TableNotExistException    thrown if the table does not exist in the catalog
    */
  @throws[DatabaseNotExistException]
  @throws[TableNotExistException]
  def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies an existing table in the catalog.
    *
    * @param table             New description of the table to update
    * @param ignoreIfNotExists Flag to specify behavior if the table or database does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
    * @throws TableNotExistException    thrown if the table does not exist in the catalog
    */
  @throws[DatabaseNotExistException]
  @throws[TableNotExistException]
  def alterTable(table: ExternalCatalogTable, ignoreIfNotExists: Boolean): Unit

  /**
    * Adds a database to the catalog.
    *
    * @param db             Description of the database to create
    * @param ignoreIfExists Flag to specify behavior if a database with the given name already
    *                       exists: if set to false, it throws a DatabaseAlreadyExistException,
    *                       if set to true, nothing happens.
    * @throws DatabaseAlreadyExistException thrown if the database does already exist in the catalog
    *                                       and ignoreIfExists is false
    */
  @throws[DatabaseAlreadyExistException]
  def createDatabase(db: ExternalCatalogDatabase, ignoreIfExists: Boolean): Unit

  /**
    * Deletes a database from the catalog.
    *
    * @param dbName            Name of the database.
    * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
    */
  @throws[DatabaseNotExistException]
  def dropDatabase(dbName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies an existing database in the catalog.
    *
    * @param db                New description of the database to update
    * @param ignoreIfNotExists Flag to specify behavior if the database does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws DatabaseNotExistException thrown if the database does not exist in the catalog
    */
  @throws[DatabaseNotExistException]
  def alterDatabase(db: ExternalCatalogDatabase, ignoreIfNotExists: Boolean): Unit

}
