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
  * This class is responsible for interact with external catalog.
  * Its main responsibilities including:
  * <ul>
  * <li> create/drop/alter database or tables for DDL operations
  * <li> provide tables for calcite catalog, it looks up databases or tables in the external catalog
  * </ul>
  */
trait CrudExternalCatalog extends ExternalCatalog {

  /**
    * Adds table into external Catalog
    *
    * @param table          description of table which to create
    * @param ignoreIfExists if table already exists in the catalog, not throw exception and leave
    *                       the existed table if ignoreIfExists is true;
    *                       else throw a TableAlreadyExistException.
    * @throws DatabaseNotExistException  if database does not exist in the catalog yet
    * @throws TableAlreadyExistException if table already exists in the catalog and
    *                                    ignoreIfExists is false
    */
  @throws[DatabaseNotExistException]
  @throws[TableAlreadyExistException]
  def createTable(table: ExternalCatalogTable, ignoreIfExists: Boolean): Unit

  /**
    * Deletes table from external Catalog
    *
    * @param dbName            database name
    * @param tableName         table name
    * @param ignoreIfNotExists if table not exist yet, not throw exception if ignoreIfNotExists is
    *                          true; else throw TableNotExistException
    * @throws DatabaseNotExistException if database does not exist in the catalog yet
    * @throws TableNotExistException    if table does not exist in the catalog yet
    */
  @throws[DatabaseNotExistException]
  @throws[TableNotExistException]
  def dropTable(dbName: String, tableName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies an existing table in the external catalog
    *
    * @param table             description of table which to modify
    * @param ignoreIfNotExists if the table not exist yet, not throw exception if ignoreIfNotExists
    *                          is true; else throw TableNotExistException
    * @throws DatabaseNotExistException if database does not exist in the catalog yet
    * @throws TableNotExistException    if table does not exist in the catalog yet
    */
  @throws[DatabaseNotExistException]
  @throws[TableNotExistException]
  def alterTable(table: ExternalCatalogTable, ignoreIfNotExists: Boolean): Unit

  /**
    * Adds database into external Catalog
    *
    * @param db             description of database which to create
    * @param ignoreIfExists if database already exists in the catalog, not throw exception and leave
    *                       the existed database if ignoreIfExists is true;
    *                       else throw a DatabaseAlreadyExistException.
    * @throws DatabaseAlreadyExistException if database already exists in the catalog and
    *                                       ignoreIfExists is false
    */
  @throws[DatabaseAlreadyExistException]
  def createDatabase(db: ExternalCatalogDatabase, ignoreIfExists: Boolean): Unit

  /**
    * Deletes database from external Catalog
    *
    * @param dbName            database name
    * @param ignoreIfNotExists if database not exist yet, not throw exception if ignoreIfNotExists
    *                          is true; else throw DatabaseNotExistException
    * @throws DatabaseNotExistException if database does not exist in the catalog yet
    */
  @throws[DatabaseNotExistException]
  def dropDatabase(dbName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies existed database into external Catalog
    *
    * @param db                description of database which to modify
    * @param ignoreIfNotExists if database not exist yet, not throw exception if ignoreIfNotExists
    *                          is true; else throw DatabaseNotExistException
    * @throws DatabaseNotExistException if database does not exist in the catalog yet
    */
  @throws[DatabaseNotExistException]
  def alterDatabase(db: ExternalCatalogDatabase, ignoreIfNotExists: Boolean): Unit

}
