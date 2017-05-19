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
  * The CrudExternalCatalog provides methods to create, drop, and alter sub catalog or tables.
  */
trait CrudExternalCatalog extends ExternalCatalog {

  /**
    * Adds a table to the current level of the catalog.
    *
    * @param tableName      The name of the table
    * @param table          Description of the table to add
    * @param ignoreIfExists Flag to specify behavior if a table with the given name already exists:
    *                       if set to false, it throws a TableAlreadyExistException,
    *                       if set to true, nothing happens.
    * @throws TableAlreadyExistException thrown if table already exists and ignoreIfExists is false
    */
  @throws[TableAlreadyExistException]
  def createTable(tableName: String, table: ExternalCatalogTable, ignoreIfExists: Boolean): Unit

  /**
    * Deletes table from the current catalog.
    *
    * @param tableName         Name of the table
    * @param ignoreIfNotExists Flag to specify behavior if the table or catalog does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws TableNotExistException    thrown if the table does not exist in the catalog
    */
  @throws[TableNotExistException]
  def dropTable(tableName: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies an existing table in the catalog.
    *
    * @param tableName         The name of the table
    * @param table             New description of the table to update
    * @param ignoreIfNotExists Flag to specify behavior if the table or catalog does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws CatalogNotExistException thrown if the catalog does not exist in the catalog
    * @throws TableNotExistException   thrown if the table does not exist in the catalog
    */
  @throws[TableNotExistException]
  def alterTable(tableName: String, table: ExternalCatalogTable, ignoreIfNotExists: Boolean): Unit

  /**
    * Adds a sub catalog.
    *
    * @param name           The name of the sub catalog
    * @param catalog        Description of the catalog to create
    * @param ignoreIfExists Flag to specify behavior if a sub catalog with the given name already
    *                       exists: if set to false, it throws a CatalogAlreadyExistException,
    *                       if set to true, nothing happens.
    * @throws CatalogAlreadyExistException
    *                       thrown if the sub catalog does already exist in the catalog
    *                       and ignoreIfExists is false
    */
  @throws[CatalogAlreadyExistException]
  def createSubCatalog(name: String, catalog: ExternalCatalog, ignoreIfExists: Boolean): Unit

  /**
    * Deletes a database from the catalog.
    *
    * @param name              Name of the catalog.
    * @param ignoreIfNotExists Flag to specify behavior if the catalog does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws CatalogNotExistException thrown if the sub catalog does not exist in the catalog
    */
  @throws[CatalogNotExistException]
  def dropSubCatalog(name: String, ignoreIfNotExists: Boolean): Unit

  /**
    * Modifies an existing sub catalog in the catalog.
    *
    * @param name              Name of the catalog.
    * @param catalog           New description of the catalog to update
    * @param ignoreIfNotExists Flag to specify behavior if the sub catalog does not exist:
    *                          if set to false, throw an exception,
    *                          if set to true, nothing happens.
    * @throws CatalogNotExistException thrown if the sub catalog does not exist in the catalog
    */
  @throws[CatalogNotExistException]
  def alterSubCatalog(name: String, catalog: ExternalCatalog, ignoreIfNotExists: Boolean): Unit

}
