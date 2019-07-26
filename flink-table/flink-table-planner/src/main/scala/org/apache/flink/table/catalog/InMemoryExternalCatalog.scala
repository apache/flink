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

import org.apache.flink.table.api.{CatalogAlreadyExistException, CatalogNotExistException, TableAlreadyExistException, TableNotExistException}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * This class is an in-memory implementation of [[ExternalCatalog]].
  *
  * @param name      The name of the catalog
  *
  * It could be used for testing or developing instead of used in production environment.
  *
  * @deprecated use [[GenericInMemoryCatalog]] instead.
  */
@deprecated
class InMemoryExternalCatalog(name: String) extends CrudExternalCatalog {

  private val databases = new mutable.HashMap[String, ExternalCatalog]
  private val tables = new mutable.HashMap[String, ExternalCatalogTable]

  @throws[TableAlreadyExistException]
  override def createTable(
    tableName: String,
    table: ExternalCatalogTable,
    ignoreIfExists: Boolean): Unit = synchronized {
    tables.get(tableName) match {
      case Some(_) if !ignoreIfExists => throw new TableAlreadyExistException(name, tableName)
      case _ => tables.put(tableName, table)
    }
  }

  @throws[TableNotExistException]
  override def dropTable(tableName: String, ignoreIfNotExists: Boolean): Unit = synchronized {
    if (tables.remove(tableName).isEmpty && !ignoreIfNotExists) {
      throw new TableNotExistException(name, tableName)
    }
  }

  @throws[TableNotExistException]
  override def alterTable(
    tableName: String,
    table: ExternalCatalogTable,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    if (tables.contains(tableName)) {
      tables.put(tableName, table)
    } else if (!ignoreIfNotExists) {
      throw new TableNotExistException(name, tableName)
    }
  }

  @throws[CatalogAlreadyExistException]
  override def createSubCatalog(
    catalogName: String,
    catalog: ExternalCatalog,
    ignoreIfExists: Boolean): Unit = synchronized {
    databases.get(catalogName) match {
      case Some(_) if !ignoreIfExists => throw CatalogAlreadyExistException(catalogName, null)
      case _ => databases.put(catalogName, catalog)
    }
  }

  @throws[CatalogNotExistException]
  override def dropSubCatalog(
    catalogName: String,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    if (databases.remove(catalogName).isEmpty && !ignoreIfNotExists) {
      throw new CatalogNotExistException(catalogName)
    }
  }

  override def alterSubCatalog(
    catalogName: String,
    catalog: ExternalCatalog,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    if (databases.contains(catalogName)) {
      databases.put(catalogName, catalog)
    } else if (!ignoreIfNotExists) {
      throw new CatalogNotExistException(catalogName)
    }
  }

  override def getTable(tableName: String): ExternalCatalogTable = synchronized {
    tables.get(tableName) match {
      case Some(t) => t
      case _ => throw new TableNotExistException(name, tableName)
    }
  }

  override def listTables(): JList[String] = synchronized {
    tables.keys.toList.asJava
  }

  @throws[CatalogNotExistException]
  override def getSubCatalog(catalogName: String): ExternalCatalog = synchronized {
    databases.get(catalogName) match {
      case Some(d) => d
      case _ => throw new CatalogNotExistException(catalogName)
    }
  }

  override def listSubCatalogs(): JList[String] = synchronized {
    databases.keys.toList.asJava
  }
}
