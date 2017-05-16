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

import org.apache.flink.table.api.{DatabaseAlreadyExistException, DatabaseNotExistException, TableAlreadyExistException, TableNotExistException}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * This class is an in-memory implementation of [[ExternalCatalog]].
  *
  * It could be used for testing or developing instead of used in production environment.
  */
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
  override def alterTable(tableName: String, table: ExternalCatalogTable,
                          ignoreIfNotExists: Boolean): Unit = synchronized {
    if (tables.contains(tableName)) {
      tables.put(tableName, table)
    } else if (!ignoreIfNotExists) {
      throw new TableNotExistException(name, tableName)
    }
  }

  @throws[DatabaseAlreadyExistException]
  override def createDatabase(dbName: String, db: ExternalCatalog,
                              ignoreIfExists: Boolean): Unit = synchronized {
    databases.get(dbName) match {
      case Some(_) if !ignoreIfExists => throw DatabaseAlreadyExistException(dbName, null)
      case _ => databases.put(dbName, db)
    }
  }

  @throws[DatabaseNotExistException]
  override def dropDatabase(dbName: String, ignoreIfNotExists: Boolean): Unit = synchronized {
    if (databases.remove(dbName).isEmpty && !ignoreIfNotExists) {
      throw DatabaseNotExistException(dbName, null)
    }
  }

  override def alterDatabase(
    dbName: String, db: ExternalCatalog,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    if (databases.contains(dbName)) {
      databases.put(dbName, db)
    } else if (!ignoreIfNotExists) {
      throw new DatabaseNotExistException(dbName)
    }
  }

  override def getTable(tableName: String): ExternalCatalogTable = synchronized {
    tables.get(tableName) match {
      case Some(t) => t
      case _ => throw TableNotExistException(name, tableName, null)
    }
  }

  override def listTables(): JList[String] = synchronized {
    tables.keys.toList.asJava
  }

  @throws[DatabaseNotExistException]
  override def getSubCatalog(dbName: String): ExternalCatalog = synchronized {
    databases.get(dbName) match {
      case Some(d) => d
      case _ => throw DatabaseNotExistException(dbName, null)
    }
  }

  override def listSubCatalog(): JList[String] = synchronized {
    databases.keys.toList.asJava
  }
}
