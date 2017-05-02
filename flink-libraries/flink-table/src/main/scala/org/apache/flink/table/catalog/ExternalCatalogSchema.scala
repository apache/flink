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

import java.util.{Collections => JCollections, Collection => JCollection, LinkedHashSet => JLinkedHashSet, Set => JSet}

import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.schema._
import org.apache.flink.table.api.{DatabaseNotExistException, TableNotExistException}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * This class is responsible for connect external catalog to calcite catalog.
  * In this way, it is possible to look-up and access tables in SQL queries
  * without registering tables in advance.
  * The databases in the external catalog registers as calcite sub-Schemas of current schema.
  * The tables in a given database registers as calcite tables
  * of the [[ExternalCatalogDatabaseSchema]].
  *
  * @param catalogIdentifier external catalog name
  * @param catalog           external catalog
  */
class ExternalCatalogSchema(
    catalogIdentifier: String,
    catalog: ExternalCatalog) extends Schema {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Looks up database by the given sub-schema name in the external catalog,
    * returns it Wrapped in a [[ExternalCatalogDatabaseSchema]] with the given database name.
    *
    * @param name Sub-schema name
    * @return Sub-schema with a given name, or null
    */
  override def getSubSchema(name: String): Schema = {
    try {
      val db = catalog.getDatabase(name)
      new ExternalCatalogDatabaseSchema(db.dbName, catalog)
    } catch {
      case e: DatabaseNotExistException =>
        LOG.warn(s"Database $name does not exist in externalCatalog $catalogIdentifier")
        null
    }
  }

  /**
    * Lists the databases of the external catalog,
    * returns the lists as the names of this schema's sub-schemas.
    *
    * @return names of this schema's child schemas
    */
  override def getSubSchemaNames: JSet[String] = new JLinkedHashSet(catalog.listDatabases())

  override def getTable(name: String): Table = null

  override def isMutable: Boolean = true

  override def getFunctions(name: String): JCollection[Function] = JCollections.emptyList[Function]

  override def getExpression(parentSchema: SchemaPlus, name: String): Expression =
    Schemas.subSchemaExpression(parentSchema, name, getClass)

  override def getFunctionNames: JSet[String] = JCollections.emptySet[String]

  override def getTableNames: JSet[String] = JCollections.emptySet[String]

  override def contentsHaveChangedSince(lastCheck: Long, now: Long): Boolean = true

  /**
    * Registers sub-Schemas to current schema plus
    *
    * @param plusOfThis
    */
  def registerSubSchemas(plusOfThis: SchemaPlus) {
    catalog.listDatabases().asScala.foreach(db => plusOfThis.add(db, getSubSchema(db)))
  }

  private class ExternalCatalogDatabaseSchema(
      schemaName: String,
      flinkExternalCatalog: ExternalCatalog) extends Schema {

    override def getTable(name: String): Table = {
      try {
        val externalCatalogTable = flinkExternalCatalog.getTable(schemaName, name)
        ExternalTableSourceUtil.fromExternalCatalogTable(externalCatalogTable)
      } catch {
        case TableNotExistException(db, table, cause) => {
          LOG.warn(s"Table $db.$table does not exist in externalCatalog $catalogIdentifier")
          null
        }
      }
    }

    override def getTableNames: JSet[String] =
      new JLinkedHashSet(flinkExternalCatalog.listTables(schemaName))

    override def getSubSchema(name: String): Schema = null

    override def getSubSchemaNames: JSet[String] = JCollections.emptySet[String]

    override def isMutable: Boolean = true

    override def getFunctions(name: String): JCollection[Function] =
      JCollections.emptyList[Function]

    override def getExpression(parentSchema: SchemaPlus, name: String): Expression =
      Schemas.subSchemaExpression(parentSchema, name, getClass)

    override def getFunctionNames: JSet[String] = JCollections.emptySet[String]

    override def contentsHaveChangedSince(lastCheck: Long, now: Long): Boolean = true

  }

}

object ExternalCatalogSchema {

  /**
    * Registers an external catalog in a Calcite schema.
    *
    * @param parentSchema              Parent schema into which the catalog is registered
    * @param externalCatalogIdentifier Identifier of the external catalog
    * @param externalCatalog           The external catalog to register
    */
  def registerCatalog(
      parentSchema: SchemaPlus,
      externalCatalogIdentifier: String,
      externalCatalog: ExternalCatalog): Unit = {
    val newSchema = new ExternalCatalogSchema(externalCatalogIdentifier, externalCatalog)
    val schemaPlusOfNewSchema = parentSchema.add(externalCatalogIdentifier, newSchema)
    newSchema.registerSubSchemas(schemaPlusOfNewSchema)
  }
}
