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
import org.apache.flink.table.api.{CatalogNotExistException, TableNotExistException}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * This class is responsible to connect an external catalog to Calcite's catalog.
  * This enables to look-up and access tables in SQL queries without registering tables in advance.
  * The the external catalog and all included sub-catalogs and tables is registered as
  * sub-schemas and tables in Calcite.
  *
  * @param catalogIdentifier external catalog name
  * @param catalog           external catalog
  */
class ExternalCatalogSchema(
    catalogIdentifier: String,
    catalog: ExternalCatalog) extends Schema {

  private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Looks up a sub-schema by the given sub-schema name in the external catalog.
    * Returns it wrapped in a [[ExternalCatalogSchema]] with the given database name.
    *
    * @param name Name of sub-schema to look up.
    * @return Sub-schema with a given name, or null.
    */
  override def getSubSchema(name: String): Schema = {
    try {
      val db = catalog.getSubCatalog(name)
      new ExternalCatalogSchema(name, db)
    } catch {
      case _: CatalogNotExistException =>
        LOG.warn(s"Sub-catalog $name does not exist in externalCatalog $catalogIdentifier")
        null
    }
  }

  /**
    * Lists the sub-schemas of the external catalog.
    * Returns a list of names of this schema's sub-schemas.
    *
    * @return names of this schema's child schemas
    */
  override def getSubSchemaNames: JSet[String] = new JLinkedHashSet(catalog.listSubCatalogs())

  /**
    * Looks up and returns a table from this schema.
    * Returns null if no table is found for the given name.
    *
    * @param name The name of the table to look up.
    * @return The table or null if no table is found.
    */
  override def getTable(name: String): Table = try {
    val externalCatalogTable = catalog.getTable(name)
    ExternalTableSourceUtil.fromExternalCatalogTable(externalCatalogTable)
  } catch {
    case TableNotExistException(table, _, _) => {
      LOG.warn(s"Table $table does not exist in externalCatalog $catalogIdentifier")
      null
    }
  }

  override def isMutable: Boolean = true

  override def getFunctions(name: String): JCollection[Function] = JCollections.emptyList[Function]

  override def getExpression(parentSchema: SchemaPlus, name: String): Expression =
    Schemas.subSchemaExpression(parentSchema, name, getClass)

  override def getFunctionNames: JSet[String] = JCollections.emptySet[String]

  override def getTableNames: JSet[String] = JCollections.emptySet[String]

  override def contentsHaveChangedSince(lastCheck: Long, now: Long): Boolean = true

  override def snapshot(l: Long): Schema = this

  /**
    * Registers sub-Schemas to current schema plus
    *
    * @param plusOfThis
    */
  def registerSubSchemas(plusOfThis: SchemaPlus) {
    catalog.listSubCatalogs().asScala.foreach(db => plusOfThis.add(db, getSubSchema(db)))
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
