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

import org.apache.flink.table.api._

import _root_.scala.collection.mutable
import _root_.scala.collection.JavaConverters._

/**
  * This class is an in-memory implementation of [[ExternalCatalog]].
  *
  * @param name      The name of the catalog
  *
  * It could be used for testing or developing instead of used in production environment.
  */
class InMemoryExternalCatalog(name: String) extends CrudExternalCatalog {

  private val databases = new mutable.HashMap[String, ExternalCatalog]
  private val tables = new mutable.HashMap[String, ExternalCatalogTable]
  private val partitions = new mutable.HashMap[String,
    mutable.HashMap[JLinkedHashMap[String, String], ExternalCatalogPartition]]

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
      throw CatalogNotExistException(catalogName, null)
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
      case _ => throw TableNotExistException(name, tableName, null)
    }
  }

  override def listTables(): JList[String] = synchronized {
    tables.keys.toList.asJava
  }

  @throws[CatalogNotExistException]
  override def getSubCatalog(catalogName: String): ExternalCatalog = synchronized {
    databases.get(catalogName) match {
      case Some(d) => d
      case _ => throw CatalogNotExistException(catalogName, null)
    }
  }

  override def listSubCatalogs(): JList[String] = synchronized {
    databases.keys.toList.asJava
  }

  /**
    * Adds partition into an external Catalog table
    *
    * @param tableName      table name
    * @param partition      partition description of partition which to create
    * @param ignoreIfExists if partition already exists in the catalog, not throw exception and
    *                       leave the existed partition if ignoreIfExists is true;
    *                       else throw PartitionAlreadyExistException
    * @throws TableNotExistException         if table does not exist in the catalog yet
    * @throws PartitionAlreadyExistException if partition exists in the catalog and
    *                                        ignoreIfExists is false
    */
  override def createPartition(
    tableName: String,
    partition: ExternalCatalogPartition,
    ignoreIfExists: Boolean): Unit = synchronized {
    val newPartSpec = partition.partitionSpec
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    if (partitions.contains(newPartSpec)) {
      if (!ignoreIfExists) {
        throw new PartitionAlreadyExistException(name, tableName, newPartSpec)
      }
    } else {
      partitions.put(newPartSpec, partition)
    }
  }

  private def getPartitions(tableName: String, table: ExternalCatalogTable)
  : mutable.HashMap[JLinkedHashMap[String, String], ExternalCatalogPartition] = table match {
    case t: ExternalCatalogPartitionedTable =>
      partitions.getOrElseUpdate(
        tableName, new mutable.HashMap[JLinkedHashMap[String, String], ExternalCatalogPartition])
    case _ => throw new UnsupportedOperationException(
      s"Cannot do any operation about partition on the non-partitioned table $tableName!")
  }

  /**
    * Deletes partition of an external Catalog table
    *
    * @param tableName         table name
    * @param partSpec          partition specification
    * @param ignoreIfNotExists if partition not exist yet, not throw exception if ignoreIfNotExists
    *                          is true; else throw PartitionNotExistException
    * @throws TableNotExistException     if table does not exist in the catalog yet
    * @throws PartitionNotExistException if partition does not exist in the catalog yet
    */
  override def dropPartition(
    tableName: String,
    partSpec: JLinkedHashMap[String, String],
    ignoreIfNotExists: Boolean): Unit = synchronized {
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    if (partitions.remove(partSpec).isEmpty && !ignoreIfNotExists) {
      throw new PartitionNotExistException(name, tableName, partSpec)
    }
  }

  /**
    * Alters an existed external Catalog table partition
    *
    * @param tableName         table name
    * @param partition         description of partition which to alter
    * @param ignoreIfNotExists if the partition not exist yet, not throw exception if
    *                          ignoreIfNotExists is true; else throw PartitionNotExistException
    * @throws TableNotExistException     if table does not exist in the catalog yet
    * @throws PartitionNotExistException if partition does not exist in the catalog yet
    */
  override def alterPartition(
    tableName: String,
    partition: ExternalCatalogPartition,
    ignoreIfNotExists: Boolean): Unit = synchronized {
    val updatedPartSpec = partition.partitionSpec
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    if (partitions.contains(updatedPartSpec)) {
      partitions.put(updatedPartSpec, partition)
    } else if (!ignoreIfNotExists) {
      throw new PartitionNotExistException(name, tableName, updatedPartSpec)
    }
  }


  /**
    * Gets the partition from external Catalog
    *
    * @param tableName table name
    * @param partSpec  partition specification
    * @throws TableNotExistException     if table does not exist in the catalog yet
    * @throws PartitionNotExistException if partition does not exist in the catalog yet
    * @return found partition
    */
  override def getPartition(
    tableName: String,
    partSpec: JLinkedHashMap[String, String]): ExternalCatalogPartition = synchronized {
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    partitions.get(partSpec) match {
      case Some(part) => part
      case None =>
        throw new PartitionNotExistException(name, tableName, partSpec)
    }
  }

  /**
    * Gets the partition specification list of a table from external catalog
    *
    * @param tableName table name
    * @throws TableNotExistException   if table does not exist in the catalog yet
    * @return list of partition spec
    */
  override def listPartitions(tableName: String): JList[JLinkedHashMap[String, String]] =
    synchronized {
    val table = getTable(tableName)
    val partitions = getPartitions(tableName, table)
    partitions.keys.toList.asJava
  }
}
