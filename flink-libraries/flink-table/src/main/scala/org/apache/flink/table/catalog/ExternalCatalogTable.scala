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

import java.util.{HashMap => JHashMap, Map => JMap}
import java.lang.{Long => JLong}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.plan.stats.TableStats

/**
  * Table definition of the external catalog.
  *
  * @param identifier           identifier of external catalog table, including dbName and tableName
  * @param tableType            type of external catalog table, e.g csv, hbase, kafka
  * @param schema               schema of table data, including column names and column types
  * @param properties           properties of external catalog table
  * @param stats                statistics of external catalog table
  * @param comment              comment of external catalog table
  * @param createTime           create time of external catalog table
  * @param lastAccessTime       last access time of of external catalog table
  */
case class ExternalCatalogTable(
    identifier: TableIdentifier,
    tableType: String,
    schema: TableSchema,
    properties: JMap[String, String] = new JHashMap(),
    stats: TableStats = null,
    comment: String = null,
    createTime: JLong = System.currentTimeMillis,
    lastAccessTime: JLong = -1L)

/**
  * Identifier of external catalog table
  *
  * @param database database name
  * @param table    table name
  */
case class TableIdentifier(
    database: String,
    table: String) {

  override def toString: String = s"$database.$table"

}
