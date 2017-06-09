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

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.plan.stats.TableStats

/**
  * Defines a table in an [[ExternalCatalog]].
  *
  * @param tableType            Table type, e.g csv, hbase, kafka
  * @param schema               Schema of the table (column names and types)
  * @param properties           Properties of the table
  * @param stats                Statistics of the table
  * @param comment              Comment of the table
  * @param createTime           Create timestamp of the table
  * @param lastAccessTime       Timestamp of last access of the table
  */
case class ExternalCatalogTable(
    tableType: String,
    schema: TableSchema,
    properties: JMap[String, String] = new JHashMap(),
    stats: TableStats = null,
    comment: String = null,
    createTime: JLong = System.currentTimeMillis,
    lastAccessTime: JLong = -1L)
