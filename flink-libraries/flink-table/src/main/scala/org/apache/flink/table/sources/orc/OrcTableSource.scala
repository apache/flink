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

package org.apache.flink.table.sources.orc

import java.util

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.sources._
import org.apache.flink.table.util.Logging
import org.apache.orc.storage.ql.io.sarg.SearchArgument

/**
  * A [[BatchTableSource]] for Orc files.
  *
  * @param filePath The path to the orc file.
  * @param fieldTypes The types of the table fields.
  * @param fieldNames The names of the table fields.
  * @param enumerateNestedFiles The flag to specify whether recursive traversal
  *                             of the input directory structure is enabled.
  * @tparam T Type of the [[org.apache.flink.streaming.api.datastream.DataStream]]
  *           created by this [[TableSource]].
  */
abstract class OrcTableSource[T](
    filePath: Path,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    fieldNullables: Array[Boolean],
    enumerateNestedFiles: Boolean)
  extends BatchTableSource[T]
  with StreamTableSource[T]
  with FilterableTableSource
  with ProjectableTableSource
  with Logging {

  protected var filterPredicate: SearchArgument = _

  // mark the filter has been push down or not
  protected var filterPushedDown: Boolean = false

  private var cachedStats: Option[TableStats] = None

  protected var schemaFieldNames: Array[String] = fieldNames

  protected def createTableSource(
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    fieldNullables: Array[Boolean]): OrcTableSource[T]

  protected def setSchemaFields(schemaFieldNames: Array[String]): Unit = {
    this.schemaFieldNames = schemaFieldNames
  }

  protected def setFilterPredicate(filterPredicate: SearchArgument): Unit = {
    this.filterPredicate = filterPredicate
  }

  override def applyPredicate(predicates: util.List[Expression]): TableSource = {
    this.filterPushedDown = true
    val filterPredicate = OrcFilters.createFilter(fieldTypes,
      fieldNames, predicates.toArray(new Array[Expression](predicates.size)))
    if (filterPredicate.isDefined && filterPredicate.get != null) {
      val orcTableSource = createTableSource(fieldTypes, fieldNames, fieldNullables)
      orcTableSource.setFilterPredicate(filterPredicate.get)
      orcTableSource.setFilterPushedDown(true)
      orcTableSource.setSchemaFields(schemaFieldNames)
      orcTableSource
    } else {
      this
    }
  }

  override def isFilterPushedDown: Boolean = filterPushedDown

  protected def setFilterPushedDown(filterPushedDown: Boolean): Unit = {
    this.filterPushedDown = filterPushedDown
  }

  override def projectFields(fields: Array[Int]): TableSource = {
    val (newFieldNames, newFieldTypes, newFieldNullables) = if (fields.nonEmpty) {
      (fields.map(fieldNames(_)), fields.map(fieldTypes(_)), fields.map(fieldNullables(_)))
    } else {
      // reporting number of records only, we must read some columns to get row count.
      // (e.g. SQL: select count(1) from parquet_table)
      // We choose the first column here.
      (Array(fieldNames.head),
        Array[InternalType](fieldTypes.head),
        Array(fieldNullables.head))
    }

    // projectFields does not change the TableStats, we can reuse origin cachedStats
    val newTableSource = createTableSource(newFieldTypes, newFieldNames, newFieldNullables)
    newTableSource.cachedStats = cachedStats
    newTableSource.setSchemaFields(schemaFieldNames)
    newTableSource
  }

  override def setRelBuilder(relBuilder: RelBuilder): Unit = {
  }

  override def getTableSchema: TableSchema = {
    val builder = TableSchema.builder()
    fieldNames.zip(fieldTypes).zip(fieldNullables).foreach {
      case ((name:String, tpe:InternalType), nullable:Boolean) =>
        builder.field(name, tpe, nullable)
    }
    builder.build()
  }

  override def getTableStats: TableStats = {
    cachedStats match {
      case Some(s) => s
      case _ =>
        val stats = try {
          OrcTableStatsCollector.collectTableStats(
            filePath,
            enumerateNestedFiles,
            fieldNames,
            fieldTypes,
            hadoopConf = None,
            maxThreads = None)
        } catch {
          case t: Throwable =>
            LOG.error(s"collectTableStats error: $t")
            null
        }
        cachedStats = Some(stats)
        stats
    }
  }
}
