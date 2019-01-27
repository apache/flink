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

package org.apache.flink.table.util

import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.types.Row

import java.util

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Utility to describe table or describe column.
  *
  */
object DescribeTableColumn extends Logging {

  /**
    * Describes info of a table, including table schema and table statistics.
    *
    * @param tableEnv  The [[TableEnvironment]] in which the given table name is registered.
    * @param tablePath The table path under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a table
    *                  registered as Table, or can be a nest names
    *                  (e.g. Array("MyCatalog", "MyDb", "MyTable")) associated with a table
    *                  registered as member of a [[org.apache.flink.table.catalog.ReadableCatalog]].
    * @param isRich    Whether need more information except for basic table info or not.
    * @return tableSchema if isRich is false, else return more info such as
    *         table statistics.
    */
  def describeTable(
      tableEnv: TableEnvironment,
      tablePath: Array[String],
      isRich: Boolean): util.List[Row] = {
    val tableName = tablePath.mkString(".")
    val tableOpt = tableEnv.getTable(tablePath)
    if (tableOpt.isEmpty) {
      throw new TableException(s"Table '$tableName' was not found.")
    }
    val table = tableOpt.get
    val data = new mutable.ArrayBuffer[Row]
    val rowType = table.getRowType(tableEnv.getTypeFactory)
    rowType.getFieldList.foreach { f =>
      data += Row.of(f.getName, f.getType.getSqlTypeName.name,
        if (f.getType.isNullable) "YES" else "NO")
    }
    if (isRich) {
      data += Row.of("", "", "")
      data += Row.of("# Detailed Table Information", "", "")
      data += Row.of("table_name", tableName, "")
      val tableStats = Option(tableEnv.getTableStats(tablePath))
      data += Row.of("row_count",
        tableStats.flatMap(ts => Option(ts.rowCount).map(_.toString)).getOrElse("NULL"), "")
    }
    data.toList
  }

  /**
    * Describes info of a column, including name, type, isNullable and column statistics.
    *
    * @param tableEnv  The [[TableEnvironment]] in which the given table name is registered.
    * @param tablePath The table path under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a table
    *                  registered as Table, or can be a nest names
    *                  (e.g. Array("MyCatalog", "MyDb", "MyTable")) associated with a table
    *                  registered as member of a [[org.apache.flink.table.catalog.ReadableCatalog]].
    * @param column    The column to describe.
    * @param isRich    Whether need more information except for column  or not.
    * @return column basic info if isRich is false, else return more info such as column statistics.
    */
  def describeColumn(
      tableEnv: TableEnvironment,
      tablePath: Array[String],
      column: String,
      isRich: Boolean): util.List[Row] = {
    val tableName = tablePath.mkString(".")
    val tableOpt = tableEnv.getTable(tablePath)
    if (tableOpt.isEmpty) {
      throw new TableException(s"Table '$tableName' was not found.")
    }
    val table = tableOpt.get
    val data = new mutable.ArrayBuffer[Row]
    val field = table.getRowType(tableEnv.getTypeFactory).getFieldList
        .find(_.getName.equalsIgnoreCase(column))
    field match {
      case Some(f) =>
        data += Row.of("column_name", f.getName)
        data += Row.of("column_type", f.getType.getSqlTypeName.name())
        data += Row.of("is_nullable", if (f.getType.isNullable) "YES" else "NO")
      case _ => throw new TableException(s"Column $column was not found.")
    }
    if (isRich) {
      val columnStats = Option(tableEnv.getTableStats(tablePath))
          .flatMap(ts => Option(ts.colStats.get(column)))
      data += Row.of("ndv",
        columnStats.flatMap(cs => Option(cs.ndv).map(_.toString)).getOrElse("NULL"))
      data += Row.of("null_count",
        columnStats.flatMap(cs => Option(cs.nullCount).map(_.toString)).getOrElse("NULL"))
      data += Row.of("avg_len",
        columnStats.flatMap(cs => Option(cs.avgLen).map(_.toString)).getOrElse("NULL"))
      data += Row.of("max_len",
        columnStats.flatMap(cs => Option(cs.maxLen).map(_.toString)).getOrElse("NULL"))
      data += Row.of("max",
        columnStats.flatMap(cs => Option(cs.max).map(_.toString)).getOrElse("NULL"))
      data += Row.of("min",
        columnStats.flatMap(cs => Option(cs.min).map(_.toString)).getOrElse("NULL"))
    }
    data.toList
  }

}
