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

package org.apache.flink.table.plan.stats

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.BigDecimalTypeInfo
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo._
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.metadata.FlinkRelMdSize
import org.apache.flink.table.util.Logging

import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.rel.`type`.RelDataType

import scala.collection.JavaConverters._

object AnalyzeStatistic extends Logging {

  /**
    * Analyzes the given columns of the given table to generate statistics.
    *
    * @param tableEnv The [[TableEnvironment]] in which the given table name is registered.
    * @param tableName The table name under which the table is registered in [[TableEnvironment]].
    *                  tableName must be a single name(e.g. "MyTable") associated with a table
    *                  registered as DataStream, DataSet, or Table.
    * @param columnNames Column names of the given table to generate [[ColumnStats]].
    *                    columnNames can be either '*'(all columns will be analyzed)
    *                    or some partial columns. If empty, no column will be analyzed.
    *                    Notes: columnNames are case sensitive.
    * @return [[TableStats]] includes rowCount and the given columns' ColumnStats.
    */
  def generateTableStats(
    tableEnv: TableEnvironment,
    tableName: String,
    columnNames: Array[String]): TableStats = {
    require(tableName != null && tableName.nonEmpty, "tableName must not be null or empty.")

    generateTableStats(tableEnv, Array(tableName), columnNames)
  }

  /**
    * Analyzes the given columns of the given table to generate statistics.
    *
    * @param tableEnv The [[TableEnvironment]] in which the given table name is registered.
    * @param tablePath The table path under which the table is registered in [[TableEnvironment]].
    *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a table
    *                  registered as Table, or can be a nest names
    *                  (e.g. Array("MyCatalog", "MyDb", "MyTable")) associated with a table
    *                  registered as member of a [[org.apache.flink.table.catalog.ReadableCatalog]].
    * @param columnNames Column names of the given table to generate [[ColumnStats]].
    *                    columnNames can be either '*'(all columns will be analyzed)
    *                    or some partial columns. If empty, no column will be analyzed.
    *                    Notes: columnNames are case sensitive.
    * @return [[TableStats]] includes rowCount and the given columns' ColumnStats.
    */
  def generateTableStats(
    tableEnv: TableEnvironment,
    tablePath: Array[String],
    columnNames: Array[String]): TableStats = {
    require(tablePath != null && tablePath.nonEmpty, "tablePath must not be null or empty.")
    require(columnNames != null, "columnNames must not be null")

    val tableName = tablePath.mkString(".")
    val tableOpt = tableEnv.getTable(tablePath)
    if (tableOpt.isEmpty) {
      throw new TableException(s"Table '$tableName' was not found.")
    }
    // TODO handle partitionable table
    val table = tableOpt.get
    val rowType = table.getRowType(tableEnv.getTypeFactory)
    val allFieldNames = rowType.getFieldNames.asScala

    val finalColumnNames = if (columnNames.nonEmpty) {
      if (columnNames.contains("*")) {
        if (columnNames.length > 1) {
          throw new TableException(s"columnNames are either star('*') or field names.")
        } else {
          // all columns
          allFieldNames.toArray
        }
      } else {
        // Case is sensitive here
        val notExistColumns = columnNames.filter(n => !allFieldNames.contains(n))
        if (notExistColumns.nonEmpty) {
          throw new TableException(
            s"Column(s): ${notExistColumns.mkString(", ")} not found in table: $tableName.")
        }
        columnNames
      }
    } else {
      Array.empty[String]
    }

    val quoting = tableEnv.getFrameworkConfig.getParserConfig.quoting()
    val tableNameWithQuoting = tablePath.map(withQuoting(_, quoting)).mkString(".")
    val rowCountStats = "CAST(COUNT(1) AS BIGINT)"
    val statsSql = if (finalColumnNames.nonEmpty) {
      val rowType = table.getRowType(tableEnv.getTypeFactory)
      val columnStatsSelects = getColumnStatsSelects(tableEnv, rowType, finalColumnNames, quoting)
      s"SELECT $rowCountStats, $columnStatsSelects FROM $tableNameWithQuoting"
    } else {
      s"SELECT $rowCountStats FROM $tableNameWithQuoting"
    }

    if (LOG.isDebugEnabled) {
      LOG.debug(s"Analyze TableStats for $tableName, SQL: $statsSql")
    }

    val result = tableEnv.sqlQuery(statsSql).collect().head

    val rowCount = result.getField(0).asInstanceOf[Long]
    val numOfColStats = 6
    val colStatsMap = finalColumnNames.zipWithIndex.map {
      case (columnName, index) =>
        val ndv = result.getField(index * numOfColStats + 1).asInstanceOf[Long]
        val nullCount = result.getField(index * numOfColStats + 2).asInstanceOf[Long]
        val avgLen = result.getField(index * numOfColStats + 3).asInstanceOf[Double]
        val maxLen = result.getField(index * numOfColStats + 4).asInstanceOf[Integer]
        val max = result.getField(index * numOfColStats + 5)
        val min = result.getField(index * numOfColStats + 6)
        (columnName, ColumnStats(ndv, nullCount, avgLen, maxLen, max, min))
    }.toMap

    TableStats(rowCount, colStatsMap.asJava)
  }

  /**
    * generate SQL select items of ColumnStats for given columns.
    *
    * @param rowType The row type of the table.
    * @param columnNames Column names of the given table to generate [[ColumnStats]].
    * @param quoting quoting column name in SQL select statements.
    * @return SQL select items for given columns.
    */
  private def getColumnStatsSelects(
    tableEnv: TableEnvironment,
    rowType: RelDataType,
    columnNames: Array[String],
    quoting: Quoting): String = {
    require(columnNames.nonEmpty)

    val allFieldNames = rowType.getFieldNames.asScala
    val columnStatsSelects = columnNames.map { columnName =>
      val fieldIndex = allFieldNames.indexOf(columnName)
      val relDataType = rowType.getFieldList.get(fieldIndex).getType
      val typeInfo = FlinkTypeFactory.toTypeInfo(relDataType)
      val (isComparableType, isFixLenType) = typeInfo match {
        case BOOLEAN_TYPE_INFO | BYTE_TYPE_INFO | SHORT_TYPE_INFO | INT_TYPE_INFO
             | LONG_TYPE_INFO | FLOAT_TYPE_INFO | DOUBLE_TYPE_INFO | _: BigDecimalTypeInfo
             | DATE | TIME | TIMESTAMP => (true, true)
        case STRING_TYPE_INFO => (true, false)
        case _ => throw new TableException("Analyzing statistics is not supported " +
          s"for column $columnName of data type: $typeInfo.")
      }

      val fixLen = FlinkRelMdSize.averageTypeValueSize(relDataType)
      val tmpSqlTypeName = FlinkTypeFactory.typeInfoToSqlTypeName(typeInfo)
      val sqlTypeName = typeInfo match {
        case d: BigDecimalTypeInfo => s"$tmpSqlTypeName(${d.precision()}, ${d.scale()})"
        case _ => tmpSqlTypeName
      }

      // Adds CAST here to make sure that the result values are expected types
      val columnNameWithQuoting = withQuoting(columnName, quoting)
      val computeNdv = if (tableEnv.isInstanceOf[BatchTableEnvironment]) {
        s"CAST(APPROX_COUNT_DISTINCT($columnNameWithQuoting) AS BIGINT)"
      } else {
        s"CAST(COUNT(DISTINCT $columnNameWithQuoting) AS BIGINT)"
      }
      val computeNullCount = s"CAST((COUNT(1) - COUNT($columnNameWithQuoting)) AS BIGINT)"
      val computeAvgLen = if (isFixLenType) {
        s"CAST($fixLen AS DOUBLE)"
      } else {
        s"CAST(AVG(CAST(CHAR_LENGTH($columnNameWithQuoting) AS DOUBLE)) AS DOUBLE)"
      }
      val computeMaxLen = if (isFixLenType) {
        s"CAST($fixLen AS INTEGER)"
      } else {
        s"CAST(MAX(CHAR_LENGTH($columnNameWithQuoting)) AS INTEGER)"
      }
      val computeMax = if (isComparableType) {
        s"CAST(MAX($columnNameWithQuoting) AS $sqlTypeName)"
      } else {
        s"CAST(NULL AS $sqlTypeName)"
      }
      val computeMin = if (isComparableType) {
        s"CAST(MIN($columnNameWithQuoting) AS $sqlTypeName)"
      } else {
        s"CAST(NULL AS $sqlTypeName)"
      }

      Seq(computeNdv,
        computeNullCount,
        computeAvgLen,
        computeMaxLen,
        computeMax,
        computeMin).mkString(", ")
    }

    columnStatsSelects.mkString(", ")
  }

  /**
    * Returns name with quoting.
    */
  private def withQuoting(name: String, quoting: Quoting): String = {
    if (name.contains(quoting.string)) {
      throw new TableException(s"$name contains ${quoting.string}, that is not supported now.")
    }
    quoting match {
      case Quoting.BRACKET => s"[$name]"
      case _ => s"${quoting.string}$name${quoting.string}"
    }
  }
}
