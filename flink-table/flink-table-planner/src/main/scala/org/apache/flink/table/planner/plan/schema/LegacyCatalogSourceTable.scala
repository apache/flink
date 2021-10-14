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

package org.apache.flink.table.planner.plan.schema

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.TableColumn.ComputedColumn
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.factories.{TableFactoryUtil, TableSourceFactory, TableSourceFactoryContextImpl}
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.planner.catalog.CatalogSchemaTable
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.sources.{StreamTableSource, TableSource, TableSourceValidation}
import org.apache.flink.table.types.logical.{LocalZonedTimestampType, TimestampKind, TimestampType}

import org.apache.calcite.plan.{RelOptSchema, RelOptTable}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * A legacy implementation of [[FlinkPreparingTableBase]] which defines the interfaces required
 * to translate the Calcite [[RelOptTable]] to the Flink specific [[LegacyTableSourceTable]].
 *
 * <p>This table is only used to translate the catalog table into [[LegacyTableSourceTable]]
 * during the last phrase of sql-to-rel conversion, it is overdue once the sql node was converted
 * to relational expression.
 *
 * <p>Note: this class can be removed once legacy [[TableSource]] interface is removed.
 *
 * @param schemaTable Schema table which takes the variables needed to find the table source
 */
class LegacyCatalogSourceTable[T](
    relOptSchema: RelOptSchema,
    names: JList[String],
    rowType: RelDataType,
  val schemaTable: CatalogSchemaTable,
  val catalogTable: CatalogTable)
  extends FlinkPreparingTableBase(relOptSchema, rowType, names, schemaTable.getStatistic) {

  lazy val columnExprs: Map[String, String] = {
    catalogTable.getSchema
      .getTableColumns
      .flatMap {
        case computedColumn: ComputedColumn =>
          Some((computedColumn.getName, computedColumn.getExpression))
        case _ =>
          None
      }
      .toMap
  }

  override def toRel(context: RelOptTable.ToRelContext): RelNode = {
    val cluster = context.getCluster
    val flinkContext = cluster
      .getPlanner
      .getContext
      .unwrap(classOf[FlinkContext])
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val conf = flinkContext.getTableConfig.getConfiguration

    val hintedOptions = FlinkHints.getHintedOptions(context.getTableHints)
    if (hintedOptions.nonEmpty
      && !conf.getBoolean(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED)) {
      throw new ValidationException(s"${FlinkHints.HINT_NAME_OPTIONS} hint is allowed only when "
        + s"${TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key} "
        + s"is set to true")
    }

    val tableSource = findAndCreateLegacyTableSource(
      hintedOptions,
      conf)

    // erase time indicator types in the rowType
    val actualRowType = eraseTimeIndicator(rowType, typeFactory, tableSource)

    val tableSourceTable = new LegacyTableSourceTable[T](
      relOptSchema,
      schemaTable.getTableIdentifier,
      actualRowType,
      statistic,
      tableSource,
      schemaTable.isStreamingMode,
      catalogTable,
      hintedOptions)

    // 1. push table scan

    // Get row type of physical fields.
    val physicalFields = getRowType
      .getFieldList
      .filter(f => !columnExprs.contains(f.getName))
      .map(f => f.getIndex)
      .toArray
    // Copy this table with physical scan row type.
    val newRelTable = tableSourceTable.copy(tableSource, physicalFields)
    val scan = LogicalTableScan.create(cluster, newRelTable, context.getTableHints)
    val relBuilder = FlinkRelBuilder.of(cluster, getRelOptSchema)
    relBuilder.push(scan)

    val toRexFactory = flinkContext.getSqlExprToRexConverterFactory

    // 2. push computed column project
    val fieldNames = actualRowType.getFieldNames.asScala
    if (columnExprs.nonEmpty) {
      val fieldExprs = fieldNames
        .map { name =>
          if (columnExprs.contains(name)) {
            columnExprs(name)
          } else {
            s"`$name`"
          }
        }.toArray
      val rexNodes = toRexFactory.create(newRelTable.getRowType, null).convertToRexNodes(fieldExprs)
      relBuilder.projectNamed(rexNodes.toList, fieldNames, true)
    }

    // 3. push watermark assigner
    val watermarkSpec = catalogTable
      .getSchema
      // we only support single watermark currently
      .getWatermarkSpecs.asScala.headOption
    if (schemaTable.isStreamingMode && watermarkSpec.nonEmpty) {
      if (TableSourceValidation.hasRowtimeAttribute(tableSource)) {
        throw new TableException(
          "If watermark is specified in DDL, the underlying TableSource of connector" +
            " shouldn't return an non-empty list of RowtimeAttributeDescriptor" +
            " via DefinedRowtimeAttributes interface.")
      }
      val rowtime = watermarkSpec.get.getRowtimeAttribute
      if (rowtime.contains(".")) {
        throw new TableException(
          s"Nested field '$rowtime' as rowtime attribute is not supported right now.")
      }
      val rowtimeIndex = fieldNames.indexOf(rowtime)
      val watermarkRexNode = toRexFactory
        .create(actualRowType, null)
        .convertToRexNode(watermarkSpec.get.getWatermarkExpr)
      relBuilder.watermark(rowtimeIndex, watermarkRexNode)
    }

    // 4. returns the final RelNode
    relBuilder.build()
  }

  /** Create the legacy table source. */
  private def findAndCreateLegacyTableSource(
      hintedOptions: JMap[String, String],
      conf: ReadableConfig): TableSource[T] = {
    val tableToFind = if (hintedOptions.nonEmpty) {
      catalogTable.copy(
        FlinkHints.mergeTableOptions(
          hintedOptions,
          catalogTable.getOptions))
    } else {
      catalogTable
    }
    val tableSource = TableFactoryUtil.findAndCreateTableSource(
      schemaTable.getCatalog.orElse(null),
      schemaTable.getTableIdentifier,
      tableToFind,
      conf,
      schemaTable.isTemporary)

    // validation
    val tableName = schemaTable.getTableIdentifier.asSummaryString
    tableSource match {
      case ts: StreamTableSource[_] =>
        if (!schemaTable.isStreamingMode && !ts.isBounded) {
          throw new ValidationException("Cannot query on an unbounded source in batch mode, " +
            s"but '$tableName' is unbounded.")
        }
      case _ =>
        throw new ValidationException("Catalog tables only support "
          + "StreamTableSource and InputFormatTableSource")
    }

    tableSource.asInstanceOf[TableSource[T]]
  }

  /**
   * Erases time indicators, i.e. converts rowtime and proctime type into regular timestamp type.
   * This is required before converting this [[CatalogSourceTable]] into multiple RelNodes,
   * otherwise the derived data types are mismatch.
   */
  private def eraseTimeIndicator(
      relDataType: RelDataType,
      factory: FlinkTypeFactory,
      tableSource: TableSource[_]): RelDataType = {

    val hasLegacyTimeAttributes =
      TableSourceValidation.hasRowtimeAttribute(tableSource) ||
        TableSourceValidation.hasProctimeAttribute(tableSource)

    // If the table source is defined by TableEnvironment.connect() and the time attributes are
    // defined by legacy proctime and rowtime descriptors, we should not erase time indicator types
    if (columnExprs.isEmpty
      && catalogTable.getSchema.getWatermarkSpecs.isEmpty
      && hasLegacyTimeAttributes) {
      relDataType
    } else {
      val logicalRowType = FlinkTypeFactory.toLogicalRowType(relDataType)
      val fieldNames = logicalRowType.getFieldNames
      val fieldTypes = logicalRowType.getFields.map { f =>
        if (FlinkTypeFactory.isTimeIndicatorType(f.getType)) {
          f.getType match {
            case ts: TimestampType =>
              new TimestampType(
                ts.isNullable,
                TimestampKind.REGULAR,
                ts.getPrecision)
            case ltz: LocalZonedTimestampType =>
              new LocalZonedTimestampType(
                ltz.isNullable,
                TimestampKind.REGULAR,
                ltz.getPrecision)
            case _ => throw new ValidationException("The supported time indicator type" +
              " are TIMESTAMP and TIMESTAMP_LTZ, but is " + f.getType + ".")
           }
        } else {
          f.getType
        }
      }
      factory.buildRelNodeRowType(fieldNames.asScala, fieldTypes)
    }
  }
}
