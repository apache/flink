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
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.connector.source.abilities._
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource}
import org.apache.flink.table.factories.FactoryUtil
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.planner.catalog.CatalogSchemaTable
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext
import org.apache.flink.table.types.logical.{TimestampKind, TimestampType}
import org.apache.flink.types.RowKind

import org.apache.calcite.plan.{RelOptCluster, RelOptSchema, RelOptTable}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalTableScan

import java.util
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * A [[FlinkPreparingTableBase]] implementation which defines the interfaces required to translate
  * the Calcite [[RelOptTable]] to the Flink specific [[TableSourceTable]].
  *
  * <p>This table is only used to translate the catalog table into [[TableSourceTable]]
  * during the last phrase of sql-to-rel conversion, it is overdue once the sql node was converted
  * to relational expression.
  *
  * @param schemaTable Schema table which takes the variables needed to find the table source
  */
class CatalogSourceTable[T](
    relOptSchema: RelOptSchema,
    names: JList[String],
    rowType: RelDataType,
    val schemaTable: CatalogSchemaTable,
    val catalogTable: CatalogTable)
  extends FlinkPreparingTableBase(relOptSchema, rowType, names, schemaTable.getStatistic) {

  override def toRel(context: RelOptTable.ToRelContext): RelNode = {
    val cluster = context.getCluster
    val flinkContext = cluster
        .getPlanner
        .getContext
        .unwrap(classOf[FlinkContext])
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val conf = flinkContext.getTableConfig.getConfiguration
    val relBuilder = FlinkRelBuilder.of(cluster, getRelOptSchema)
    val toRexFactory = flinkContext.getSqlExprToRexConverterFactory

    // 1. push table scan
    val scan = buildTableScan(cluster, context.getTableHints, conf, typeFactory)
    relBuilder.push(scan)

    // 2. push computed column project
    if (containsGeneratedColumns(catalogTable)) {
      val fieldExprs = catalogTable.getSchema.getTableColumns
        .map(c => if (c.isGenerated) c.getExpr.get() else s"`${c.getName}`")
        .toArray
      val fieldNames = util.Arrays.asList(catalogTable.getSchema.getFieldNames: _*)
      val rexNodes = toRexFactory.create(scan.getRowType).convertToRexNodes(fieldExprs)
      relBuilder.projectNamed(rexNodes.toList, fieldNames, true)
    }

    // 3. push watermark assigner
    val watermarkSpec = catalogTable
      .getSchema
      // we only support single watermark currently
      .getWatermarkSpecs.asScala.headOption
    if (schemaTable.isStreamingMode && watermarkSpec.nonEmpty) {
      val rowtime = watermarkSpec.get.getRowtimeAttribute
      if (rowtime.contains(".")) {
        throw new TableException(
          s"Nested field '$rowtime' as rowtime attribute is not supported right now.")
      }
      val inputRowType = relBuilder.peek().getRowType
      val rowtimeIndex = inputRowType.getFieldNames.indexOf(rowtime)
      val watermarkRexNode = toRexFactory
          .create(inputRowType)
          .convertToRexNode(watermarkSpec.get.getWatermarkExpr)
      relBuilder.watermark(rowtimeIndex, watermarkRexNode)
    }

    // 4. returns the final RelNode
    relBuilder.build()
  }

  private def buildTableScan(
      cluster: RelOptCluster,
      hints: JList[RelHint],
      conf: ReadableConfig,
      typeFactory: FlinkTypeFactory): LogicalTableScan = {
    val hintedOptions = FlinkHints.getHintedOptions(hints)
    if (hintedOptions.nonEmpty
      && !conf.get(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED)) {
      throw new ValidationException(s"${FlinkHints.HINT_NAME_OPTIONS} hint is allowed only when "
        + s"${TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key} "
        + s"is set to true")
    }
    // Get row type of physical fields.
    // erase time indicator types in the rowType
    val erasedRowType = eraseTimeIndicator(rowType, typeFactory)
    val physicalFieldIndexes = catalogTable.getSchema.getTableColumns.zipWithIndex.flatMap {
      case (column, index) => if (column.isGenerated) None else Some(index)
    }.toArray
    val sourceRowType = typeFactory.projectStructType(erasedRowType, physicalFieldIndexes)

    val newCatalogTable = getOrCreateCatalogTableWithHints(hintedOptions)
    val tableSource = FactoryUtil.createTableSource(
      schemaTable.getCatalog,
      schemaTable.getTableIdentifier,
      newCatalogTable,
      conf,
      Thread.currentThread.getContextClassLoader)

    validateTableSource(tableSource)

    val tableSourceTable = new TableSourceTable(
      relOptSchema,
      schemaTable.getTableIdentifier,
      sourceRowType,
      statistic,
      tableSource,
      schemaTable.isStreamingMode,
      catalogTable,
      hintedOptions)
    LogicalTableScan.create(cluster, tableSourceTable, hints)
  }

  /**
   * Erases time indicators, i.e. converts rowtime and proctime type into regular timestamp type.
   * This is required before converting this [[CatalogSourceTable]] into multiple RelNodes,
   * otherwise the derived data types are mismatch.
   */
  private def eraseTimeIndicator(
      relDataType: RelDataType,
      factory: FlinkTypeFactory): RelDataType = {
    val logicalRowType = FlinkTypeFactory.toLogicalRowType(relDataType)
    val fieldNames = logicalRowType.getFieldNames
    val fieldTypes = logicalRowType.getFields.map { f =>
      if (FlinkTypeFactory.isTimeIndicatorType(f.getType)) {
        val timeIndicatorType = f.getType.asInstanceOf[TimestampType]
        new TimestampType(
          timeIndicatorType.isNullable,
          TimestampKind.REGULAR,
          timeIndicatorType.getPrecision)
      } else {
        f.getType
      }
    }
    factory.buildRelNodeRowType(fieldNames, fieldTypes)
  }

  /**
   * Returns true if there is any generated columns defined on the catalog table.
   */
  private def containsGeneratedColumns(catalogTable: CatalogTable): Boolean = {
    catalogTable.getSchema.getTableColumns.exists(_.isGenerated)
  }

  /**
   * Creates a new catalog table with the given hint options,
   * but return the original catalog table if the given hint options is empty.
   */
  private def getOrCreateCatalogTableWithHints(
      hintedOptions: JMap[String, String]): CatalogTable = {
    if (hintedOptions.nonEmpty) {
      catalogTable.copy(FlinkHints.mergeTableOptions(hintedOptions, catalogTable.getOptions))
    } else {
      catalogTable
    }
  }

  private def validateTableSource(tableSource: DynamicTableSource): Unit = {
    // throw exception if unsupported ability interface is implemented
    val unsupportedAbilities = List(
      classOf[SupportsFilterPushDown],
      classOf[SupportsLimitPushDown],
      classOf[SupportsPartitionPushDown],
      classOf[SupportsComputedColumnPushDown],
      classOf[SupportsWatermarkPushDown])
    unsupportedAbilities.foreach { ability =>
      if (ability.isAssignableFrom(tableSource.getClass)) {
        throw new UnsupportedOperationException("Currently, a DynamicTableSource with " +
          s"${ability.getSimpleName} ability is not supported.")
      }
    }

    // scan validation
    val tableName = schemaTable.getTableIdentifier.asSummaryString
    tableSource match {
      case ts: ScanTableSource =>
        val changelogMode = ts.getChangelogMode
        if (!schemaTable.isStreamingMode) {
          // batch only supports bounded source
          val provider = ts.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE)
          if (!provider.isBounded) {
            throw new ValidationException("Cannot query on an unbounded source in batch mode, " +
              s"but '$tableName' is unbounded.")
          }
          // batch only supports INSERT only source
          if (!changelogMode.containsOnly(RowKind.INSERT)) {
            throw new UnsupportedOperationException(
              "Currently, batch mode only supports INSERT only source, but " +
              s"'$tableName' source produces not INSERT only messages")
          }
        } else {
          // sanity check for produced ChangelogMode
          val hasUpdateBefore = changelogMode.contains(RowKind.UPDATE_BEFORE)
          val hasUpdateAfter = changelogMode.contains(RowKind.UPDATE_AFTER)
          (hasUpdateBefore, hasUpdateAfter) match {
            case (true, true) =>
              // UPDATE_BEFORE and UPDATE_AFTER, pass
            case (false, true) =>
              // only UPDATE_AFTER
              throw new UnsupportedOperationException(
                "Currently, ScanTableSource doesn't support producing ChangelogMode " +
                  "which contains UPDATE_AFTER but no UPDATE_BEFORE. Please adapt the " +
                  s"implementation of '${ts.asSummaryString()}' source.")
            case (true, false) =>
               // only UPDATE_BEFORE
              throw new ValidationException(
                s"'$tableName' source produces ChangelogMode which " +
                  s"contains UPDATE_BEFORE but doesn't contain UPDATE_AFTER, this is invalid.")
            case _ =>
              // no updates, pass
          }

          // watermark defined on a changelog source is not supported
          if (!catalogTable.getSchema.getWatermarkSpecs.isEmpty &&
              !changelogMode.containsOnly(RowKind.INSERT)) {
            throw new UnsupportedOperationException(
              "Currently, defining WATERMARK on a changelog source is not supported.")
          }
        }
      case _ =>
        // pass, lookup table source is validated in LookupJoin node
    }
  }
}
