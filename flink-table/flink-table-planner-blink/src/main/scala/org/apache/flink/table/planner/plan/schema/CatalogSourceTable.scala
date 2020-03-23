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
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.factories.{TableFactoryUtil, TableSourceFactory, TableSourceFactoryContextImpl}
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.planner.catalog.CatalogSchemaTable
import org.apache.flink.table.sources.{StreamTableSource, TableSource, TableSourceValidation}
import org.apache.flink.table.utils.TableConnectorUtils.generateRuntimeName
import org.apache.calcite.plan.{RelOptSchema, RelOptTable}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.flink.table.types.logical.{TimestampKind, TimestampType}

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

  lazy val columnExprs: Map[String, String] = {
    catalogTable.getSchema
      .getTableColumns
      .filter(column => column.isGenerated)
      .map(column => (column.getName, column.getExpr.get()))
      .toMap
  }

  override def getQualifiedName: JList[String] = {
    // Do not explain source, we already have full names, table source should be created in toRel.
    val ret = new util.ArrayList[String](names)
    // Add class name to distinguish TableSourceTable.
    val name = generateRuntimeName(getClass, catalogTable.getSchema.getFieldNames)
    ret.add(s"catalog_source: [$name]")
    ret
  }

  override def toRel(context: RelOptTable.ToRelContext): RelNode = {
    val cluster = context.getCluster
    val flinkContext = cluster
        .getPlanner
        .getContext
        .unwrap(classOf[FlinkContext])
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    // erase time indicator types in the rowType
    val erasedRowType = eraseTimeIndicator(rowType, typeFactory)

    val tableSource = findAndCreateTableSource(flinkContext.getTableConfig.getConfiguration)
    val tableSourceTable = new TableSourceTable[T](
      relOptSchema,
      schemaTable.getTableIdentifier,
      erasedRowType,
      statistic,
      tableSource,
      schemaTable.isStreamingMode,
      catalogTable)

    // 1. push table scan

    // Get row type of physical fields.
    val physicalFields = getRowType
      .getFieldList
      .filter(f => !columnExprs.contains(f.getName))
      .map(f => f.getIndex)
      .toArray
    // Copy this table with physical scan row type.
    val newRelTable = tableSourceTable.copy(tableSource, physicalFields)
    val scan = LogicalTableScan.create(cluster, newRelTable)
    val relBuilder = FlinkRelBuilder.of(cluster, getRelOptSchema)
    relBuilder.push(scan)

    val toRexFactory = flinkContext.getSqlExprToRexConverterFactory

    // 2. push computed column project
    val fieldNames = erasedRowType.getFieldNames.asScala
    if (columnExprs.nonEmpty) {
      val fieldExprs = fieldNames
          .map { name =>
            if (columnExprs.contains(name)) {
              columnExprs(name)
            } else {
              s"`$name`"
            }
          }.toArray
      val rexNodes = toRexFactory.create(newRelTable.getRowType).convertToRexNodes(fieldExprs)
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
          .create(erasedRowType)
          .convertToRexNode(watermarkSpec.get.getWatermarkExpr)
      relBuilder.watermark(rowtimeIndex, watermarkRexNode)
    }

    // 4. returns the final RelNode
    relBuilder.build()
  }

  /** Create the table source. */
  private def findAndCreateTableSource(conf: ReadableConfig): TableSource[T] = {
    val tableFactoryOpt = schemaTable.getTableFactory
    val context = new TableSourceFactoryContextImpl(
      schemaTable.getTableIdentifier, catalogTable, conf)
    val tableSource = if (tableFactoryOpt.isPresent) {
      tableFactoryOpt.get() match {
        case tableSourceFactory: TableSourceFactory[_] =>
          tableSourceFactory.createTableSource(context)
        case _ => throw new ValidationException("Cannot query a sink-only table. "
          + "TableFactory provided by catalog must implement TableSourceFactory")
      }
    } else {
      TableFactoryUtil.findAndCreateTableSource(context)
    }

    // validation
    val tableName = schemaTable.getTableIdentifier.asSummaryString();
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
}
