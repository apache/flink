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

import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.sources.TableSourceUtil
import org.apache.flink.table.sources.{TableSource, TableSourceValidation}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions
import org.apache.flink.table.api.{TableException, WatermarkSpec}
import org.apache.flink.table.types.logical.{TimestampKind, TimestampType}

import scala.collection.JavaConverters._

/**
  * Abstract class which define the interfaces required to convert a [[TableSource]] to
  * a Calcite Table
  *
  * @param tableSource The [[TableSource]] for which is converted to a Calcite Table.
  * @param isStreamingMode A flag that tells if the current table is in stream mode.
  * @param statistic The table statistics.
  */
class TableSourceTable[T](
    val tableSource: TableSource[T],
    val isStreamingMode: Boolean,
    val statistic: FlinkStatistic,
    val selectedFields: Option[Array[Int]],
    val catalogTable: CatalogTable)
  extends FlinkTable {

  def this(
      tableSource: TableSource[T],
      isStreamingMode: Boolean,
      statistic: FlinkStatistic,
      catalogTable: CatalogTable) {
    this(tableSource, isStreamingMode, statistic, None, catalogTable)
  }

  Preconditions.checkNotNull(tableSource)
  Preconditions.checkNotNull(statistic)
  Preconditions.checkNotNull(catalogTable)

  val watermarkSpec: Option[WatermarkSpec] = catalogTable
    .getSchema
    // we only support single watermark currently
    .getWatermarkSpecs.asScala.headOption

  if (TableSourceValidation.hasRowtimeAttribute(tableSource) && watermarkSpec.isDefined) {
    throw new TableException(
        "If watermark is specified in DDL, the underlying TableSource of connector shouldn't" +
          " return an non-empty list of RowtimeAttributeDescriptor" +
          " via DefinedRowtimeAttributes interface.")
  }

  // TODO implements this
  // TableSourceUtil.validateTableSource(tableSource)

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val factory = typeFactory.asInstanceOf[FlinkTypeFactory]
    val (fieldNames, fieldTypes) = TableSourceUtil.getFieldNamesTypes(
      tableSource,
      selectedFields,
      streaming = isStreamingMode)
    // patch rowtime field according to WatermarkSpec
    val patchedTypes = if (isStreamingMode && watermarkSpec.isDefined) {
      // TODO: [FLINK-14473] we only support top-level rowtime attribute right now
      val rowtime = watermarkSpec.get.getRowtimeAttribute
      if (rowtime.contains(".")) {
        throw new TableException(
          s"Nested field '$rowtime' as rowtime attribute is not supported right now.")
      }
      val idx = fieldNames.indexOf(rowtime)
      val originalType = fieldTypes(idx).asInstanceOf[TimestampType]
      val rowtimeType = new TimestampType(
        originalType.isNullable,
        TimestampKind.ROWTIME,
        originalType.getPrecision)
      fieldTypes.patch(idx, Seq(rowtimeType), 1)
    } else {
      fieldTypes
    }
    factory.buildRelNodeRowType(fieldNames, patchedTypes)
  }

  /**
    * Creates a copy of this table, changing statistic.
    *
    * @param statistic A new FlinkStatistic.
    * @return Copy of this table, substituting statistic.
    */
  override def copy(statistic: FlinkStatistic): TableSourceTable[T] = {
    new TableSourceTable(tableSource, isStreamingMode, statistic, catalogTable)
  }

  /**
    * Returns statistics of current table.
    */
  override def getStatistic: FlinkStatistic = statistic

  /**
    * Replaces table source with the given one, and create a new table source table.
    *
    * @param tableSource tableSource to replace.
    * @return new TableSourceTable
    */
  def replaceTableSource(tableSource: TableSource[T]): TableSourceTable[T] = {
    new TableSourceTable[T](
      tableSource, isStreamingMode, statistic, catalogTable)
  }
}
