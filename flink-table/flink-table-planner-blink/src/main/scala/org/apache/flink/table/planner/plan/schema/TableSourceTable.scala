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
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{TableSource, TableSourceValidation}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions
import org.apache.flink.table.api.{TableException, WatermarkSpec}

import org.apache.calcite.plan.{RelOptSchema, RelOptTable}

import java.util.{List => JList}

import scala.collection.JavaConverters._

/**
  * A [[FlinkPreparingTableBase]] implementation which defines the context variables
  * required to translate the Calcite [[RelOptTable]] to the Flink specific
  * relational expression with [[TableSource]].
  *
  * <p>It also defines the [[copy]] method used for push down rules.
  *
  * @param tableSource The [[TableSource]] for which is converted to a Calcite Table
  * @param isStreamingMode A flag that tells if the current table is in stream mode
  * @param catalogTable Catalog table where this table source table comes from
  */
class TableSourceTable[T](
    relOptSchema: RelOptSchema,
    names: JList[String],
    rowType: RelDataType,
    statistic: FlinkStatistic,
    val tableSource: TableSource[T],
    val isStreamingMode: Boolean,
    val catalogTable: CatalogTable)
  extends FlinkPreparingTableBase(relOptSchema, rowType, names, statistic) {

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

  override def getQualifiedName: JList[String] = explainSourceAsString(tableSource)

  /**
    * Creates a copy of this table, changing table source and statistic.
    *
    * @param tableSource tableSource to replace
    * @param statistic New FlinkStatistic to replace
    * @return New TableSourceTable instance with specified table source and [[FlinkStatistic]]
    */
  def copy(tableSource: TableSource[_], statistic: FlinkStatistic): TableSourceTable[T] = {
    new TableSourceTable[T](relOptSchema, names, rowType, statistic,
      tableSource.asInstanceOf[TableSource[T]], isStreamingMode, catalogTable)
  }

  /**
    * Creates a copy of this table, changing table source and rowType based on
    * selected fields.
    *
    * @param tableSource tableSource to replace
    * @param selectedFields Selected indices of the table source output fields
    * @return New TableSourceTable instance with specified table source
    *         and selected fields
    */
  def copy(tableSource: TableSource[_], selectedFields: Array[Int]): TableSourceTable[T] = {
    val newRowType = relOptSchema
      .getTypeFactory
      .createStructType(
        selectedFields
          .map(idx => rowType.getFieldList.get(idx))
          .toList
          .asJava)
    new TableSourceTable[T](relOptSchema, names, newRowType, statistic,
      tableSource.asInstanceOf[TableSource[T]], isStreamingMode, catalogTable)
  }
}
