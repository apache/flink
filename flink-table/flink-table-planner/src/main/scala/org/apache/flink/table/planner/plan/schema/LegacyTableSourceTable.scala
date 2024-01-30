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

import org.apache.flink.shaded.guava31.com.google.common.base.Preconditions
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.TableSource

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptSchema, RelOptTable}
import org.apache.calcite.rel.`type`.RelDataType

import java.util
import java.util.{Collections, List => JList}

/**
 * A [[FlinkPreparingTableBase]] implementation which defines the context variables required to
 * translate the Calcite [[RelOptTable]] to the Flink specific relational expression with
 * [[TableSource]].
 *
 * <p>It also defines the [[copy]] method used for push down rules.
 *
 * @param tableIdentifier
 *   full path of the table to retrieve.
 * @param tableSource
 *   The [[TableSource]] for which is converted to a Calcite Table
 * @param isStreamingMode
 *   A flag that tells if the current table is in stream mode
 * @param catalogTable
 *   Catalog table where this table source table comes from
 */
class LegacyTableSourceTable[T](
    relOptSchema: RelOptSchema,
    val tableIdentifier: ObjectIdentifier,
    rowType: RelDataType,
    statistic: FlinkStatistic,
    val tableSource: TableSource[T],
    val isStreamingMode: Boolean,
    val catalogTable: CatalogTable,
    val dynamicOptions: JMap[String, String])
  extends FlinkPreparingTableBase(
    relOptSchema,
    rowType,
    util.Arrays.asList(
      tableIdentifier.getCatalogName,
      tableIdentifier.getDatabaseName,
      tableIdentifier.getObjectName),
    statistic) {

  def this(
      relOptSchema: RelOptSchema,
      tableIdentifier: ObjectIdentifier,
      rowType: RelDataType,
      statistic: FlinkStatistic,
      tableSource: TableSource[T],
      isStreamingMode: Boolean,
      catalogTable: CatalogTable) = this(
    relOptSchema,
    tableIdentifier,
    rowType,
    statistic,
    tableSource,
    isStreamingMode,
    catalogTable,
    Collections.emptyMap())

  Preconditions.checkNotNull(tableSource)
  Preconditions.checkNotNull(statistic)
  Preconditions.checkNotNull(catalogTable)

  override def getQualifiedName: JList[String] = {
    val sourceExplain = explainSourceAsString(tableSource)
    if (dynamicOptions.size() == 0) {
      sourceExplain
    } else {
      // Add the dynamic options as part of the table digest,
      // this is a temporary solution, we expect to avoid this
      // before Calcite 1.23.0.
      ImmutableList
        .builder[String]()
        .addAll(sourceExplain)
        .add(s"dynamic options: $dynamicOptions")
        .build()
    }
  }

  /**
   * Creates a copy of this table, changing table source and statistic.
   *
   * @param tableSource
   *   tableSource to replace
   * @param statistic
   *   New FlinkStatistic to replace
   * @return
   *   New TableSourceTable instance with specified table source and [[FlinkStatistic]]
   */
  def copy(tableSource: TableSource[_], statistic: FlinkStatistic): LegacyTableSourceTable[T] = {
    new LegacyTableSourceTable[T](
      relOptSchema,
      tableIdentifier,
      rowType,
      statistic,
      tableSource.asInstanceOf[TableSource[T]],
      isStreamingMode,
      catalogTable,
      dynamicOptions)
  }

  /**
   * Creates a copy of this table, changing table source and rowType based on selected fields.
   *
   * @param tableSource
   *   tableSource to replace
   * @param selectedFields
   *   Selected indices of the table source output fields
   * @return
   *   New TableSourceTable instance with specified table source and selected fields
   */
  def copy(tableSource: TableSource[_], selectedFields: Array[Int]): LegacyTableSourceTable[T] = {
    val newRowType = relOptSchema.getTypeFactory
      .asInstanceOf[FlinkTypeFactory]
      .projectStructType(rowType, selectedFields)
    new LegacyTableSourceTable[T](
      relOptSchema,
      tableIdentifier,
      newRowType,
      statistic,
      tableSource.asInstanceOf[TableSource[T]],
      isStreamingMode,
      catalogTable,
      dynamicOptions)
  }
}
