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

import org.apache.flink.table.catalog.ContextResolvedTable
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.connectors.DynamicSourceUtils
import org.apache.flink.table.planner.plan.abilities.source.{SourceAbilityContext, SourceAbilitySpec}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptSchema
import org.apache.calcite.rel.`type`.RelDataType

import java.util

/**
 * A [[FlinkPreparingTableBase]] implementation which defines the context variables required to
 * translate the Calcite [[org.apache.calcite.plan.RelOptTable]] to the Flink specific relational
 * expression with [[DynamicTableSource]].
 *
 * @param relOptSchema
 *   The RelOptSchema that this table comes from
 * @param rowType
 *   The table row type
 * @param statistic
 *   The table statistics
 * @param tableSource
 *   The [[DynamicTableSource]] for which is converted to a Calcite Table
 * @param isStreamingMode
 *   A flag that tells if the current table is in stream mode
 * @param contextResolvedTable
 *   Resolved catalog table where this table source table comes from
 * @param flinkContext
 *   The flink context which is used to generate extra digests based on abilitySpecs
 * @param abilitySpecs
 *   The abilitySpecs applied to the source
 */
class TableSourceTable(
    relOptSchema: RelOptSchema,
    rowType: RelDataType,
    statistic: FlinkStatistic,
    val tableSource: DynamicTableSource,
    val isStreamingMode: Boolean,
    val contextResolvedTable: ContextResolvedTable,
    val flinkContext: FlinkContext,
    val flinkTypeFactory: FlinkTypeFactory,
    val abilitySpecs: Array[SourceAbilitySpec] = Array.empty)
  extends FlinkPreparingTableBase(
    relOptSchema,
    rowType,
    contextResolvedTable.getIdentifier.toList,
    statistic) {

  override def getQualifiedName: util.List[String] = {
    val builder = ImmutableList
      .builder[String]()
      .addAll(super.getQualifiedName)
    builder.addAll(getSpecDigests)
    builder.build()
  }

  def getSpecDigests: util.List[String] = {
    val builder = ImmutableList.builder[String]()
    if (abilitySpecs != null && abilitySpecs.length != 0) {
      var newProducedType =
        DynamicSourceUtils.createProducedType(contextResolvedTable.getResolvedSchema, tableSource)

      for (spec <- abilitySpecs) {
        val sourceAbilityContext =
          new SourceAbilityContext(flinkContext, flinkTypeFactory, newProducedType)

        builder.add(spec.getDigests(sourceAbilityContext))
        newProducedType = spec.getProducedType.orElse(newProducedType)
      }
    }
    builder.build()
  }

  /** Adds the newSpec replacing any spec of the same class from existing ones. */
  private def mergeSpecs(
      original: Array[SourceAbilitySpec],
      newSpec: Array[SourceAbilitySpec]): Array[SourceAbilitySpec] = {
    original.filter(old => !newSpec.exists(n => old.getClass == n.getClass)) ++ newSpec
  }

  /**
   * Creates a copy of this table with specified digest.
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newRowType
   *   new row type
   * @return
   *   added TableSourceTable instance with specified digest
   */
  def copy(
      newTableSource: DynamicTableSource,
      newRowType: RelDataType,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      newRowType,
      statistic,
      newTableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      mergeSpecs(abilitySpecs, newAbilitySpecs)
    )
  }

  /**
   * Creates a copy of this table with specified digest and context resolved table
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newResolveTable
   *   resolved table to replace
   * @param newRowType
   *   new row type
   * @return
   *   added TableSourceTable instance with specified digest
   */
  def copy(
      newTableSource: DynamicTableSource,
      newResolveTable: ContextResolvedTable,
      newRowType: RelDataType,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      newRowType,
      statistic,
      newTableSource,
      isStreamingMode,
      newResolveTable,
      flinkContext,
      flinkTypeFactory,
      mergeSpecs(abilitySpecs, newAbilitySpecs)
    )
  }

  /**
   * Creates a copy of this table with replaced ability specs.
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newRowType
   *   new row type
   */
  def replace(
      newTableSource: DynamicTableSource,
      newRowType: RelDataType,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      newRowType,
      statistic,
      newTableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      newAbilitySpecs
    )
  }

  /**
   * Creates a copy of this table with specified digest and statistic.
   *
   * @param newTableSource
   *   tableSource to replace
   * @param newStatistic
   *   statistic to replace
   * @return
   *   added TableSourceTable instance with specified digest and statistic
   */
  def copy(
      newTableSource: DynamicTableSource,
      newStatistic: FlinkStatistic,
      newAbilitySpecs: Array[SourceAbilitySpec]): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      rowType,
      newStatistic,
      newTableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      mergeSpecs(abilitySpecs, newAbilitySpecs))
  }

  /**
   * Creates a copy of this table, changing the statistic
   *
   * @param newStatistic
   *   new table statistic
   * @return
   *   New TableSourceTable instance with new statistic
   */
  def copy(newStatistic: FlinkStatistic): TableSourceTable = {
    new TableSourceTable(
      relOptSchema,
      rowType,
      newStatistic,
      tableSource,
      isStreamingMode,
      contextResolvedTable,
      flinkContext,
      flinkTypeFactory,
      abilitySpecs)
  }
}
