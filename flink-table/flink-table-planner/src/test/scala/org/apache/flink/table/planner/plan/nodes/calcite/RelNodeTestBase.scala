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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.calcite.{FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.delegation.PlannerContext
import org.apache.flink.table.planner.plan.metadata.MockMetaTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.utils.CatalogManagerMocks

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.{Convention, ConventionTraitDef, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.RelCollationTraitDef
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.junit.Before

import java.util

/**
 * A base class for rel node test.
 * TODO refactor the metadata test to extract the common logic for all related tests.
 */
class RelNodeTestBase {
  val tableConfig = new TableConfig()
  val rootSchema: SchemaPlus = CalciteSchema.createRootSchema(true, false).plus()
  val catalogManager: CatalogManager = CatalogManagerMocks.createEmptyCatalogManager()
  val moduleManager = new ModuleManager

  val plannerContext: PlannerContext = new PlannerContext(
    tableConfig,
    new FunctionCatalog(tableConfig, catalogManager, moduleManager),
    catalogManager,
    CalciteSchema.from(rootSchema),
    util.Arrays.asList(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE
      )
    )

  val typeFactory: FlinkTypeFactory = plannerContext.getTypeFactory
  var relBuilder: FlinkRelBuilder = _
  var rexBuilder: RexBuilder = _
  var cluster: RelOptCluster = _
  var logicalTraits: RelTraitSet = _

  @Before
  def setUp(): Unit = {
    relBuilder = plannerContext.createRelBuilder("default_catalog", "default_database")
    rexBuilder = relBuilder.getRexBuilder
    cluster = relBuilder.getCluster
    logicalTraits = cluster.traitSetOf(Convention.NONE)
  }

  /**
   * Build a [[LogicalTableScan]] based on a [[MockMetaTable]] using given field names and types.
    @param fieldNames String array
   * @param fieldTypes [[LogicalType]] array
   * @return a [[LogicalTableScan]]
   */
  def buildLogicalTableScan(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): LogicalTableScan = {
    val flinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    val rowType = flinkTypeFactory.buildRelNodeRowType(fieldNames, fieldTypes)
    val table = new MockMetaTable(rowType, FlinkStatistic.UNKNOWN)
    LogicalTableScan.create(cluster, table, new util.ArrayList[RelHint]())
  }
}
