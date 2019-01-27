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

package org.apache.flink.table.calcite

import java.lang.Iterable
import java.util

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.SqlRankFunction
import org.apache.calcite.tools.RelBuilder.{AggCall, GroupKey}
import org.apache.calcite.tools.{FrameworkConfig, RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.{ImmutableBitSet, Util}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkRelFactories.{ExpandFactory, SinkFactory}
import org.apache.flink.table.catalog.CatalogManager
import org.apache.flink.table.expressions.WindowProperty
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.nodes.calcite.{LogicalRank, LogicalWindowAggregate}
import org.apache.flink.table.plan.util.RankRange
import org.apache.flink.table.sinks.TableSink

/**
  * Flink specific [[RelBuilder]] that changes the default type factory to a [[FlinkTypeFactory]].
  */
class FlinkRelBuilder(
    context: Context,
    relOptCluster: RelOptCluster,
    relOptSchema: RelOptSchema)
  extends RelBuilder(
    context,
    relOptCluster,
    relOptSchema) {

  private val expandFactory: ExpandFactory = {
    Util.first(
      getContext.unwrap(classOf[ExpandFactory]),
      FlinkRelFactories.DEFAULT_EXPAND_FACTORY)
  }

  private val sinkFactory: SinkFactory = {
    Util.first(
      getContext.unwrap(classOf[SinkFactory]),
      FlinkRelFactories.DEFAULT_SINK_FACTORY)
  }

  private def getContext = if (context == null) {
    Contexts.EMPTY_CONTEXT
  } else {
    context
  }

  def getRelOptSchema: RelOptSchema = relOptSchema

  def getPlanner: RelOptPlanner = cluster.getPlanner

  def getCluster: RelOptCluster = relOptCluster

  override def getTypeFactory: FlinkTypeFactory =
    super.getTypeFactory.asInstanceOf[FlinkTypeFactory]

  def aggregate(
      window: LogicalWindow,
      groupKey: GroupKey,
      namedProperties: Seq[NamedWindowProperty],
      aggCalls: Iterable[AggCall])
    : RelBuilder = {
    // build logical aggregate
    val aggregate = super.aggregate(groupKey, aggCalls).build().asInstanceOf[LogicalAggregate]

    // build logical window aggregate from it
    push(LogicalWindowAggregate.create(window, namedProperties, aggregate))
    this
  }

  def expand(
      outputRowType: RelDataType,
      projects: util.List[util.List[RexNode]],
      expandIdIndex: Int): RelBuilder = {
    val input = build()
    val expand = expandFactory.createExpand(input, outputRowType, projects, expandIdIndex)
    push(expand)
  }

  def sink(
    sink: TableSink[_],
    sinkName: String): RelBuilder = {
    val input = build()
    val sinkNode = sinkFactory.createSink(input, sink, sinkName)
    push(sinkNode)
  }

  def rank(
      rankFunction: SqlRankFunction,
      partitionKey: ImmutableBitSet,
      sortCollation: RelCollation,
      rankRange: RankRange): RelBuilder = {
    val input = build()
    val rank = LogicalRank.create(input, rankFunction, partitionKey, sortCollation, rankRange)
    push(rank)
  }
}

object FlinkRelBuilder {

  def create(config: FrameworkConfig,
      tableConfig: TableConfig,
      typeFactory: RelDataTypeFactory,
      traitDefs: Array[RelTraitDef[_ <: RelTrait]] = Array(ConventionTraitDef.INSTANCE),
      catalogManager: CatalogManager)
      : FlinkRelBuilder = {

    // create context instances with Flink type factory
    val planner = new VolcanoPlanner(
      config.getCostFactory, FlinkChainContext.chain(Contexts.of(tableConfig)))
    planner.setExecutor(config.getExecutor)
    traitDefs.foreach(planner.addRelTraitDef)
    val cluster = FlinkRelOptClusterFactory.create(planner, new RexBuilder(typeFactory))
    val calciteSchema = CalciteSchema.from(config.getDefaultSchema)
    val relOptSchema = new FlinkCalciteCatalogReader(
      calciteSchema,
      catalogManager.getCalciteReaderDefaultPaths(config.getDefaultSchema),
      typeFactory,
      CalciteConfig.connectionConfig(config.getParserConfig))

    new FlinkRelBuilder(config.getContext, cluster, relOptSchema)
  }

  /**
    * Information necessary to create a window aggregate.
    *
    * Similar to [[RelBuilder.AggCall]] or [[RelBuilder.GroupKey]].
    */
  case class NamedWindowProperty(name: String, property: WindowProperty)

  def proto(context: Context) = new RelBuilderFactory() {
    def create(cluster: RelOptCluster, schema: RelOptSchema): RelBuilder =
      new FlinkRelBuilder(context, cluster, schema)
  }
}
