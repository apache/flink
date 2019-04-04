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

import org.apache.flink.table.calcite.FlinkRelFactories.{ExpandFactory, RankFactory, SinkFactory}
import org.apache.flink.table.expressions.WindowProperty
import org.apache.flink.table.runtime.rank.{RankRange, RankType}
import org.apache.flink.table.sinks.TableSink

import org.apache.calcite.config.{CalciteConnectionConfigImpl, CalciteConnectionProperty}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.tools.{FrameworkConfig, RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.{ImmutableBitSet, Util}

import java.util
import java.util.{Collections, Properties}

import scala.collection.JavaConversions._

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

  require(context != null)

  private val expandFactory: ExpandFactory = {
    Util.first(context.unwrap(classOf[ExpandFactory]), FlinkRelFactories.DEFAULT_EXPAND_FACTORY)
  }

  private val rankFactory: RankFactory = {
    Util.first(context.unwrap(classOf[RankFactory]), FlinkRelFactories.DEFAULT_RANK_FACTORY)
  }

  private val sinkFactory: SinkFactory = {
    Util.first(context.unwrap(classOf[SinkFactory]), FlinkRelFactories.DEFAULT_SINK_FACTORY)
  }

  def getRelOptSchema: RelOptSchema = relOptSchema

  def getPlanner: RelOptPlanner = cluster.getPlanner

  def getCluster: RelOptCluster = relOptCluster

  override def getTypeFactory: FlinkTypeFactory =
    super.getTypeFactory.asInstanceOf[FlinkTypeFactory]

  def expand(
      outputRowType: RelDataType,
      projects: util.List[util.List[RexNode]],
      expandIdIndex: Int): RelBuilder = {
    val input = build()
    val expand = expandFactory.createExpand(input, outputRowType, projects, expandIdIndex)
    push(expand)
  }

  def sink(sink: TableSink[_], sinkName: String): RelBuilder = {
    val input = build()
    val sinkNode = sinkFactory.createSink(input, sink, sinkName)
    push(sinkNode)
  }

  def rank(
      partitionKey: ImmutableBitSet,
      orderKey: RelCollation,
      rankType: RankType,
      rankRange: RankRange,
      rankNumberType: RelDataTypeField,
      outputRankNumber: Boolean): RelBuilder = {
    val input = build()
    val rank = rankFactory.createRank(input, partitionKey, orderKey, rankType, rankRange,
      rankNumberType, outputRankNumber)
    push(rank)
  }
}

object FlinkRelBuilder {

  def create(config: FrameworkConfig): FlinkRelBuilder = {

    // create Flink type factory
    val typeSystem = config.getTypeSystem
    val typeFactory = new FlinkTypeFactory(typeSystem)

    // create context instances with Flink type factory
    val context = config.getContext
    val planner = new VolcanoPlanner(config.getCostFactory, context)
    planner.setExecutor(config.getExecutor)
    config.getTraitDefs.foreach(planner.addRelTraitDef)

    val cluster = FlinkRelOptClusterFactory.create(planner, new RexBuilder(typeFactory))
    val calciteSchema = CalciteSchema.from(config.getDefaultSchema)

    val prop = new Properties()
    prop.setProperty(
      CalciteConnectionProperty.CASE_SENSITIVE.camelName,
      String.valueOf(config.getParserConfig.caseSensitive))
    val connectionConfig = new CalciteConnectionConfigImpl(prop)

    val relOptSchema = new FlinkCalciteCatalogReader(
      calciteSchema,
      Collections.emptyList(),
      typeFactory,
      connectionConfig)

    new FlinkRelBuilder(context, cluster, relOptSchema)
  }

  /**
    * Information necessary to create a window aggregate.
    *
    * Similar to [[RelBuilder.AggCall]] or [[RelBuilder.GroupKey]].
    */
  case class NamedWindowProperty(name: String, property: WindowProperty)

  def proto(context: Context): RelBuilderFactory = new RelBuilderFactory() {
    def create(cluster: RelOptCluster, schema: RelOptSchema): RelBuilder =
      new FlinkRelBuilder(context, cluster, schema)
  }
}
