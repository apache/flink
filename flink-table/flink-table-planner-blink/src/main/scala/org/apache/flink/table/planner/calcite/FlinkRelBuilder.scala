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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.planner.calcite.FlinkRelFactories.{ExpandFactory, RankFactory}
import org.apache.flink.table.planner.expressions.{PlannerNamedWindowProperty, WindowProperty}
import org.apache.flink.table.planner.plan.QueryOperationConverter
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalTableAggregate, LogicalWatermarkAssigner, LogicalWindowAggregate, LogicalWindowTableAggregate}
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.table.runtime.operators.rank.{RankRange, RankType}

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.RelBuilder.{AggCall, Config, GroupKey}
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.{ImmutableBitSet, Util}

import java.lang.Iterable
import java.util
import java.util.List
import java.util.function.UnaryOperator

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

  private val toRelNodeConverter = {
    new QueryOperationConverter(this)
  }

  private val expandFactory: ExpandFactory = {
    Util.first(context.unwrap(classOf[ExpandFactory]), FlinkRelFactories.DEFAULT_EXPAND_FACTORY)
  }

  private val rankFactory: RankFactory = {
    Util.first(context.unwrap(classOf[RankFactory]), FlinkRelFactories.DEFAULT_RANK_FACTORY)
  }

  override def getRelOptSchema: RelOptSchema = relOptSchema

  override def getCluster: RelOptCluster = relOptCluster

  override def getTypeFactory: FlinkTypeFactory =
    super.getTypeFactory.asInstanceOf[FlinkTypeFactory]

  override def transform(transform: UnaryOperator[RelBuilder.Config]): FlinkRelBuilder = {
    // Override in order to return a FlinkRelBuilder.
    FlinkRelBuilder.of(transform.apply(Config.DEFAULT), cluster, relOptSchema)
  }

  def expand(
      outputRowType: RelDataType,
      projects: util.List[util.List[RexNode]],
      expandIdIndex: Int): RelBuilder = {
    val input = build()
    val expand = expandFactory.createExpand(input, outputRowType, projects, expandIdIndex)
    push(expand)
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

  /**
    * Build non-window aggregate for either aggregate or table aggregate.
    */
  override def aggregate(groupKey: GroupKey, aggCalls: Iterable[AggCall]): RelBuilder = {
    // build a relNode, the build() may also return a project
    val relNode = super.aggregate(groupKey, aggCalls).build()

    def isCountStartAgg(agg: LogicalAggregate): Boolean = {
      if (agg.getGroupCount != 0 || agg.getAggCallList.size() != 1) {
        return false
      }
      val call = agg.getAggCallList.head
      call.getAggregation.getKind == SqlKind.COUNT &&
          call.filterArg == -1 && call.getArgList.isEmpty
    }

    relNode match {
      case logicalAggregate: LogicalAggregate
        if AggregateUtil.isTableAggregate(logicalAggregate.getAggCallList) =>
        push(LogicalTableAggregate.create(logicalAggregate))
      case logicalAggregate2: LogicalAggregate
        if isCountStartAgg(logicalAggregate2) =>
        val newAggInput = push(logicalAggregate2.getInput(0))
            .project(literal(0)).build()
        push(logicalAggregate2.copy(logicalAggregate2.getTraitSet, ImmutableList.of(newAggInput)))
      case _ => push(relNode)
    }
  }

  /**
    * Build window aggregate for either aggregate or table aggregate.
    */
  def windowAggregate(
      window: LogicalWindow,
      groupKey: GroupKey,
      namedProperties: List[PlannerNamedWindowProperty],
      aggCalls: Iterable[AggCall]): RelBuilder = {
    // build logical aggregate

    // Because of:
    // [CALCITE-3763] RelBuilder.aggregate should prune unused fields from the input,
    // if the input is a Project.
    //
    // the field can not be pruned if it is referenced by other expressions
    // of the window aggregation(i.e. the TUMBLE_START/END).
    // To solve this, we config the RelBuilder to forbidden this feature.
    val aggregate = super.transform(
      new UnaryOperator[RelBuilder.Config] {
        override def apply(t: RelBuilder.Config)
          : RelBuilder.Config = t.withPruneInputOfAggregate(false)
      })
      .push(build())
      .aggregate(groupKey, aggCalls)
      .build()
      .asInstanceOf[LogicalAggregate]

    // build logical window aggregate from it
    aggregate match {
      case logicalAggregate: LogicalAggregate
        if AggregateUtil.isTableAggregate(logicalAggregate.getAggCallList) =>
        push(LogicalWindowTableAggregate.create(window, namedProperties, aggregate))
      case _ => push(LogicalWindowAggregate.create(window, namedProperties, aggregate))
    }
  }

  /**
    * Build watermark assigner relation node.
    */
  def watermark(rowtimeFieldIndex: Int, watermarkExpr: RexNode): RelBuilder = {
    val input = build()
    val watermarkAssigner = LogicalWatermarkAssigner
      .create(cluster, input, rowtimeFieldIndex, watermarkExpr)
    push(watermarkAssigner)
    this
  }

  def queryOperation(queryOperation: QueryOperation): RelBuilder = {
    val relNode = queryOperation.accept(toRelNodeConverter)
    push(relNode)
    this
  }
}

object FlinkRelBuilder {

  case class NamedWindowProperty(name: String, property: WindowProperty)

  def proto(context: Context): RelBuilderFactory = new RelBuilderFactory() {
    def create(cluster: RelOptCluster, schema: RelOptSchema): RelBuilder = {
      val clusterContext = cluster.getPlanner.getContext.unwrap(classOf[FlinkContext])
      val mergedContext = Contexts.chain(context, clusterContext)

      new FlinkRelBuilder(mergedContext, cluster, schema)
    }
  }

  def of(cluster: RelOptCluster, relOptSchema: RelOptSchema): FlinkRelBuilder = {
    val clusterContext = cluster.getPlanner.getContext
    new FlinkRelBuilder(
      clusterContext,
      cluster,
      relOptSchema)
  }

  def of(contextVar: Object, cluster: RelOptCluster, relOptSchema: RelOptSchema)
    : FlinkRelBuilder = {
    val mergedContext = Contexts.of(contextVar, cluster.getPlanner.getContext)
    new FlinkRelBuilder(
      mergedContext,
      cluster,
      relOptSchema)
  }
}
