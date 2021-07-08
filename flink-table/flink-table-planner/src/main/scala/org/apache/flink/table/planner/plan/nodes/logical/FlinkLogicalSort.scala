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

package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalSortRule.TABLE_EXEC_RANGE_SORT_ENABLED
import org.apache.flink.table.planner.plan.utils.SortUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName

/**
  * Sub-class of [[Sort]] that is a relational expression which imposes
  * a particular sort order on its input without otherwise changing its content in Flink.
  */
class FlinkLogicalSort(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    child: RelNode,
    collation: RelCollation,
    sortOffset: RexNode,
    sortFetch: RexNode)
  extends Sort(cluster, traits, child, collation, sortOffset, sortFetch)
  with FlinkLogicalRel {

  private lazy val limitStart: Long = SortUtil.getLimitStart(offset)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new FlinkLogicalSort(cluster, traitSet, newInput, newCollation, offset, fetch)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val inputRowCnt = mq.getRowCount(this.getInput)
    if (inputRowCnt == null) {
      inputRowCnt
    } else {
      val rowCount = (inputRowCnt - limitStart).max(1.0)
      if (fetch != null) {
        val limit = RexLiteral.intValue(fetch)
        rowCount.min(limit)
      } else {
        rowCount
      }
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // by default, assume cost is proportional to number of rows
    val rowCount: Double = mq.getRowCount(this)
    planner.getCostFactory.makeCost(rowCount, rowCount, 0)
  }

}

class FlinkLogicalSortStreamConverter
  extends ConverterRule(
    classOf[LogicalSort],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalSortStreamConverter") {

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[LogicalSort]
    val newInput = RelOptRule.convert(sort.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalSort.create(newInput, sort.getCollation, sort.offset, sort.fetch)
  }
}

class FlinkLogicalSortBatchConverter extends ConverterRule(
  classOf[LogicalSort],
  Convention.NONE,
  FlinkConventions.LOGICAL,
  "FlinkLogicalSortBatchConverter") {

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[LogicalSort]
    val newInput = RelOptRule.convert(sort.getInput, FlinkConventions.LOGICAL)
    val config = sort.getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val enableRangeSort = config.getConfiguration.getBoolean(TABLE_EXEC_RANGE_SORT_ENABLED)
    val limitValue = config.getConfiguration.getInteger(TABLE_EXEC_SORT_DEFAULT_LIMIT)
    val (offset, fetch) = if (sort.fetch == null && sort.offset == null
      && !enableRangeSort && limitValue > 0) {
      //force the sort add limit
      val rexBuilder = rel.getCluster.getRexBuilder
      val intType = rexBuilder.getTypeFactory.createSqlType(SqlTypeName.INTEGER)
      val offset = rexBuilder.makeLiteral(0, intType, true)
      val fetch = rexBuilder.makeLiteral(limitValue, intType, true)
      (offset, fetch)
    } else {
      (sort.offset, sort.fetch)
    }
    FlinkLogicalSort.create(newInput, sort.getCollation, offset, fetch)
  }
}

object FlinkLogicalSort {
  val BATCH_CONVERTER: RelOptRule = new FlinkLogicalSortBatchConverter
  val STREAM_CONVERTER: RelOptRule = new FlinkLogicalSortStreamConverter

  def create(
      input: RelNode,
      collation: RelCollation,
      sortOffset: RexNode,
      sortFetch: RexNode): FlinkLogicalSort = {
    val cluster = input.getCluster
    val collationTrait = RelCollationTraitDef.INSTANCE.canonize(collation)
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).replace(collationTrait)
    new FlinkLogicalSort(cluster,  traitSet, input, collation, sortOffset, sortFetch)
  }
}
