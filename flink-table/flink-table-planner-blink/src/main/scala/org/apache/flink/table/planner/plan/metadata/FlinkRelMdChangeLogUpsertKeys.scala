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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner._
import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.ChangeLogUpsertKeys
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WatermarkAssigner, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalGroupAggregateBase, BatchPhysicalOverAggregate, BatchPhysicalWindowAggregateBase}
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalChangelogNormalize, StreamPhysicalDeduplicate, StreamPhysicalDropUpdateBefore, StreamPhysicalGlobalGroupAggregate, StreamPhysicalGroupAggregate, StreamPhysicalGroupWindowAggregate, StreamPhysicalIntervalJoin, StreamPhysicalLocalGroupAggregate, StreamPhysicalOverAggregate}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core.{Aggregate, Calc, Exchange, Filter, Join, JoinInfo, JoinRelType, Project, SetOp, Sort, TableScan, Window}
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelDistribution, RelNode, SingleRel}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.{Bug, ImmutableBitSet, Util}

import java.util

import scala.collection.JavaConversions._

/**
 * FlinkRelMdChangeLogUpsertKeys supplies a default implementation of
 * [[FlinkRelMetadataQuery#getChangeLogUpsertKeys]] for the standard logical algebra.
 */
class FlinkRelMdChangeLogUpsertKeys private extends MetadataHandler[ChangeLogUpsertKeys] {

  override def getDef: MetadataDef[ChangeLogUpsertKeys] = ChangeLogUpsertKeys.DEF

  def getChangeLogUpsertKeys(rel: TableScan, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    rel.getTable match {
      case t: IntermediateRelTable => t.changeLogUpsertKeys
      case _ => mq.getUniqueKeys(rel)
    }
  }

  def getChangeLogUpsertKeys(rel: Project, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    getProjectChangeLogUpsertKeys(rel.getProjects, rel.getInput, mq)

  def getChangeLogUpsertKeys(rel: Filter, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(rel.getInput)

  def getChangeLogUpsertKeys(calc: Calc, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getProjectChangeLogUpsertKeys(projects, calc.getInput, mq)
  }

  private def getProjectChangeLogUpsertKeys(
      projects: JList[RexNode],
      input: RelNode,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    FlinkRelMdUniqueKeys.INSTANCE.getProjectUniqueKeys(
      projects,
      input.getCluster.getTypeFactory,
      () => FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(input),
      ignoreNulls = false)

  def getChangeLogUpsertKeys(rel: Expand, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    FlinkRelMdUniqueKeys.INSTANCE.getExpandUniqueKeys(
      rel, () => FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(rel.getInput))

  def getChangeLogUpsertKeys(rel: Exchange, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val keys = FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(rel.getInput)
    rel.getDistribution.getType match {
      case RelDistribution.Type.HASH_DISTRIBUTED =>
        filterKeys(keys, ImmutableBitSet.of(rel.getDistribution.getKeys))
      case RelDistribution.Type.SINGLETON => keys
      case t => throw new UnsupportedOperationException("Unsupported distribution type: " + t)
    }
  }

  def getChangeLogUpsertKeys(rel: Rank, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val inputKeys = filterKeys(FlinkRelMetadataQuery.reuseOrCreate(mq)
        .getChangeLogUpsertKeys(rel.getInput), rel.partitionKey)
    FlinkRelMdUniqueKeys.INSTANCE.getRankUniqueKeys(rel, inputKeys)
  }

  def getChangeLogUpsertKeys(rel: Sort, mq: RelMetadataQuery): JSet[ImmutableBitSet] =
    filterKeys(
      FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(rel.getInput),
      ImmutableBitSet.of(rel.getCollation.getKeys))

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalDeduplicate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    ImmutableSet.of(ImmutableBitSet.of(rel.getUniqueKeys.map(Integer.valueOf).toList))
  }

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalChangelogNormalize, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    ImmutableSet.of(ImmutableBitSet.of(rel.uniqueKeys.map(Integer.valueOf).toList))
  }

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalDropUpdateBefore, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(rel.getInput)
  }

  def getChangeLogUpsertKeys(
      rel: Aggregate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.getGroupSet.toArray)
  }

  def getChangeLogUpsertKeys(
      rel: BatchPhysicalGroupAggregateBase, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    if (rel.isFinal) {
      FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.grouping)
    } else {
      null
    }
  }

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalGroupAggregate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.grouping)
  }

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalLocalGroupAggregate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = null

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalGlobalGroupAggregate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnAggregate(rel.grouping)
  }

  def getChangeLogUpsertKeys(
      rel: WindowAggregate, mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnWindowAgg(
      rel.getRowType.getFieldCount,
      rel.getNamedProperties,
      rel.getGroupSet.toArray)
  }

  def getChangeLogUpsertKeys(
      rel: BatchPhysicalWindowAggregateBase, mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    if (rel.isFinal) {
      FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnWindowAgg(
        rel.getRowType.getFieldCount,
        rel.namedWindowProperties,
        rel.grouping)
    } else {
      null
    }
  }

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalGroupWindowAggregate, mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    FlinkRelMdUniqueKeys.INSTANCE.getUniqueKeysOnWindowAgg(
      rel.getRowType.getFieldCount, rel.namedWindowProperties, rel.grouping)
  }

  def getChangeLogUpsertKeys(
      rel: Window, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    getChangeLogUpsertKeysOnOver(rel, mq, rel.groups.map(_.keys): _*)
  }

  def getChangeLogUpsertKeys(
      rel: BatchPhysicalOverAggregate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    getChangeLogUpsertKeysOnOver(rel, mq, ImmutableBitSet.of(rel.partitionKeyIndices: _*))
  }

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalOverAggregate, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    getChangeLogUpsertKeysOnOver(rel, mq, rel.logicWindow.groups.map(_.keys): _*)
  }

  private def getChangeLogUpsertKeysOnOver(
      rel: SingleRel,
      mq: RelMetadataQuery,
      distributionKeys: ImmutableBitSet*): JSet[ImmutableBitSet] = {
    var inputKeys = FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(rel.getInput)
    for (distributionKey <- distributionKeys) {
      inputKeys = filterKeys(inputKeys, distributionKey)
    }
    inputKeys
  }

  def getChangeLogUpsertKeys(
      join: Join, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val joinInfo = join.analyzeCondition()
    join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        filterKeys(
          FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(join.getLeft),
          joinInfo.leftSet())
      case _ =>
        getJoinChangeLogUpsertKeys(joinInfo, join.getJoinType, join.getLeft, join.getRight, mq)
    }
  }

  def getChangeLogUpsertKeys(
      rel: StreamPhysicalIntervalJoin, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.originalCondition)
    getJoinChangeLogUpsertKeys(joinInfo, rel.getJoinType, rel.getLeft, rel.getRight, mq)
  }

  def getChangeLogUpsertKeys(
      join: CommonPhysicalLookupJoin, mq: RelMetadataQuery): util.Set[ImmutableBitSet] = {
    val left = join.getInput
    val leftKeys = FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(left)
    val leftType = left.getRowType
    val leftJoinKeys = join.joinInfo.leftSet
    FlinkRelMdUniqueKeys.INSTANCE.getJoinUniqueKeys(
      join.joinType, leftType, filterKeys(leftKeys, leftJoinKeys), null,
      areColumnsUpsertKeys(leftKeys, leftJoinKeys),
      // TODO get uniqueKeys from TableSchema of TableSource
      null)
  }

  private def getJoinChangeLogUpsertKeys(
      joinInfo: JoinInfo,
      joinRelType: JoinRelType,
      left: RelNode,
      right: RelNode,
      mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val leftKeys = fmq.getChangeLogUpsertKeys(left)
    val rightKeys = fmq.getChangeLogUpsertKeys(right)
    FlinkRelMdUniqueKeys.INSTANCE.getJoinUniqueKeys(
      joinRelType,
      left.getRowType,
      filterKeys(leftKeys, joinInfo.leftSet),
      filterKeys(rightKeys, joinInfo.rightSet),
      areColumnsUpsertKeys(leftKeys, joinInfo.leftSet),
      areColumnsUpsertKeys(rightKeys, joinInfo.rightSet))
  }

  def getChangeLogUpsertKeys(rel: SetOp, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    if (!rel.all) {
      ImmutableSet.of(ImmutableBitSet.range(rel.getRowType.getFieldCount))
    } else {
      ImmutableSet.of()
    }
  }

  def getChangeLogUpsertKeys(
      subset: RelSubset, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    if (!Bug.CALCITE_1048_FIXED) {
      //if the best node is null, so we can get the uniqueKeys based original node, due to
      //the original node is logically equivalent as the rel.
      val rel = Util.first(subset.getBest, subset.getOriginal)
      FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(rel)
    } else {
      throw new RuntimeException("CALCITE_1048 is fixed, so check this method again!")
    }
  }

  def getChangeLogUpsertKeys(
      subset: HepRelVertex, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(subset.getCurrentRel)
  }

  def getChangeLogUpsertKeys(
      subset: WatermarkAssigner, mq: RelMetadataQuery): JSet[ImmutableBitSet] = {
    FlinkRelMetadataQuery.reuseOrCreate(mq).getChangeLogUpsertKeys(subset.getInput)
  }

  private def filterKeys(
      keys: JSet[ImmutableBitSet], distributionKey: ImmutableBitSet): JSet[ImmutableBitSet] = {
    if (keys != null) {
      keys.filter(k => k.contains(distributionKey))
    } else {
      null
    }
  }

  private def areColumnsUpsertKeys(
      keys: JSet[ImmutableBitSet], columns: ImmutableBitSet): Boolean = {
    if (keys != null) {
      keys.exists(columns.contains)
    } else {
      false
    }
  }

  // Catch-all rule when none of the others apply.
  def getChangeLogUpsertKeys(rel: RelNode, mq: RelMetadataQuery): JSet[ImmutableBitSet] = null
}

object FlinkRelMdChangeLogUpsertKeys {

  private val INSTANCE = new FlinkRelMdChangeLogUpsertKeys

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    ChangeLogUpsertKeys.METHOD, INSTANCE)

}
