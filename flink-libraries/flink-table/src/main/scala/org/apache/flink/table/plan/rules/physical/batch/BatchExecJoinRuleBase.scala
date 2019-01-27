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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalSemiJoin}
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecLocalHashAggregate, BatchPhysicalRel}
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.Join
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet

import java.lang.Double
import java.util.Collections

trait BatchExecJoinRuleBase {

  def addLocalDistinctAgg(
      node: RelNode,
      distinctKeys: Seq[Int],
      relBuilder: RelBuilder): RelNode = {
    val localRequiredTraitSet = node.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newInput = RelOptRule.convert(node, localRequiredTraitSet)
    val providedTraitSet = localRequiredTraitSet

    new BatchExecLocalHashAggregate(
      node.getCluster,
      relBuilder,
      providedTraitSet,
      newInput,
      Seq(),
      node.getRowType,
      node.getRowType,
      distinctKeys.toArray,
      Array.empty)
  }

  def chooseSemiBuildDistinct(
      buildRel: RelNode,
      distinctKeys: Seq[Int]): Boolean = {
    val tableConfig = FlinkRelOptUtil.getTableConfig(buildRel)
    val mq = buildRel.getCluster.getMetadataQuery
    val ratioConf = tableConfig.getConf.getDouble(
      TableConfigOptions.SQL_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO)
    val inputRows = mq.getRowCount(buildRel)
    val ndvOfGroupKey = mq.getDistinctRowCount(
      buildRel, ImmutableBitSet.of(distinctKeys: _*), null)
    if (ndvOfGroupKey == null) false else ndvOfGroupKey / inputRows < ratioConf
  }

  def getFlinkJoinRelType(join: Join): FlinkJoinRelType = join match {
    case j: FlinkLogicalJoin =>
      FlinkJoinRelType.toFlinkJoinRelType(j.getJoinType)
    case sj: FlinkLogicalSemiJoin =>
      if (sj.isAnti) FlinkJoinRelType.ANTI else FlinkJoinRelType.SEMI
    case _ => throw new IllegalArgumentException(s"Illegal join node: ${join.getClass.getName}")
  }

  def getInputRowType(join: Join): RelDataType = join match {
    case j: FlinkLogicalJoin => j.getRowType
    case sj: FlinkLogicalSemiJoin =>
      // Combines inputs' RowType, the result is different from SemiJoin's RowType.
      SqlValidatorUtil.deriveJoinRowType(
        sj.getLeft.getRowType,
        sj.getRight.getRowType,
        sj.getJoinType,
        sj.getCluster.getTypeFactory,
        null,
        Collections.emptyList[RelDataTypeField]
      )
    case _ => throw new IllegalArgumentException(s"Illegal join node: ${join.getClass.getName}")
  }

  private[flink] def binaryRowRelNodeSize(relNode: RelNode): Double = {
    val mq = relNode.getCluster.getMetadataQuery
    val rowCount = mq.getRowCount(relNode)
    if(rowCount == null) {
      null
    } else {
      rowCount * BatchPhysicalRel.binaryRowAverageSize(relNode)
    }
  }
}
