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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{StreamTransformation, UnionTransformation}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type.{ANY, BROADCAST_DISTRIBUTED, HASH_DISTRIBUTED, RANDOM_DISTRIBUTED, RANGE_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED, SINGLETON}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Union]].
  */
class BatchExecUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: util.List[RelNode],
    all: Boolean,
    outputRowType: RelDataType)
  extends Union(cluster, traitSet, inputRels, all)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  require(all, "Only support union all now")

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode], all: Boolean): SetOp = {
    new BatchExecUnion(cluster, traitSet, inputs, all, outputRowType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("union", getRowType.getFieldNames.mkString(", "))
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    // union will destroy collation trait. So does not push down collation requirement.
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val pushDownDistribution = requiredDistribution.getType match {
      case RANDOM_DISTRIBUTED | ROUND_ROBIN_DISTRIBUTED | BROADCAST_DISTRIBUTED =>
        requiredDistribution
      // apply strict hash distribution of each child to avoid inconsistent of shuffle of each child
      case HASH_DISTRIBUTED => FlinkRelDistribution.hash(requiredDistribution.getKeys)
      // range distribution cannot push down because partition's [lower, upper]  of each union child
      // may be different
      case RANGE_DISTRIBUTED => null
      // Singleton cannot push down. Singleton exchange limit the parallelism of later RelNode to 1.
      // Push down Singleton into input of union will destroy the limitation.
      case SINGLETON => null
      // there is no need to push down Any distribution
      case ANY => null
    }
    if (pushDownDistribution == null) {
      null
    } else {
      val relNodes = getInputs.map(RelOptRule.convert(_, pushDownDistribution))
      copy(getTraitSet.replace(pushDownDistribution), relNodes)
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    getInputs.map(_.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val transformations = getInputNodes.map {
      input => input.translateToPlan(tableEnv).asInstanceOf[StreamTransformation[BaseRow]]
    }
    new UnionTransformation(transformations)
  }
}
