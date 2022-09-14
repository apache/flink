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
package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef, TraitUtil}
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.planner.plan.utils.RelExplainUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelCollationTraitDef, RelDistribution, RelFieldCollation, RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.core.{Correlate, JoinRelType}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.util.mapping.{Mapping, Mappings, MappingType}

import scala.collection.JavaConversions._

/** Base Batch physical RelNode for [[Correlate]] (user defined table function). */
abstract class BatchPhysicalCorrelateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    outputRowType: RelDataType,
    joinType: JoinRelType)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel {

  require(joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT)

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    copy(traitSet, inputs.get(0), outputRowType)
  }

  /** Note: do not passing member 'child' because singleRel.replaceInput may update 'input' rel. */
  def copy(traitSet: RelTraitSet, child: RelNode, outputType: RelDataType): RelNode

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    super
      .explainTerms(pw)
      .item("invocation", scan.getCall)
      .item(
        "correlate",
        RelExplainUtil.correlateToString(
          input.getRowType,
          rexCall,
          getExpressionString,
          RelExplainUtil.preferExpressionDetail(pw)))
      .item("select", outputRowType.getFieldNames.mkString(","))
      .item("rowType", outputRowType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    // Correlate could not provide broadcast distribution
    if (requiredDistribution.getType == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      return None
    }

    def getOutputInputMapping: Mapping = {
      val inputFieldCnt = getInput.getRowType.getFieldCount
      val mapping = Mappings.create(MappingType.FUNCTION, inputFieldCnt, inputFieldCnt)
      (0 until inputFieldCnt).foreach(index => mapping.set(index, index))
      mapping
    }

    val mapping = getOutputInputMapping
    val appliedDistribution = requiredDistribution.apply(mapping)
    // If both distribution and collation can be satisfied, satisfy both. If only distribution
    // can be satisfied, only satisfy distribution. There is no possibility to only satisfy
    // collation here except for there is no distribution requirement.
    if ((!requiredDistribution.isTop) && (appliedDistribution eq FlinkRelDistribution.ANY)) {
      return None
    }

    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val appliedCollation = TraitUtil.apply(requiredCollation, mapping)
    // the required collation can be satisfied if field collations are not empty
    // and the direction of each field collation is non-STRICTLY
    val canSatisfyCollation = appliedCollation.getFieldCollations.nonEmpty &&
      !appliedCollation.getFieldCollations.exists {
        c =>
          (c.getDirection eq RelFieldCollation.Direction.STRICTLY_ASCENDING) ||
          (c.getDirection eq RelFieldCollation.Direction.STRICTLY_DESCENDING)
      }
    // If required traits only contains collation requirements, but collation keys are not columns
    // from input, then no need to satisfy required traits.
    if ((appliedDistribution eq FlinkRelDistribution.ANY) && !canSatisfyCollation) {
      return None
    }

    var inputRequiredTraits = getInput.getTraitSet
    var providedTraits = getTraitSet
    if (!appliedDistribution.isTop) {
      inputRequiredTraits = inputRequiredTraits.replace(appliedDistribution)
      providedTraits = providedTraits.replace(requiredDistribution)
    }
    if (canSatisfyCollation) {
      inputRequiredTraits = inputRequiredTraits.replace(appliedCollation)
      providedTraits = providedTraits.replace(requiredCollation)
    }
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    Some(copy(providedTraits, Seq(newInput)))
  }
}
