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
package org.apache.flink.table.planner.plan.rules.physical

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecSort, BatchPhysicalExchange, BatchPhysicalRel}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule._

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.AbstractConverter
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelCollations, RelNode}

/**
  * Rule which converts an [[AbstractConverter]] to a RelNode which satisfies the target traits.
  */
class FlinkExpandConversionRule(flinkConvention: Convention)
  extends RelOptRule(
    operand(classOf[AbstractConverter],
      operand(classOf[RelNode], any)),
    "FlinkExpandConversionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val toTraitSet = call.rel(0).asInstanceOf[AbstractConverter].getTraitSet
    val fromTraitSet = call.rel(1).asInstanceOf[RelNode].getTraitSet
    toTraitSet.contains(flinkConvention) &&
      fromTraitSet.contains(flinkConvention) &&
      !fromTraitSet.satisfies(toTraitSet)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val converter: AbstractConverter = call.rel(0)
    val child: RelNode = call.rel(1)
    val toTraitSet = converter.getTraitSet
    // try to satisfy required trait by itself.
    satisfyTraitsBySelf(child, toTraitSet, call)
    // try to push down required traits to children.
    satisfyTraitsByInput(child, toTraitSet, call)
  }

  private def satisfyTraitsBySelf(
      node: RelNode,
      requiredTraits: RelTraitSet,
      call: RelOptRuleCall): Unit = {
    var transformedNode = node
    val definedTraitDefs = call.getPlanner.getRelTraitDefs
    if (definedTraitDefs.contains(FlinkRelDistributionTraitDef.INSTANCE)) {
      val toDistribution = requiredTraits.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
      transformedNode = satisfyDistribution(flinkConvention, transformedNode, toDistribution)
    }
    if (definedTraitDefs.contains(RelCollationTraitDef.INSTANCE)) {
      val toCollation = requiredTraits.getTrait(RelCollationTraitDef.INSTANCE)
      transformedNode = satisfyCollation(flinkConvention, transformedNode, toCollation)
    }
    checkSatisfyRequiredTrait(transformedNode, requiredTraits)
    call.transformTo(transformedNode)
  }

  private def satisfyTraitsByInput(
      node: RelNode,
      requiredTraits: RelTraitSet,
      call: RelOptRuleCall): Unit = {
    node match {
      case batchRel: BatchPhysicalRel =>
        val otherChoice = batchRel.satisfyTraits(requiredTraits)
        otherChoice match {
          case Some(newRel) =>
            // It is possible only push down distribution instead of push down both distribution and
            // collation. So it is necessary to check whether collation satisfy requirement.
            val requiredCollation = requiredTraits.getTrait(RelCollationTraitDef.INSTANCE)
            val finalRel = satisfyCollation(flinkConvention, newRel, requiredCollation)
            checkSatisfyRequiredTrait(finalRel, requiredTraits)
            call.transformTo(finalRel)
          case _ => // do nothing
        }
      case _ => // ignore
    }
  }

  private def checkSatisfyRequiredTrait(node: RelNode, requiredTraits: RelTraitSet): Unit = {
    if (!node.getTraitSet.satisfies(requiredTraits)) {
      throw new RuntimeException(
        "the node which is converted by FlinkExpandConversionRule can't satisfied the target " +
          s"traits!\nnode traits: ${node.getTraitSet}\nrequired traits: $requiredTraits")
    }
  }

}

object FlinkExpandConversionRule {
  val BATCH_INSTANCE = new FlinkExpandConversionRule(FlinkConventions.BATCH_PHYSICAL)
  val STREAM_INSTANCE = new FlinkExpandConversionRule(FlinkConventions.STREAM_PHYSICAL)

  def satisfyDistribution(
      flinkConvention: Convention,
      node: RelNode,
      requiredDistribution: FlinkRelDistribution): RelNode = {
    val fromTraitSet = node.getTraitSet
    val fromDistribution = fromTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    if (!fromDistribution.satisfies(requiredDistribution)) {
      requiredDistribution.getType match {
        case SINGLETON | HASH_DISTRIBUTED | RANGE_DISTRIBUTED |
             BROADCAST_DISTRIBUTED | RANDOM_DISTRIBUTED =>
          flinkConvention match {
            case FlinkConventions.BATCH_PHYSICAL =>
              // replace collation with empty since distribution destroy collation
              val traitSet = fromTraitSet
                .replace(requiredDistribution)
                .replace(flinkConvention)
                .replace(RelCollations.EMPTY)
              new BatchPhysicalExchange(node.getCluster, traitSet, node, requiredDistribution)
            case FlinkConventions.STREAM_PHYSICAL =>
              val modifyKindSetTrait = fromTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
              val updateKindTrait = fromTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
              // replace collation with empty since distribution destroy collation
              val traitSet = fromTraitSet
                .replace(requiredDistribution)
                .replace(flinkConvention)
                .replace(RelCollations.EMPTY)
                .replace(modifyKindSetTrait)
                .replace(updateKindTrait)
              new StreamPhysicalExchange(node.getCluster, traitSet, node, requiredDistribution)
            case _ => throw new TableException(s"Unsupported convention: $flinkConvention")
          }
        case _ => throw new TableException(s"Unsupported type: ${requiredDistribution.getType}")
      }
    } else {
      node
    }
  }

  def satisfyCollation(
      flinkConvention: Convention,
      node: RelNode,
      requiredCollation: RelCollation): RelNode = {
    val fromCollation = node.getTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (!fromCollation.satisfies(requiredCollation)) {
      val traitSet = node.getTraitSet.replace(requiredCollation).replace(flinkConvention)
      val sortCollation = RelCollationTraitDef.INSTANCE.canonize(requiredCollation)
      flinkConvention match {
        case FlinkConventions.BATCH_PHYSICAL =>
          new BatchExecSort(node.getCluster, traitSet, node, sortCollation)
        case _ => throw new TableException(s"Unsupported convention: $flinkConvention")
      }
    } else {
      node
    }
  }
}
