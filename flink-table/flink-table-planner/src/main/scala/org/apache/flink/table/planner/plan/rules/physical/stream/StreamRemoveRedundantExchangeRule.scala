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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.`trait`.{InputRelDistributionTrait, InputRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalExchange, StreamPhysicalRel}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConverters._

/**
 * Rule that removes redundant [[StreamPhysicalExchange]]s in plan. Currently only hash distribution
 * is supported for optimization.
 */
class StreamRemoveRedundantExchangeRule extends RelOptRule(operand(classOf[RelNode], any())) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val node: RelNode = call.rel(0)
    if (node.getInputs.isEmpty || hasDistributionTraitFromInputs(node)) {
      return false
    }
    node.getInputs.asScala
      .map(unpackNode)
      .exists(input => isHashExchange(input) || hasDistributionTraitFromInputs(input))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val node: RelNode = call.rel(0)
    val inputs = node.getInputs.asScala
      .map(unpackNode)
      .toList
    val traitsFromInputs = inputs
      .map {
        case exchange: StreamPhysicalExchange =>
          val dist = exchange.distribution
          if (dist.getType == Type.HASH_DISTRIBUTED) {
            val inputDistTrait =
              exchange.getTraitSet.getTrait(InputRelDistributionTraitDef.INSTANCE)
            if (inputDistTrait != null && inputDistTrait.satisfies(dist)) {
              // remove redundant exchange
              val inputIndex = inputs.indexOf(exchange)
              node.replaceInput(inputIndex, exchange.getInput)
            }
            // pass new input distribution trait
            Some(InputRelDistributionTrait.inputHash(dist.getKeys))
          } else {
            None
          }
        case input =>
          if (hasDistributionTraitFromInputs(input)) {
            // pass existing input distribution trait
            Some(input.getTraitSet.getTrait(InputRelDistributionTraitDef.INSTANCE))
          } else {
            None
          }
      }

    // there can be maximum 2 inputs
    Preconditions.checkState(traitsFromInputs.size == 1 || traitsFromInputs.size == 2)
    val requiredTrait = if (traitsFromInputs.size == 1) {
      traitsFromInputs.head.orNull
    } else {
      val leftTrait = traitsFromInputs.head
      val rightTrait = traitsFromInputs(1)
      if (leftTrait.nonEmpty && rightTrait.nonEmpty) {
        InputRelDistributionTrait.twoInputsHash(leftTrait.get.getKeys, rightTrait.get.getKeys)
      } else if (leftTrait.nonEmpty) {
        InputRelDistributionTrait.leftInputHash(leftTrait.get.getKeys)
      } else if (rightTrait.nonEmpty) {
        InputRelDistributionTrait.rightInputHash(rightTrait.get.getKeys)
      } else {
        null
      }
    }
    if (requiredTrait != null) {
      node match {
        case streamRel: StreamPhysicalRel =>
          val requiredTraitSet = node.getTraitSet.plus(requiredTrait)
          val nextWithTraits = streamRel.satisfyTraitsFromInputs(requiredTraitSet)
          nextWithTraits match {
            case Some(newRel) =>
              call.transformTo(newRel)
            case _ =>
          }
        case _ =>
      }
    }
  }

  private def isHashExchange(node: RelNode): Boolean = {
    node.isInstanceOf[StreamPhysicalExchange] && node
      .asInstanceOf[StreamPhysicalExchange]
      .distribution
      .getType
      .equals(Type.HASH_DISTRIBUTED)
  }

  private def hasDistributionTraitFromInputs(node: RelNode): Boolean = {
    node.getTraitSet.getTrait(InputRelDistributionTraitDef.INSTANCE) != null
  }

  private def unpackNode(node: RelNode): RelNode = {
    node match {
      case helpNode: HepRelVertex => helpNode.getCurrentRel
      case _ => node
    }
  }
}

object StreamRemoveRedundantExchangeRule {
  val INSTANCE: StreamRemoveRedundantExchangeRule = new StreamRemoveRedundantExchangeRule
}
