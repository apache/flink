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

import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.physical.stream._

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

/**
  * Collection of rules to annotate [[StreamPhysicalRel]] nodes with retraction information.
  *
  * The rules have to be applied in the following order:
  * - [[StreamExecRetractionRules.DEFAULT_RETRACTION_INSTANCE]]
  * - [[StreamExecRetractionRules.UPDATES_AS_RETRACTION_INSTANCE]]
  * - [[StreamExecRetractionRules.ACCMODE_INSTANCE]]
  *
  * The rules will assign a [[AccModeTrait]] to each [[StreamPhysicalRel]] node of the plan. The
  * trait defines the [[AccMode]] a node.
  * - [[AccMode.Acc]] defines that the node produces only accumulate messages, i.e., all types of
  * modifications (insert, update, delete) are encoded as accumulate messages.
  * - [[AccMode.AccRetract]] defines that the node produces accumulate and retraction messages.
  * Insert modifications are encoded as accumulate message, delete modifications as retraction
  * message, and update modifications as a pair of accumulate and retraction messages.
  */
object StreamExecRetractionRules {

  /**
    * Rule instance that assigns default retraction to [[StreamPhysicalRel]] nodes.
    */
  val DEFAULT_RETRACTION_INSTANCE = new AssignDefaultRetractionRule()

  /**
    * Rule instance that checks if [[StreamPhysicalRel]] nodes need to ship updates as retractions.
    */
  val UPDATES_AS_RETRACTION_INSTANCE = new SetUpdatesAsRetractionRule()

  /**
    * Rule instance that assigns the [[AccMode]] to [[StreamPhysicalRel]] nodes.
    */
  val ACCMODE_INSTANCE = new SetAccModeRule()

  /**
    * Get all children RelNodes of a RelNode.
    *
    * @param parent The parent RelNode
    * @return All child nodes
    */
  def getChildRelNodes(parent: RelNode): Seq[RelNode] = {
    parent.getInputs.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  /**
    * Checks if a [[RelNode]] ships updates as retractions.
    *
    * @param node The node to check.
    * @return True if the node ships updates as retractions, false otherwise.
    */
  def shipUpdatesAsRetraction(node: StreamPhysicalRel): Boolean = {
    containUpdatesAsRetraction(node) && !node.consumesRetractions
  }

  def containUpdatesAsRetraction(node: RelNode): Boolean = {
    val retractionTrait = node.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)
    retractionTrait != null && retractionTrait.sendsUpdatesAsRetractions
  }

  /**
    * Checks if a [[RelNode]] is in [[AccMode.AccRetract]] mode.
    */
  def isAccRetract(node: RelNode): Boolean = {
    val accModeTrait = node.getTraitSet.getTrait(AccModeTraitDef.INSTANCE)
    null != accModeTrait && accModeTrait.getAccMode == AccMode.AccRetract
  }

  /**
    * Rule that assigns the default retraction information to [[StreamPhysicalRel]] nodes.
    * The default is to not publish updates as retraction messages and [[AccMode.Acc]].
    */
  class AssignDefaultRetractionRule extends RelOptRule(
    operand(classOf[StreamPhysicalRel], none()),
    "AssignDefaultRetractionRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {
      val rel: StreamPhysicalRel = call.rel(0)
      val traits = rel.getTraitSet

      val traitsWithUpdateAsRetraction =
        if (null == traits.getTrait(UpdateAsRetractionTraitDef.INSTANCE)) {
          traits.plus(UpdateAsRetractionTrait.DEFAULT)
        } else {
          traits
        }

      val accModeTrait = traitsWithUpdateAsRetraction.getTrait(AccModeTraitDef.INSTANCE)
      val traitsWithAccMode =
        if (null == accModeTrait || accModeTrait == AccModeTrait.UNKNOWN) {
          traitsWithUpdateAsRetraction.plus(AccModeTrait(AccMode.Acc))
        } else {
          traitsWithUpdateAsRetraction
        }

      val finalTraits = rel match {
        case scan: StreamExecDataStreamScan if scan.isAccRetract =>
          traitsWithAccMode.replace(AccModeTrait(AccMode.AccRetract))
        case _ => traitsWithAccMode
      }

      if (traits != finalTraits) {
        val newScan = rel.copy(finalTraits, rel.getInputs)
        call.transformTo(newScan)
      }
    }
  }

  /**
    * Rule that annotates all [[StreamPhysicalRel]] nodes that need to sent out update changes with
    * retraction messages.
    */
  class SetUpdatesAsRetractionRule extends RelOptRule(
    operand(classOf[StreamPhysicalRel], none()),
    "SetUpdatesAsRetractionRule") {

    /**
      * Checks if a [[StreamPhysicalRel]] requires that update changes are sent with retraction
      * messages.
      */
    def needsUpdatesAsRetraction(node: StreamPhysicalRel, input: RelNode): Boolean = {
      shipUpdatesAsRetraction(node) || node.needsUpdatesAsRetraction(input)
    }

    /**
      * Annotates a [[RelNode]] to send out update changes with retraction messages.
      */
    def setUpdatesAsRetraction(relNode: RelNode): RelNode = {
      val traitSet = relNode.getTraitSet
      val newTraitSet = traitSet.plus(UpdateAsRetractionTrait(true))
      relNode.copy(newTraitSet, relNode.getInputs)
    }

    /**
      * Annotates the children of a parent node with the information that they need to forward
      * update and delete modifications as retraction messages.
      *
      * A child needs to produce retraction messages, if
      *
      * 1. its parent requires retraction messages by itself because it is a certain type
      * of operator, such as a [[StreamExecGroupAggregate]] or [[StreamExecOverAggregate]], or
      * 2. its parent requires retraction because its own parent requires retraction
      * (transitive requirement).
      *
      */
    override def onMatch(call: RelOptRuleCall): Unit = {
      val parent: StreamPhysicalRel = call.rel(0)

      val children = getChildRelNodes(parent)
      // check if children need to sent out retraction messages
      val newChildren = for (c <- children) yield {
        if (needsUpdatesAsRetraction(parent, c) && !containUpdatesAsRetraction(c)) {
          setUpdatesAsRetraction(c)
        } else {
          c
        }
      }

      // update parent if a child was updated
      if (children != newChildren) {
        val newRel = parent.copy(parent.getTraitSet, newChildren)
        call.transformTo(newRel)
      }
    }
  }

  /**
    * Sets the [[AccMode]] of [[StreamPhysicalRel]] nodes.
    */
  class SetAccModeRule extends RelOptRule(
    operand(classOf[StreamPhysicalRel], none()),
    "SetAccModeRule") {

    /**
      * Set [[AccMode.AccRetract]] to a [[RelNode]].
      */
    def setAccRetract(relNode: RelNode): RelNode = {
      val traitSet = relNode.getTraitSet
      val newTraitSet = traitSet.plus(new AccModeTrait(AccMode.AccRetract))
      relNode.copy(newTraitSet, relNode.getInputs)
    }

    /**
      * Checks if a [[StreamPhysicalRel]] produces retraction messages.
      */
    def producesRetractions(node: StreamPhysicalRel): Boolean = {
      containUpdatesAsRetraction(node) && node.producesUpdates || node.producesRetractions
    }

    /**
      * Checks if a [[StreamPhysicalRel]] forwards retraction messages from its children.
      */
    def forwardsRetractions(parent: StreamPhysicalRel, children: Seq[RelNode]): Boolean = {
      children.exists(c => isAccRetract(c)) && !parent.consumesRetractions
    }

    /**
      * Updates the [[AccMode]] of a [[RelNode]] and its children if necessary.
      */
    override def onMatch(call: RelOptRuleCall): Unit = {
      val parent: StreamPhysicalRel = call.rel(0)
      val children = getChildRelNodes(parent)

      // check if the AccMode of the parent needs to be updated
      if (!isAccRetract(parent) &&
        (producesRetractions(parent) || forwardsRetractions(parent, children))) {
        val newRel = setAccRetract(parent)
        call.transformTo(newRel)
      }
    }
  }

}
