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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.RelOptRule.{operand, _}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.plan.nodes.datastream._

import scala.collection.JavaConverters._

/**
  * Collection of rules to annotate [[DataStreamRel]] nodes with retraction information.
  *
  * The rules have to be applied in the following order:
  * - [[DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE]]
  * - [[DataStreamRetractionRules.UPDATES_AS_RETRACTION_INSTANCE]]
  * - [[DataStreamRetractionRules.ACCMODE_INSTANCE]]
  *
  * The rules will assign a [[AccModeTrait]] to each [[DataStreamRel]] node of the plan. The
  * trait defines the [[AccMode]] a node.
  * - [[AccMode.Acc]] defines that the node produces only accumulate messages, i.e., all types of
  * modifications (insert, update, delete) are encoded as accumulate messages.
  * - [[AccMode.AccRetract]] defines that the node produces accumulate and retraction messages.
  * Insert modifications are encoded as accumulate message, delete modifications as retraction
  * message, and update modifications as a pair of accumulate and retraction messages.
  *
  */
object DataStreamRetractionRules {

  /**
    * Rule instance that assigns default retraction to [[DataStreamRel]] nodes.
    */
  val DEFAULT_RETRACTION_INSTANCE = new AssignDefaultRetractionRule()

  /**
    * Rule instance that checks if [[DataStreamRel]] nodes need to ship updates as retractions.
    */
  val UPDATES_AS_RETRACTION_INSTANCE = new SetUpdatesAsRetractionRule()

  /**
    * Rule instance that assigns the [[AccMode]] to [[DataStreamRel]] nodes.
    */
  val ACCMODE_INSTANCE = new SetAccModeRule()

  /**
    * Get all children RelNodes of a RelNode.
    *
    * @param parent The parent RelNode
    * @return All child nodes
    */
  def getChildRelNodes(parent: RelNode): Seq[RelNode] = {
    parent.getInputs.asScala.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  /**
    * Checks if a [[RelNode]] ships updates as retractions.
    *
    * @param node The node to check.
    * @return True if the node ships updates as retractions, false otherwise.
    */
  def sendsUpdatesAsRetraction(node: RelNode): Boolean = {
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
    * Rule that assigns the default retraction information to [[DataStreamRel]] nodes.
    * The default is to not publish updates as retraction messages and [[AccMode.Acc]].
    */
  class AssignDefaultRetractionRule extends RelOptRule(
    operand(
      classOf[DataStreamRel], none()),
    "AssignDefaultRetractionRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {
      val rel = call.rel(0).asInstanceOf[DataStreamRel]
      val traits = rel.getTraitSet

      val traitsWithUpdateAsRetraction =
        if (null == traits.getTrait(UpdateAsRetractionTraitDef.INSTANCE)) {
        traits.plus(UpdateAsRetractionTrait.DEFAULT)
      } else {
        traits
      }
      val traitsWithAccMode =
        if (null == traitsWithUpdateAsRetraction.getTrait(AccModeTraitDef.INSTANCE)) {
          traitsWithUpdateAsRetraction.plus(AccModeTrait.DEFAULT)
      } else {
        traitsWithUpdateAsRetraction
      }

      if (traits != traitsWithAccMode) {
        call.transformTo(rel.copy(traitsWithAccMode, rel.getInputs))
      }
    }
  }

  /**
    * Rule that annotates all [[DataStreamRel]] nodes that need to sent out update changes with
    * retraction messages.
    */
  class SetUpdatesAsRetractionRule extends RelOptRule(
    operand(
      classOf[DataStreamRel], none()),
    "SetUpdatesAsRetractionRule") {

    /**
      * Checks if a [[RelNode]] requires that update changes are sent with retraction
      * messages.
      */
    def needsUpdatesAsRetraction(node: RelNode): Boolean = {
      node match {
        case _ if sendsUpdatesAsRetraction(node) => true
        case dsr: DataStreamRel => dsr.needsUpdatesAsRetraction
      }
    }

    /**
      * Annotates a [[RelNode]] to send out update changes with retraction messages.
      */
    def setUpdatesAsRetraction(relNode: RelNode): RelNode = {
      val traitSet = relNode.getTraitSet
      relNode.copy(traitSet.plus(new UpdateAsRetractionTrait(true)), relNode.getInputs)
    }

    /**
      * Annotates the children of a parent node with the information that they need to forward
      * update and delete modifications as retraction messages.
      *
      * A child needs to produce retraction messages, if
      *
      * 1. its parent requires retraction messages by itself because it is a certain type
      *    of operator, such as a [[DataStreamGroupAggregate]] or [[DataStreamOverAggregate]], or
      * 2. its parent requires retraction because its own parent requires retraction
      *    (transitive requirement).
      *
      */
    override def onMatch(call: RelOptRuleCall): Unit = {
      val parent = call.rel(0).asInstanceOf[DataStreamRel]

      val children = getChildRelNodes(parent)
      // check if children need to sent out retraction messages
      val newChildren = for (c <- children) yield {
        if (needsUpdatesAsRetraction(parent) && !sendsUpdatesAsRetraction(c)) {
          setUpdatesAsRetraction(c)
        } else {
          c
        }
      }

      // update parent if a child was updated
      if (children != newChildren) {
        call.transformTo(parent.copy(parent.getTraitSet, newChildren.asJava))
      }
    }
  }

  /**
    * Sets the [[AccMode]] of [[DataStreamRel]] nodes.
    */
  class SetAccModeRule extends RelOptRule(
    operand(
      classOf[DataStreamRel], none()),
    "SetAccModeRule") {

    /**
      * Set [[AccMode.AccRetract]] to a [[RelNode]].
      */
    def setAccRetract(relNode: RelNode): RelNode = {
      val traitSet = relNode.getTraitSet
      relNode.copy(traitSet.plus(new AccModeTrait(AccMode.AccRetract)), relNode.getInputs)
    }

    /**
      * Checks if a [[DataStreamRel]] produces retraction messages.
      */
    def producesRetractions(node: DataStreamRel): Boolean = {
      sendsUpdatesAsRetraction(node) && node.producesUpdates || node.producesRetractions
    }

    /**
      * Checks if a [[DataStreamRel]] forwards retraction messages from its children.
      */
    def forwardsRetractions(parent: DataStreamRel, children: Seq[RelNode]): Boolean = {
      children.exists(c => isAccRetract(c)) && !parent.consumesRetractions
    }

    /**
      * Updates the [[AccMode]] of a [[RelNode]] and its children if necessary.
      */
    override def onMatch(call: RelOptRuleCall): Unit = {
      val parent = call.rel(0).asInstanceOf[DataStreamRel]
      val children = getChildRelNodes(parent)

      // check if the AccMode of the parent needs to be updated
      if (!isAccRetract(parent) &&
          (producesRetractions(parent) || forwardsRetractions(parent, children))) {
        call.transformTo(setAccRetract(parent))
      }
    }
  }

}
