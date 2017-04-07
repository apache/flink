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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Collection of retraction rules that apply various transformations on DataStreamRel trees.
  * Currently, there are three transformations: InitProcessRule, NeedToRetractProcessRule and
  * AccModeProcessRule. Note: these rules must be called in order (InitProcessRule ->
  * NeedToRetractProcessRule -> AccModeProcessRule).
  */
object DataStreamRetractionRule {

  /**
    * Singleton rule that init retraction trait inside a [[DataStreamRel]]
    */
  val INIT_INSTANCE = new InitProcessRule()

  /**
    * Singleton rule that decide needToRetract property inside a [[DataStreamRel]]
    */
  val NEEDTORETRACT_INSTANCE = new NeedToRetractProcessRule()

  /**
    * Singleton rule that decide accMode inside a [[DataStreamRel]]
    */
  val ACCMODE_INSTANCE = new AccModeProcessRule()

  /**
    * Get all child RelNodes of a RelNode
    * @param topRel The input RelNode
    * @return All child nodes
    */
  def getChildRelNodes(topRel: RelNode): ListBuffer[RelNode] = {
    val topRelInputs = new ListBuffer[RelNode]()
    topRelInputs.++=(topRel.getInputs.asScala)
    topRelInputs.transform(e => e.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  def traitSetContainNeedToRetract(traitSet: RelTraitSet): Boolean = {
    val retractionTrait = traitSet.getTrait(RetractionTraitDef.INSTANCE)
    if (null == retractionTrait) {
      false
    } else {
      retractionTrait.getNeedToRetract
    }
  }


  /**
    * Find all needToRetract nodes. A node needs to retract means that there are downstream
    * nodes need retraction from it. Currently, [[DataStreamOverAggregate]] and
    * [[DataStreamGroupWindowAggregate]] need retraction from upstream nodes, besides, a
    * needToRetract node also need retraction from it's upstream nodes.
    */
  class NeedToRetractProcessRule extends RelOptRule(
    operand(
      classOf[DataStreamRel], none()),
    "NeedToRetractProcessRule") {

    /**
      * Return true if bottom RelNode does not contain needToRetract and top RelNode need
      * retraction from bottom RelNode. Currently, operators which contain aggregations need
      * retraction from upstream nodes, besides, a needToRetract node also needs retraction from
      * it's upstream nodes.
      */
    def bottomNeedToRetract(topRel: RelNode, bottomRel: RelNode): Boolean = {
      val bottomTraits = bottomRel.getTraitSet
      if(!traitSetContainNeedToRetract(bottomTraits)){
        topRel match {
          case _: DataStreamGroupAggregate => true
          case _: DataStreamGroupWindowAggregate => true
          case _: DataStreamOverAggregate => true
          case _ if traitSetContainNeedToRetract(topRel.getTraitSet) => true
          case _ => false
        }
      } else {
        false
      }
    }

    /**
      * Add needToRetract for the input RelNode
      */
    def addNeedToRetract(relNode: RelNode): RelNode = {
      val traitSet = relNode.getTraitSet
      var retractionTrait = traitSet.getTrait(RetractionTraitDef.INSTANCE)
      if (null == retractionTrait) {
        retractionTrait = new RetractionTrait(true, AccMode.Acc)
      } else {
        retractionTrait = new RetractionTrait(true, retractionTrait.getAccMode)
      }

      relNode.copy(traitSet.plus(retractionTrait), relNode.getInputs)
    }

    /**
      * Returns a new topRel and a needTransform flag for a given topRel and bottomRels. The new
      * topRel contains new bottomRels with needToRetract properly marked. The needTransform flag
      * will be true if any transformation has been done.
      *
      * @param topRel The input top RelNode.
      * @param bottomRels The input bottom RelNodes.
      * @return A tuple holding a new top RelNode and a needTransform flag
      */
    def needToRetractProcess(
        topRel: RelNode,
        bottomRels: ListBuffer[RelNode])
    : (RelNode, Boolean) = {

      var needTransform = false
      var i = 0
      while(i < bottomRels.size) {
        val bottomRel = bottomRels(i)
        if(bottomNeedToRetract(topRel, bottomRel)) {
          needTransform = true
          bottomRels(i) = addNeedToRetract(bottomRel)
        }
        i += 1
      }
      val newTopRel = topRel.copy(topRel.getTraitSet, bottomRels.asJava)
      (newTopRel, needTransform)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val topRel = call.rel(0).asInstanceOf[DataStreamRel]

      // get bottom relnodes
      val bottomRels = getChildRelNodes(topRel)

      // process bottom relnodes
      val (newTopRel, needTransform) = needToRetractProcess(topRel, bottomRels)

      if (needTransform) {
        call.transformTo(newTopRel)
      }
    }

  }


  /**
    * Find all AccRetract nodes. A node in AccRetract Mode means that the operator may generate
    * or forward an additional retraction message.
    *
    */
  class AccModeProcessRule extends RelOptRule(
    operand(
      classOf[DataStreamRel], none()),
    "AccModeProcessRule") {


    /**
      * Return true if result table is a replace table
      */
    def resultTableIsReplaceTable(relNode: RelNode): Boolean = {
      relNode match {
        case _: DataStreamGroupAggregate => true
        case _ => false
      }
    }


    def traitSetContainAccRetract(traitSet: RelTraitSet): Boolean = {
      val retractionTrait = traitSet.getTrait(RetractionTraitDef.INSTANCE)
      if (null == retractionTrait) {
        false
      } else {
        retractionTrait.getAccMode == AccMode.AccRetract
      }
    }

    /**
      * Return true if the result table of input RelNode is a replace table and the input RelNode
      * is under AccMode with needToRetract.
      */
    def needSetAccRetract(relNode: RelNode): Boolean = {
      val traitSet = relNode.getTraitSet
      if (traitSetContainNeedToRetract(traitSet)
        && resultTableIsReplaceTable(relNode)
        && !traitSetContainAccRetract(traitSet)) {
        true
      } else {
        false
      }
    }

    /**
      * Set AccMode to AccRetract for the input RelNode
      */
    def setAccRetract(relNode: RelNode): RelNode = {
      val traitSet = relNode.getTraitSet
      var retractionTrait = traitSet.getTrait(RetractionTraitDef.INSTANCE)
      if (null == retractionTrait) {
        retractionTrait = new RetractionTrait(false, AccMode.AccRetract)
      } else {
        retractionTrait = new RetractionTrait(retractionTrait.getNeedToRetract,
                                              AccMode.AccRetract)
      }
      relNode.copy(traitSet.plus(retractionTrait), relNode.getInputs)
    }

    /**
      * Currently, window (including group window and over window) does not contain early firing,
      * so [[DataStreamGroupWindowAggregate]] and [[DataStreamOverAggregate]] will digest
      * retraction messages. [[DataStreamGroupAggregate]] can also digest retraction because
      * it digests retraction first then generate new retraction messages by itself.
      */
    def canDigestRetraction(relNode: RelNode): Boolean = {
      relNode match {
        case _: DataStreamGroupAggregate => true
        case _: DataStreamGroupWindowAggregate => true
        case _: DataStreamOverAggregate => true
        case _ => false
      }
    }

    /**
      * Return true if topRel needs to work under AccRetract
      */
    def topRelNeedSetAccRetract(topRel: RelNode, bottomRels: ListBuffer[RelNode]): Boolean = {
      var needSet: Boolean = false
      var bottomRelsUnderAccRetract: Boolean = false
      val traitSet = topRel.getTraitSet

      // check if top RelNode needs to work under AccRetract by itself
      needSet = needSetAccRetract(topRel)

      // check if there is a bottom RelNode working under AccRetract
      var i = 0
      while(i < bottomRels.size) {
        if (traitSetContainAccRetract(bottomRels(i).getTraitSet)) {
          bottomRelsUnderAccRetract = true
        }
        i += 1
      }

      // Return true
      // if topRel needs to work under AccRetract by itself
      // or bottom relNodes make topRel to work under AccRetract
      needSet ||
        (bottomRelsUnderAccRetract
          && !canDigestRetraction(topRel)
          && !traitSetContainAccRetract(traitSet))
    }

    /**
      * Returns a new topRel and a needTransform flag for a given topRel and bottomRels. The new
      * topRel contains new bottomRels with AccMode properly setted. The needTransform flag
      * will be true if any transformation has been done.
      *
      * @param topRel The input top RelNode.
      * @param bottomRels The input bottom RelNodes.
      * @return A tuple holding a new top RelNode and a needTransform flag
      */
    def accModeProcess(topRel: RelNode, bottomRels: ListBuffer[RelNode]): (RelNode, Boolean) = {
      var needTransform = false
      var i = 0
      var newTopRel = topRel

      // process bottom RelNodes
      while (i < bottomRels.size) {
        val bottomRel = bottomRels(i)
        if(needSetAccRetract(bottomRel)) {
          needTransform = true
          bottomRels(i) = setAccRetract(bottomRel)
        }
        i += 1
      }

      // process top RelNode
      if (topRelNeedSetAccRetract(topRel, bottomRels)) {
        needTransform = true
        newTopRel = setAccRetract(topRel)
      }

      newTopRel = newTopRel.copy(newTopRel.getTraitSet, bottomRels.asJava)
      (newTopRel, needTransform)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val topRel = call.rel(0).asInstanceOf[DataStreamRel]

      // get bottom relnodes
      val bottomRels = getChildRelNodes(topRel)

      // process bottom and top relnodes
      val (newTopRel, needTransform) = accModeProcess(topRel, bottomRels)

      if (needTransform) {
        call.transformTo(newTopRel)
      }
    }
  }


  /**
    * Rule that init retraction trait inside a [[DataStreamRel]]. If a [[DataStreamRel]] does not
    * contain retraction trait, initialize one for the [[DataStreamRel]]. After
    * initiallization, needToRetract will be set to false and AccMode will be set to Acc.
    */
  class InitProcessRule extends RelOptRule(
    operand(
      classOf[DataStreamRel], none()),
    "InitProcessRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {
      val rel = call.rel(0).asInstanceOf[DataStreamRel]
      var retractionTrait = rel.getTraitSet.getTrait(RetractionTraitDef.INSTANCE)
      if (null == retractionTrait) {
        retractionTrait = new RetractionTrait(false, AccMode.Acc)
        call.transformTo(rel.copy(rel.getTraitSet.plus(retractionTrait), rel.getInputs))
      }
    }
  }
}


