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

package org.apache.flink.table.plan.optimize

import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableImpl}
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecIntermediateTableScan, StreamPhysicalRel}
import org.apache.flink.table.plan.optimize.program.{FlinkStreamProgram, StreamOptimizeContext}
import org.apache.flink.table.plan.schema.IntermediateRelTable
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sinks.{DataStreamTableSink, RetractStreamTableSink}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexBuilder

import java.util

import scala.collection.JavaConversions._

/**
  * A [[CommonSubGraphBasedOptimizer]] for Stream.
  */
class StreamCommonSubGraphBasedOptimizer(tEnv: StreamTableEnvironment)
  extends CommonSubGraphBasedOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // build RelNodeBlock plan
    val sinkBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, tEnv)
    // infer updateAsRetraction property for sink block
    sinkBlocks.foreach { sinkBlock =>
      val retractionFromRoot = sinkBlock.outputNode match {
        case n: Sink =>
          n.sink match {
            case _: RetractStreamTableSink[_] => true
            case s: DataStreamTableSink[_] => s.updatesAsRetraction
            case _ => false
          }
        case o =>
          o.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE).sendsUpdatesAsRetractions
      }
      sinkBlock.setUpdateAsRetraction(retractionFromRoot)
    }

    if (sinkBlocks.size == 1) {
      // If there is only one sink block, the given relational expressions are a simple tree
      // (only one root), not a dag. So many operations (e.g. `infer updateAsRetraction property`,
      // `propagate updateAsRetraction property`) can be omitted to save optimization time.
      val block = sinkBlocks.head
      val optimizedTree = optimizeTree(
        block.getPlan,
        block.isUpdateAsRetraction,
        isSinkBlock = true)
      block.setOptimizedPlan(optimizedTree)
      return sinkBlocks
    }

    // infer updateAsRetraction property for all input blocks
    sinkBlocks.foreach(b => inferUpdateAsRetraction(
      b, b.isUpdateAsRetraction, isSinkBlock = true))
    // propagate updateAsRetraction to all input blocks
    sinkBlocks.foreach(propagateTraits)
    // clear the intermediate result
    sinkBlocks.foreach(resetIntermediateResult)
    // optimize recursively RelNodeBlock
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
    sinkBlocks
  }

  private def optimizeBlock(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          optimizeBlock(child, isSinkBlock = false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case s: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree(
          s,
          updatesAsRetraction = block.isUpdateAsRetraction,
          isSinkBlock = true)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree(
          o,
          updatesAsRetraction = block.isUpdateAsRetraction,
          isSinkBlock = isSinkBlock)
        val isAccRetract = optimizedPlan.getTraitSet
          .getTrait(AccModeTraitDef.INSTANCE).getAccMode == AccMode.AccRetract
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(tEnv, name, optimizedPlan, isAccRetract)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.asInstanceOf[TableImpl].getRelNode)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @param updatesAsRetraction True if request updates as retraction messages.
    * @param isSinkBlock True if the given block is sink block.
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(
      relNode: RelNode,
      updatesAsRetraction: Boolean,
      isSinkBlock: Boolean): RelNode = {

    val config = tEnv.getConfig
    val programs = config.getCalciteConfig.getStreamProgram
      .getOrElse(FlinkStreamProgram.buildProgram(config.getConf))
    Preconditions.checkNotNull(programs)

    programs.optimize(relNode, new StreamOptimizeContext() {

      override def getRexBuilder: RexBuilder = tEnv.getRelBuilder.getRexBuilder

      override def needFinalTimeIndicatorConversion: Boolean = true

      override def getTableConfig: TableConfig = config

      override def getVolcanoPlanner: VolcanoPlanner = tEnv.getPlanner.asInstanceOf[VolcanoPlanner]

      override def updateAsRetraction: Boolean = updatesAsRetraction
    })
  }

  /**
    * Infer UpdateAsRetraction property for each block.
    * NOTES: this method should not change the original RelNode tree.
    *
    * @param block              The [[RelNodeBlock]] instance.
    * @param retractionFromRoot Whether the sink need update as retraction messages.
    * @param isSinkBlock        True if the given block is sink block.
    */
  private def inferUpdateAsRetraction(
      block: RelNodeBlock,
      retractionFromRoot: Boolean,
      isSinkBlock: Boolean): Unit = {

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          inferUpdateAsRetraction(child, retractionFromRoot = false, isSinkBlock = false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case n: Sink =>
        require(isSinkBlock)
        val optimizedPlan = optimizeTree(
          n, retractionFromRoot, isSinkBlock = true)
        block.setOptimizedPlan(optimizedPlan)

      case o =>
        val optimizedPlan = optimizeTree(
          o, retractionFromRoot, isSinkBlock = isSinkBlock)
        val name = tEnv.createUniqueTableName()
        registerIntermediateTable(tEnv, name, optimizedPlan, isAccRetract = false)
        val newTable = tEnv.scan(name)
        block.setNewOutputNode(newTable.asInstanceOf[TableImpl].getRelNode)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Propagate updateAsRetraction to all input blocks.
    *
    * @param block The [[RelNodeBlock]] instance.
    */
  private def propagateTraits(block: RelNodeBlock): Unit = {

    // process current block
    def shipTraits(rel: RelNode, updateAsRetraction: Boolean): Unit = {
      rel match {
        case _: StreamExecDataStreamScan | _: StreamExecIntermediateTableScan =>
          val scan = rel.asInstanceOf[TableScan]
          val retractionTrait = scan.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)
          val tableName = scan.getTable.getQualifiedName.last
          val inputBlocks = block.children.filter(_.getOutputTableName eq tableName)
          Preconditions.checkArgument(inputBlocks.size <= 1)
          if (inputBlocks.size == 1) {
            if (retractionTrait.sendsUpdatesAsRetractions || updateAsRetraction) {
              inputBlocks.head.setUpdateAsRetraction(true)
            }
          }
        case ser: StreamPhysicalRel => ser.getInputs.foreach { e =>
          if (ser.needsUpdatesAsRetraction(e) || (updateAsRetraction && !ser.consumesRetractions)) {
            shipTraits(e, updateAsRetraction = true)
          } else {
            shipTraits(e, updateAsRetraction = false)
          }
        }
      }
    }

    shipTraits(block.getOptimizedPlan, block.isUpdateAsRetraction)
    block.children.foreach(propagateTraits)
  }

  /**
    * Reset the intermediate result including newOutputNode and outputTableName
    *
    * @param block the [[RelNodeBlock]] instance.
    */
  private def resetIntermediateResult(block: RelNodeBlock): Unit = {
    block.setNewOutputNode(null)
    block.setOutputTableName(null)

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.nonEmpty) {
          resetIntermediateResult(child)
        }
    }
  }

  private def registerIntermediateTable(
      tEnv: StreamTableEnvironment,
      name: String,
      relNode: RelNode,
      isAccRetract: Boolean): Unit = {
    val uniqueKeys = getUniqueKeys(tEnv, relNode)
    val monotonicity = FlinkRelMetadataQuery
      .reuseOrCreate(tEnv.getRelBuilder.getCluster.getMetadataQuery)
      .getRelModifiedMonotonicity(relNode)
    val statistic = FlinkStatistic.builder()
      .uniqueKeys(uniqueKeys)
      .relModifiedMonotonicity(monotonicity)
      .build()

    val table = new IntermediateRelTable(
      relNode,
      isAccRetract,
      statistic)
    tEnv.registerTableInternal(name, table)
  }

  private def getUniqueKeys(
      tEnv: StreamTableEnvironment,
      relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(tEnv.getRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(relNode)
    if (uniqueKeys != null) {
      uniqueKeys.filter(_.nonEmpty).map { uniqueKey =>
        val keys = new util.HashSet[String]()
        uniqueKey.asList().foreach { idx =>
          keys.add(rowType.getFieldNames.get(idx))
        }
        keys
      }
    } else {
      null
    }
  }

}
