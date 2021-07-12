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

package org.apache.flink.table.planner.plan.optimize

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkRelBuilder, SqlExprToRexConverterFactory}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.{LegacySink, Sink}
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.optimize.program.{FlinkStreamProgram, StreamOptimizeContext}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.util.Preconditions

import java.util
import java.util.Collections
import scala.collection.JavaConversions._

/**
  * A [[CommonSubGraphBasedOptimizer]] for Stream.
  */
class StreamCommonSubGraphBasedOptimizer(planner: StreamPlanner)
  extends CommonSubGraphBasedOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    val config = planner.getTableConfig
    // build RelNodeBlock plan
    val sinkBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, config)
    // infer trait properties for sink block
    sinkBlocks.foreach { sinkBlock =>
      // don't require update before by default
      sinkBlock.setUpdateBeforeRequired(false)

      val miniBatchInterval: MiniBatchInterval = if (config.getConfiguration.getBoolean(
        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
        val miniBatchLatency = config.getConfiguration.get(
          ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY).toMillis
        Preconditions.checkArgument(miniBatchLatency > 0,
          "MiniBatch Latency must be greater than 0 ms.", null)
        new MiniBatchInterval(miniBatchLatency, MiniBatchMode.ProcTime)
      }  else {
        MiniBatchIntervalTrait.NONE.getMiniBatchInterval
      }
      sinkBlock.setMiniBatchInterval(miniBatchInterval)
    }

    if (sinkBlocks.size == 1) {
      // If there is only one sink block, the given relational expressions are a simple tree
      // (only one root), not a dag. So many operations (e.g. infer and propagate
      // requireUpdateBefore) can be omitted to save optimization time.
      val block = sinkBlocks.head
      val optimizedTree = optimizeTree(
        block.getPlan,
        block.isUpdateBeforeRequired,
        block.getMiniBatchInterval,
        isSinkBlock = true)
      block.setOptimizedPlan(optimizedTree)
      return sinkBlocks
    }

    // TODO FLINK-24048: Move changeLog inference out of optimizing phase
    // infer modifyKind property for each blocks independently
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
    // infer and propagate updateKind and miniBatchInterval property for each blocks
    sinkBlocks.foreach { b =>
      propagateUpdateKindAndMiniBatchInterval(
        b, b.isUpdateBeforeRequired, b.getMiniBatchInterval, isSinkBlock = true)
    }
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
      case _: LegacySink | _: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree(
          blockLogicalPlan,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = true)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree(
          o,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = isSinkBlock)
        val modifyKindSetTrait = optimizedPlan.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable = createIntermediateRelTable(
          name,
          optimizedPlan,
          modifyKindSetTrait.modifyKindSet,
          block.isUpdateBeforeRequired)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @param updateBeforeRequired True if UPDATE_BEFORE message is required for updates
    * @param miniBatchInterval mini-batch interval of the block.
    * @param isSinkBlock True if the given block is sink block.
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(
      relNode: RelNode,
      updateBeforeRequired: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean): RelNode = {

    val config = planner.getTableConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(config)
    val programs = calciteConfig.getStreamProgram
      .getOrElse(FlinkStreamProgram.buildProgram(config.getConfiguration))
    Preconditions.checkNotNull(programs)

    val context = relNode.getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])

    programs.optimize(relNode, new StreamOptimizeContext() {

      override def isBatchMode: Boolean = false

      override def getTableConfig: TableConfig = config

      override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

      override def getCatalogManager: CatalogManager = planner.catalogManager

      override def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory =
        context.getSqlExprToRexConverterFactory

      override def getFlinkRelBuilder: FlinkRelBuilder = planner.getRelBuilder

      override def isUpdateBeforeRequired: Boolean = updateBeforeRequired

      def getMiniBatchInterval: MiniBatchInterval = miniBatchInterval

      override def needFinalTimeIndicatorConversion: Boolean = isSinkBlock
    })
  }

  /**
   * Infer updateKind and MiniBatchInterval property for each block.
   * Optimize order: from parent block to child blocks.
   * NOTES: this method should not change the original RelNode tree.
   *
   * @param block              The [[RelNodeBlock]] instance.
   * @param updateBeforeRequired True if UPDATE_BEFORE message is required for updates
   * @param miniBatchInterval  mini-batch interval of the block.
   * @param isSinkBlock        True if the given block is sink block.
   */
  private def propagateUpdateKindAndMiniBatchInterval(
      block: RelNodeBlock,
      updateBeforeRequired: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean): Unit = {
    val blockLogicalPlan = block.getPlan
    // infer updateKind and miniBatchInterval with required trait
    val optimizedPlan = optimizeTree(
      blockLogicalPlan, updateBeforeRequired, miniBatchInterval, isSinkBlock)
    // propagate the inferred updateKind and miniBatchInterval to the child blocks
    propagateTraits(optimizedPlan)

    block.children.foreach {
      child =>
        propagateUpdateKindAndMiniBatchInterval(
          child,
          updateBeforeRequired = child.isUpdateBeforeRequired,
          miniBatchInterval = child.getMiniBatchInterval,
          isSinkBlock = false)
    }

    def propagateTraits(rel: RelNode): Unit = rel match {
      case _: StreamPhysicalDataStreamScan | _: StreamPhysicalIntermediateTableScan |
           _: StreamPhysicalLegacyTableSourceScan | _: StreamPhysicalTableSourceScan =>
        val scan = rel.asInstanceOf[TableScan]
        val updateKindTrait = scan.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
        val miniBatchIntervalTrait = scan.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
        val tableName = scan.getTable.getQualifiedName.mkString(".")
        val inputBlocks = block.children.filter(b => tableName.equals(b.getOutputTableName))
        Preconditions.checkArgument(inputBlocks.size <= 1)
        if (inputBlocks.size == 1) {
          val childBlock = inputBlocks.head
          // propagate miniBatchInterval trait to child block
          childBlock.setMiniBatchInterval(miniBatchIntervalTrait.getMiniBatchInterval)
          // propagate updateKind trait to child block
          val requireUB = updateKindTrait.updateKind == UpdateKind.BEFORE_AND_AFTER
          childBlock.setUpdateBeforeRequired(requireUB || childBlock.isUpdateBeforeRequired)
        }
      case ser: StreamPhysicalRel => ser.getInputs.foreach(e => propagateTraits(e))
      case _ => // do nothing
    }
  }

  /**
    * Reset the intermediate result including newOutputNode and outputTableName
    *
    * @param block the [[RelNodeBlock]] instance.
    */
  private def resetIntermediateResult(block: RelNodeBlock): Unit = {
    block.setNewOutputNode(null)
    block.setOutputTableName(null)
    block.setOptimizedPlan(null)

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.nonEmpty) {
          resetIntermediateResult(child)
        }
    }
  }

  private def createIntermediateRelTable(
      name: String,
      relNode: RelNode,
      modifyKindSet: ModifyKindSet,
      isUpdateBeforeRequired: Boolean): IntermediateRelTable = {
    val uniqueKeys = getUniqueKeys(relNode)
    val fmq = FlinkRelMetadataQuery
      .reuseOrCreate(planner.getRelBuilder.getCluster.getMetadataQuery)
    val monotonicity = fmq.getRelModifiedMonotonicity(relNode)
    val windowProperties = fmq.getRelWindowProperties(relNode)
    val statistic = FlinkStatistic.builder()
      .uniqueKeys(uniqueKeys)
      .relModifiedMonotonicity(monotonicity)
      .relWindowProperties(windowProperties)
      .build()
    new IntermediateRelTable(
      Collections.singletonList(name),
      relNode,
      modifyKindSet,
      isUpdateBeforeRequired,
      fmq.getUpsertKeys(relNode),
      statistic)
  }

  private def getUniqueKeys(relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(planner.getRelBuilder.getCluster.getMetadataQuery)
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
