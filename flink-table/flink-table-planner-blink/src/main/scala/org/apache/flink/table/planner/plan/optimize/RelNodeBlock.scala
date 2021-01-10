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

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.plan.`trait`.MiniBatchInterval
import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink
import org.apache.flink.table.planner.plan.reuse.SubplanReuser.{SubplanReuseContext, SubplanReuseShuttle}
import org.apache.flink.table.planner.plan.rules.logical.WindowPropertiesRules
import org.apache.flink.table.planner.plan.utils.{DefaultRelShuttle, ExpandTableScanShuttle}
import org.apache.flink.util.Preconditions

import com.google.common.collect.Sets
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.{Aggregate, Project, Snapshot, TableFunctionScan, Union}
import org.apache.calcite.rex.RexNode

import java.lang.{Boolean => JBoolean}
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A [[RelNodeBlock]] is a sub-tree in the [[RelNode]] DAG, and represents common sub-graph
  * in [[CommonSubGraphBasedOptimizer]]. All [[RelNode]]s in each block have
  * only one [[LegacySink]] output.
  *
  * The algorithm works as follows:
  * 1. If there is only one tree, the whole tree is in one block. (the next steps is needless.)
  * 2. reuse common sub-plan in different RelNode tree, generate a RelNode DAG,
  * 3. traverse each tree from root to leaf, and mark the sink RelNode of each RelNode
  * 4. traverse each tree from root to leaf again, if meet a RelNode which has multiple sink
  * RelNode, the RelNode is the output node of a new block (or named break-point).
  * There are several special cases that a RelNode can not be a break-point.
  * (1). UnionAll is not a break-point
  * when [[RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED]] is false
  * (2). [[TableFunctionScan]], [[Snapshot]] or window aggregate ([[Aggregate]] on a [[Project]]
  * with window attribute) are not a break-point because their physical RelNodes are a composite
  * RelNode, each of them cannot be optimized individually. e.g. FlinkLogicalTableFunctionScan and
  * FlinkLogicalCorrelate will be combined into a BatchExecCorrelate or a StreamExecCorrelate.
  *
  * For example: (Table API)
  *
  * {{{-
  *  val sourceTable = tEnv.scan("test_table").select('a, 'b, 'c)
  *  val leftTable = sourceTable.filter('a > 0).select('a as 'a1, 'b as 'b1)
  *  val rightTable = sourceTable.filter('c.isNotNull).select('b as 'b2, 'c as 'c2)
  *  val joinTable = leftTable.join(rightTable, 'a1 === 'b2)
  *  joinTable.where('a1 >= 70).select('a1, 'b1).writeToSink(sink1)
  *  joinTable.where('a1 < 70 ).select('a1, 'c2).writeToSink(sink2)
  * }}}
  *
  * the RelNode DAG is:
  *
  * {{{-
  * Sink(sink1)     Sink(sink2)
  *    |               |
  * Project(a1,b1)  Project(a1,c2)
  *    |               |
  * Filter(a1>=70)  Filter(a1<70)
  *       \          /
  *        Join(a1=b2)
  *       /           \
  * Project(a1,b1)  Project(b2,c2)
  *      |             |
  * Filter(a>0)     Filter(c is not null)
  *      \           /
  *      Project(a,b,c)
  *          |
  *       TableScan
  * }}}
  *
  * This [[RelNode]] DAG will be decomposed into three [[RelNodeBlock]]s, the break-point
  * is the [[RelNode]](`Join(a1=b2)`) which data outputs to multiple [[LegacySink]]s.
  * <p>Notes: Although `Project(a,b,c)` has two parents (outputs),
  * they eventually merged at `Join(a1=b2)`. So `Project(a,b,c)` is not a break-point.
  * <p>the first [[RelNodeBlock]] includes TableScan, Project(a,b,c), Filter(a>0),
  * Filter(c is not null), Project(a1,b1), Project(b2,c2) and Join(a1=b2)
  * <p>the second one includes Filter(a1>=70), Project(a1,b1) and Sink(sink1)
  * <p>the third one includes Filter(a1<70), Project(a1,c2) and Sink(sink2)
  * <p>And the first [[RelNodeBlock]] is the child of another two.
  *
  * The [[RelNodeBlock]] plan is:
  * {{{-
  * RelNodeBlock2  RelNodeBlock3
  *        \            /
  *        RelNodeBlock1
  * }}}
  *
  * The optimizing order is from child block to parent. The optimized result (RelNode)
  * will be wrapped as an IntermediateRelTable first, and then be converted to a new TableScan
  * which is the new output node of current block and is also the input of its parent blocks.
  *
  * @param outputNode A RelNode of the output in the block, which could be a [[LegacySink]] or
  *                   other RelNode which data outputs to multiple [[LegacySink]]s.
  */
class RelNodeBlock(val outputNode: RelNode) {
  // child (or input) blocks
  private val childBlocks = mutable.LinkedHashSet[RelNodeBlock]()

  // After this block has been optimized, the result will be converted to a new TableScan as
  // new output node
  private var newOutputNode: Option[RelNode] = None

  private var outputTableName: Option[String] = None

  private var optimizedPlan: Option[RelNode] = None

  // whether any parent block requires UPDATE_BEFORE messages
  private var updateBeforeRequired: Boolean = false

  private var miniBatchInterval: MiniBatchInterval = MiniBatchInterval.NONE

  def addChild(block: RelNodeBlock): Unit = childBlocks += block

  def children: Seq[RelNodeBlock] = childBlocks.toSeq

  def setNewOutputNode(newNode: RelNode): Unit = newOutputNode = Option(newNode)

  def getNewOutputNode: Option[RelNode] = newOutputNode

  def setOutputTableName(name: String): Unit = outputTableName = Option(name)

  def getOutputTableName: String = outputTableName.orNull

  def setOptimizedPlan(rel: RelNode): Unit = this.optimizedPlan = Option(rel)

  def getOptimizedPlan: RelNode = optimizedPlan.orNull

  def setUpdateBeforeRequired(requireUpdateBefore: Boolean): Unit = {
    // set the child block whether need to produce update before messages for updates,
    // a child block may have multiple parents (outputs), if one of the parents require
    // update before message, then this child block has to produce update before for updates.
    if (requireUpdateBefore) {
      this.updateBeforeRequired = true
    }
  }

  /**
   * Returns true if any parent block requires UPDATE_BEFORE messages for updates.
   */
  def isUpdateBeforeRequired: Boolean = updateBeforeRequired

  def setMiniBatchInterval(miniBatchInterval: MiniBatchInterval): Unit = {
    this.miniBatchInterval = miniBatchInterval
  }

  def getMiniBatchInterval: MiniBatchInterval = miniBatchInterval

  def getChildBlock(node: RelNode): Option[RelNodeBlock] = {
    val find = children.filter(_.outputNode.equals(node))
    if (find.isEmpty) {
      None
    } else {
      Preconditions.checkArgument(find.size == 1)
      Some(find.head)
    }
  }

  /**
    * Get new plan of this block. The child blocks (inputs) will be replace with new RelNodes (the
    * optimized result of child block).
    *
    * @return New plan of this block
    */
  def getPlan: RelNode = {
    val shuttle = new RelNodeBlockShuttle
    outputNode.accept(shuttle)
  }

  private class RelNodeBlockShuttle extends DefaultRelShuttle {
    override def visit(rel: RelNode): RelNode = {
      val block = getChildBlock(rel)
      block match {
        case Some(b) => b.getNewOutputNode.get
        case _ => super.visit(rel)
      }
    }
  }

}

/**
  * Holds information to build [[RelNodeBlock]].
  */
class RelNodeWrapper(relNode: RelNode) {
  // parent nodes of `relNode`
  private val parentNodes = Sets.newIdentityHashSet[RelNode]()
  // output nodes of some blocks that data of `relNode` outputs to
  private val blockOutputNodes = Sets.newIdentityHashSet[RelNode]()
  // stores visited parent nodes when builds RelNodeBlock
  private val visitedParentNodes = Sets.newIdentityHashSet[RelNode]()

  def addParentNode(parent: Option[RelNode]): Unit = {
    parent match {
      case Some(p) => parentNodes.add(p)
      case None => // Ignore
    }
  }

  def addVisitedParentNode(parent: Option[RelNode]): Unit = {
    parent match {
      case Some(p) =>
        require(parentNodes.contains(p))
        visitedParentNodes.add(p)
      case None => // Ignore
    }
  }

  def addBlockOutputNode(blockOutputNode: RelNode): Unit = blockOutputNodes.add(blockOutputNode)

  /**
    * Returns true if all parent nodes had been visited, else false
    */
  def allParentNodesVisited: Boolean = parentNodes.size() == visitedParentNodes.size()

  /**
    * Returns true if number of `blockOutputNodes` is greater than 1, else false
    */
  def hasMultipleBlockOutputNodes: Boolean = blockOutputNodes.size() > 1

  /**
    * Returns the output node of the block that the `relNode` belongs to
    */
  def getBlockOutputNode: RelNode = {
    if (hasMultipleBlockOutputNodes) {
      // If has multiple block output nodes, the `relNode` is a break-point.
      // So the `relNode` is the output node of the block that the `relNode` belongs to
      relNode
    } else {
      // the `relNode` is not a break-point
      require(blockOutputNodes.size == 1)
      blockOutputNodes.head
    }
  }
}

/**
  * Builds [[RelNodeBlock]] plan
  */
class RelNodeBlockPlanBuilder private(config: TableConfig) {

  private val node2Wrapper = new util.IdentityHashMap[RelNode, RelNodeWrapper]()
  private val node2Block = new util.IdentityHashMap[RelNode, RelNodeBlock]()

  private val isUnionAllAsBreakPointEnabled = config.getConfiguration.getBoolean(
    RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED)

  /**
    * Decompose the [[RelNode]] plan into many [[RelNodeBlock]]s,
    * and rebuild [[RelNodeBlock]] plan.
    *
    * @param  sinks RelNode DAG to decompose
    * @return Sink-RelNodeBlocks, each Sink-RelNodeBlock is a tree.
    */
  def buildRelNodeBlockPlan(sinks: Seq[RelNode]): Seq[RelNodeBlock] = {
    sinks.foreach(buildRelNodeWrappers(_, None))
    buildBlockOutputNodes(sinks)
    sinks.map(buildBlockPlan)
  }

  private def buildRelNodeWrappers(node: RelNode, parent: Option[RelNode]): Unit = {
    node2Wrapper.getOrElseUpdate(node, new RelNodeWrapper(node)).addParentNode(parent)
    node.getInputs.foreach(child => buildRelNodeWrappers(child, Some(node)))
  }

  private def buildBlockPlan(node: RelNode): RelNodeBlock = {
    val currentBlock = new RelNodeBlock(node)
    buildBlock(node, currentBlock, createNewBlockWhenMeetValidBreakPoint = false)
    currentBlock
  }

  private def buildBlock(
      node: RelNode,
      currentBlock: RelNodeBlock,
      createNewBlockWhenMeetValidBreakPoint: Boolean): Unit = {
    val hasDiffBlockOutputNodes = node2Wrapper(node).hasMultipleBlockOutputNodes
    val validBreakPoint = isValidBreakPoint(node)

    if (validBreakPoint && (createNewBlockWhenMeetValidBreakPoint || hasDiffBlockOutputNodes)) {
      val childBlock = node2Block.getOrElseUpdate(node, new RelNodeBlock(node))
      currentBlock.addChild(childBlock)
      node.getInputs.foreach {
        child => buildBlock(child, childBlock, createNewBlockWhenMeetValidBreakPoint = false)
      }
    } else {
      val newCreateNewBlockWhenMeetValidBreakPoint =
        createNewBlockWhenMeetValidBreakPoint || hasDiffBlockOutputNodes && !validBreakPoint
      node.getInputs.foreach {
        child => buildBlock(child, currentBlock, newCreateNewBlockWhenMeetValidBreakPoint)
      }
    }
  }

  /**
    * TableFunctionScan/Snapshot/Window Aggregate cannot be optimized individually,
    * so TableFunctionScan/Snapshot/Window Aggregate is not a valid break-point
    * even though it has multiple parents.
    */
  private def isValidBreakPoint(node: RelNode): Boolean = node match {
    case _: TableFunctionScan | _: Snapshot => false
    case union: Union if union.all => isUnionAllAsBreakPointEnabled
    case project: Project => project.getProjects.forall(p => !hasWindowGroup(p))
    case agg: Aggregate =>
      agg.getInput match {
        case project: Project =>
          agg.getGroupSet.forall { group =>
            val p = project.getProjects.get(group)
            !hasWindowGroup(p)
          }
        case _ => true
      }
    case _ => true
  }

  private def hasWindowGroup(rexNode: RexNode): Boolean = {
    WindowPropertiesRules.hasGroupAuxiliaries(rexNode) ||
      WindowPropertiesRules.hasGroupFunction(rexNode)
  }

  private def buildBlockOutputNodes(sinks: Seq[RelNode]): Unit = {
    // init sink block output node
    sinks.foreach(sink => node2Wrapper.get(sink).addBlockOutputNode(sink))

    val unvisitedNodeQueue: util.Deque[RelNode] = new util.ArrayDeque[RelNode]()
    unvisitedNodeQueue.addAll(sinks)
    while (unvisitedNodeQueue.nonEmpty) {
      val node = unvisitedNodeQueue.removeFirst()
      val wrapper = node2Wrapper.get(node)
      require(wrapper != null)
      val blockOutputNode = wrapper.getBlockOutputNode
      buildBlockOutputNodes(None, node, blockOutputNode, unvisitedNodeQueue)
    }
  }

  private def buildBlockOutputNodes(
      parent: Option[RelNode],
      node: RelNode,
      curBlockOutputNode: RelNode,
      unvisitedNodeQueue: util.Deque[RelNode]): Unit = {
    val wrapper = node2Wrapper.get(node)
    require(wrapper != null)
    wrapper.addBlockOutputNode(curBlockOutputNode)
    wrapper.addVisitedParentNode(parent)

    // the node can be visited only when its all parent nodes have been visited
    if (wrapper.allParentNodesVisited) {
      val newBlockOutputNode = if (wrapper.hasMultipleBlockOutputNodes) {
        // if the node has different output node, the node is the output node of current block.
        node
      } else {
        curBlockOutputNode
      }
      node.getInputs.foreach { input =>
        buildBlockOutputNodes(Some(node), input, newBlockOutputNode, unvisitedNodeQueue)
      }
      unvisitedNodeQueue.remove(node)
    } else {
      // visit later
      unvisitedNodeQueue.addLast(node)
    }
  }

}

object RelNodeBlockPlanBuilder {

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_UNIONALL_AS_BREAKPOINT_ENABLED: ConfigOption[JBoolean] =
    key("table.optimizer.union-all-as-breakpoint-enabled")
        .defaultValue(JBoolean.valueOf(true))
        .withDescription("When true, the optimizer will breakup the graph at union-all node " +
          "when it's a breakpoint. When false, the optimizer will skip the union-all node " +
          "even it's a breakpoint, and will try find the breakpoint in its inputs.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED: ConfigOption[JBoolean] =
    key("table.optimizer.reuse-optimize-block-with-digest-enabled")
        .defaultValue(JBoolean.valueOf(false))
        .withDescription("When true, the optimizer will try to find out duplicated sub-plan by " +
            "digest to build optimize block(a.k.a. common sub-graph). " +
            "Each optimize block will be optimized independently.")

  /**
    * Decompose the [[RelNode]] trees into [[RelNodeBlock]] trees. First, convert LogicalNode
    * trees to RelNode trees. Second, reuse same sub-plan in different trees. Third, decompose the
    * RelNode dag to [[RelNodeBlock]] trees.
    *
    * @param  sinkNodes SinkNodes belongs to a LogicalNode plan.
    * @return Sink-RelNodeBlocks, each Sink-RelNodeBlock is a tree.
    */
  def buildRelNodeBlockPlan(
      sinkNodes: Seq[RelNode],
      config: TableConfig): Seq[RelNodeBlock] = {
    require(sinkNodes.nonEmpty)

    // expand QueryOperationCatalogViewTable in TableScan
    val shuttle = new ExpandTableScanShuttle
    val convertedRelNodes = sinkNodes.map(_.accept(shuttle))

    if (convertedRelNodes.size == 1) {
      Seq(new RelNodeBlock(convertedRelNodes.head))
    } else {
      // merge multiple RelNode trees to RelNode dag
      val relNodeDag = reuseRelNodes(convertedRelNodes, config)
      val builder = new RelNodeBlockPlanBuilder(config)
      builder.buildRelNodeBlockPlan(relNodeDag)
    }
  }

  /**
    * Reuse common sub-plan in different RelNode tree, generate a RelNode dag
    *
    * @param relNodes RelNode trees
    * @return RelNode dag which reuse common subPlan in each tree
    */
  private def reuseRelNodes(relNodes: Seq[RelNode], tableConfig: TableConfig): Seq[RelNode] = {
    val findOpBlockWithDigest = tableConfig.getConfiguration.getBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED)
    if (!findOpBlockWithDigest) {
      return relNodes
    }

    // reuse sub-plan with same digest in input RelNode trees.
    val context = new SubplanReuseContext(true, relNodes: _*)
    val reuseShuttle = new SubplanReuseShuttle(context)
    relNodes.map(_.accept(reuseShuttle))
  }

}
