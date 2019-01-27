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

package org.apache.flink.table.plan.subplan

import org.apache.flink.table.api.{TableConfigOptions, TableEnvironment, TableException}
import org.apache.flink.table.plan.logical.{LogicalNode, SinkNode}
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.rules.logical.WindowPropertiesRules
import org.apache.flink.table.plan.schema.RelTable
import org.apache.flink.table.plan.util.{SubplanReuseContext, SubplanReuseShuttle}
import org.apache.flink.util.Preconditions

import com.google.common.collect.Sets
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.{Aggregate, Project, Snapshot, TableFunctionScan, TableScan, Union}
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rex.RexNode

import java.util.IdentityHashMap

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A [[RelNodeBlock]] is a sub-tree in the [[RelNode]] plan. All [[RelNode]]s in
  * each block have at most one parent (output) node.
  * The nodes in different block will be optimized independently.
  *
  * For example: (Table API)
  *
  * {{{-
  *  val table = tEnv.scan("test_table").select('a, 'b, 'c)
  *  table.where('a >= 70).select('a, 'b).writeToSink(sink1)
  *  table.where('a < 70 ).select('a, 'c).writeToSink(sink2)
  * }}}
  *
  * the RelNode DAG is:
  *
  * {{{-
  *        TableScan
  *            |
  *       Project(a,b,c)
  *        /          \
  * Filter(a>=70)  Filter(a<70)
  *     |              |
  * Project(a,b)  Project(a,c)
  *     |              |
  * Sink(sink1)   Sink(sink2)
  * }}}
  *
  * This [[RelNode]] DAG will be decomposed into three [[RelNodeBlock]]s, the break-point
  * is a [[RelNode]] which has more than one output nodes.
  * the first [[RelNodeBlock]] includes TableScan and Project('a,'b,'c)
  * the second one includes Filter('a>=70), Project('a,'b) and Sink(sink1)
  * the third one includes Filter('a<70), Project('a,'c), Sink(sink2)
  * And the first [[RelNodeBlock]] is the child of another two.
  * The [[RelNodeBlock]] plan is:
  *
  * {{{-
  *         RelNodeBlock1
  *          /            \
  * RelNodeBlock2  RelNodeBlock3
  * }}}
  *
  * The optimizing order is from child block to parent. The optimized result (DataStream)
  * will be registered into tables first, and then be converted to a new TableScan which is the
  * new output node of current block and is also the input of its parent blocks.
  *
  * @param outputNode A RelNode of the output in the block, which could be a [[Sink]] or
  *                   other RelNode with more than one parent nodes.
  */
class RelNodeBlock(val outputNode: RelNode, tEnv: TableEnvironment) {
  // child (or input) blocks
  private val childBlocks = mutable.LinkedHashSet[RelNodeBlock]()

  // After this block has been optimized, the result will be converted to a new TableScan as
  // new output node
  private var newOutputNode: Option[RelNode] = None

  private var outputTableName: Option[String] = None

  private var optimizedPlan: Option[RelNode] = None

  private var updateAsRetract: Boolean = false

  def addChild(block: RelNodeBlock): Unit = childBlocks += block

  def children: Seq[RelNodeBlock] = childBlocks.toSeq

  def setNewOutputNode(newNode: RelNode): Unit = newOutputNode = Option(newNode)

  def getNewOutputNode: Option[RelNode] = newOutputNode

  def setOutputTableName(name: String): Unit = outputTableName = Option(name)

  def getOutputTableName: String = outputTableName.orNull

  def setOptimizedPlan(rel: RelNode): Unit = this.optimizedPlan = Option(rel)

  def getOptimizedPlan: RelNode = optimizedPlan.orNull

  def setUpdateAsRetraction(updateAsRetract: Boolean): Unit = {
    // set child block updateAsRetract, a child may have multi father.
    if (updateAsRetract) {
      this.updateAsRetract = true
    }
  }

  def isUpdateAsRetraction: Boolean = updateAsRetract

  def isChildBlockOutputRelNode(node: RelNode): Option[RelNodeBlock] = {
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

  private class RelNodeBlockShuttle extends RelShuttleImpl {

    override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
      val block = isChildBlockOutputRelNode(parent)
      if (block.isDefined) {
        block.get.getNewOutputNode.get
      } else {
        super.visitChild(parent, i, child)
      }
    }

    override def visitChildren(rel: RelNode): RelNode = {
      val block = isChildBlockOutputRelNode(rel)
      if (block.isDefined) {
        block.get.getNewOutputNode.get
      } else {
        super.visitChildren(rel)
      }
    }
  }

}

/**
  * Builds [[RelNodeBlock]] plan
  */
class RelNodeBlockPlanBuilder private(tEnv: TableEnvironment) {

  private val node2Wrapper = new IdentityHashMap[RelNode, RelNodeWrapper]()
  private val node2Block = new IdentityHashMap[RelNode, RelNodeBlock]()

  private val isUnionAllAsBreakPointDisabled = tEnv.config.getConf.getBoolean(
    TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED)


  /**
    * Decompose the [[RelNode]] plan into many [[RelNodeBlock]]s,
    * and rebuild [[RelNodeBlock]] plan.
    *
    * @param  sinks RelNode DAG to decompose
    * @return Sink-RelNodeBlocks, each Sink-RelNodeBlock is a tree.
    */
  def buildRelNodeBlockPlan(sinks: Seq[RelNode]): Seq[RelNodeBlock] = {
    sinks.foreach(buildRelNodeWrappers(_, None))
    sinks.map(buildBlockPlan)
  }

  private def buildRelNodeWrappers(node: RelNode, parent: Option[RelNode]): Unit = {
    node2Wrapper.getOrElseUpdate(node, new RelNodeWrapper(node)).addParentNode(parent)
    node.getInputs.foreach(child => buildRelNodeWrappers(child, Some(node)))
  }

  private def buildBlockPlan(node: RelNode): RelNodeBlock = {
    val currentBlock = new RelNodeBlock(node, tEnv)
    buildBlock(node, currentBlock, createNewBlockWhenMeetValidBreakPoint = false)
    currentBlock
  }

  private def buildBlock(
      node: RelNode,
      currentBlock: RelNodeBlock,
      createNewBlockWhenMeetValidBreakPoint: Boolean): Unit = {
    val hasMultipleParents = node2Wrapper(node).hasMultipleParents
    val validBreakPoint = isValidBreakPoint(node)

    if (validBreakPoint && (createNewBlockWhenMeetValidBreakPoint || hasMultipleParents)) {
      val childBlock = node2Block.getOrElseUpdate(node, new RelNodeBlock(node, tEnv))
      currentBlock.addChild(childBlock)
      node.getInputs.foreach {
        child => buildBlock(child, childBlock, createNewBlockWhenMeetValidBreakPoint = false)
      }
    } else {
      val newCreateNewBlockWhenMeetValidBreakPoint =
        createNewBlockWhenMeetValidBreakPoint || hasMultipleParents && !validBreakPoint
      node.getInputs.foreach {
        child => buildBlock(child, currentBlock, newCreateNewBlockWhenMeetValidBreakPoint)
      }
    }
  }

  /**
    * TableFunctionScan/Snapshot/Window Aggregate cannot be optimized individually,
    * so TableFunctionScan/Snapshot/Window Aggregate is not a break-point
    * even though it has multiple parents.
    */
  private def isValidBreakPoint(node: RelNode): Boolean = node match {
    case _: TableFunctionScan | _: Snapshot => false
    case union: Union if union.all => !isUnionAllAsBreakPointDisabled
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

}

object RelNodeBlockPlanBuilder {

  /**
    * Decompose the [[LogicalNode]] trees into [[RelNodeBlock]] trees. First, convert LogicalNode
    * trees to RelNode trees. Second, reuse same sub-plan in different trees. Third, decompose the
    * RelNode dag to [[RelNodeBlock]] trees.
    *
    * @param  sinkNodes SinkNodes belongs to a LogicalNode plan.
    * @return Sink-RelNodeBlocks, each Sink-RelNodeBlock is a tree.
    */
  def buildRelNodeBlockPlan(
      sinkNodes: Seq[LogicalNode],
      tEnv: TableEnvironment): Seq[RelNodeBlock] = {

    // checks sink node
    sinkNodes.foreach {
      case _: SinkNode => // do nothing
      case o => throw new TableException(s"Error node: $o, Only SinkNode is supported.")
    }
    // convert LogicalNode tree to RelNode tree
    val relNodeTrees = sinkNodes.map(_.toRelNode(tEnv.getRelBuilder))
    // merge RelNode tree to RelNode dag
    val relNodeDag = reuseRelNodes(relNodeTrees)
    val builder = new RelNodeBlockPlanBuilder(tEnv)
    builder.buildRelNodeBlockPlan(relNodeDag)
  }

  /**
    * Reuse common subPlan in different RelNode tree, generate a RelNode dag
    *
    * @param relNodes RelNode trees
    * @return RelNode dag which reuse common subPlan in each tree
    */
  private def reuseRelNodes(relNodes: Seq[RelNode]): Seq[RelNode] = {

    class ExpandTableScanShuttle extends RelShuttleImpl {

      /**
        * Converts [[LogicalTableScan]] the result [[RelNode]] tree by calling [[RelTable]]#toRel
        */
      override def visit(scan: TableScan): RelNode = {

        scan match {
          case scan: LogicalTableScan =>
            val relTable = scan.getTable.unwrap(classOf[RelTable])
            if (relTable != null) {
              val relNode = relTable.toRel(RelOptUtil.getContext(scan.getCluster), scan.getTable)
              relNode.accept(this)
            } else {
              scan
            }
          case _ => scan
        }
      }
    }
    // expand RelTable in TableScan
    val shuttle = new ExpandTableScanShuttle
    val convertedRelNodes = relNodes.map(_.accept(shuttle))
    // reuse subPlan with same digest in input RelNode trees
    val context = new SubplanReuseContext(false, convertedRelNodes: _*)
    val reuseShuttle = new SubplanReuseShuttle(context)
    convertedRelNodes.map(_.accept(reuseShuttle))
  }

}

class RelNodeWrapper(relNode: RelNode) {
  private val parentNodes = Sets.newIdentityHashSet[RelNode]()

  def addParentNode(parent: Option[RelNode]): Unit = {
    parent match {
      case Some(p) => parentNodes.add(p)
      case None => // Ignore
    }
  }

  def hasMultipleParents: Boolean = parentNodes.size > 1
}
