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

package org.apache.flink.table.planner.plan.reuse

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.planner.plan.nodes.calcite.{LegacySink, Sink}
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalLegacyTableSourceScan, FlinkLogicalTableSourceScan}
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.PhysicalLegacyTableSourceScan
import org.apache.flink.table.planner.plan.utils.{DefaultRelShuttle, FlinkRelOptUtil}

import com.google.common.collect.{Maps, Sets}
import org.apache.calcite.rel.core.{Exchange, TableFunctionScan}
import org.apache.calcite.rel.{RelNode, RelVisitor}

import java.util

import scala.collection.JavaConversions._

/**
  * An utility class for finding out duplicated sub-plans by digest and reusing them.
  *
  * <p>e.g.
  * if Project1-Scan1 and Project2-Scan2 have same digest, they could be reused,
  * and only one of duplicated sub-plan will be retained.
  * after
  * {{{
  *       Join                      Join
  *     /      \                  /      \
  * Filter1  Filter2          Filter1  Filter2
  *    |        |        =>       \     /
  * Project1 Project2            Project1
  *    |        |                   |
  *  Scan1    Scan2               Scan1
  * }}}
  */
object SubplanReuser {

  /**
    * Finds duplicated sub-plans and return the reused plan.
    */
  def reuseDuplicatedSubplan(rels: Seq[RelNode], tableConfig: TableConfig): Seq[RelNode] = {
    if (!tableConfig.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED)) {
      return rels
    }
    val tableSourceReuseEnabled = tableConfig.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED)
    val context = new SubplanReuseContext(tableSourceReuseEnabled, rels: _*)
    val reuseShuttle = new SubplanReuseShuttle(context)
    rels.map(_.accept(reuseShuttle))
  }

  /**
    * The Context holds sub-plan reuse information.
    *
    * <p>If two sub-plan ([[RelNode]] tree with leaf node) , even belongs to different tree, have
    * same digest, they are in a reusable sub-plan group.
    * In a reusable sub-plan group, the leftmost sub-plan in the earlier visited tree is reused
    * sub-plan and the remaining will reuse the leftmost one in the earlier visited tree.
    * <p>Uses reuse id to distinguish different reusable sub-plan group,
    * the reuse id of each sub-plan is same in a group.
    *
    * <p>e.g.
    * {{{
    *       Join
    *     /      \
    * Filter1  Filter2
    *    |        |
    * Project1 Project2
    *    |        |
    *  Scan1    Scan2
    * }}}
    * Project1-Scan1 and Project2-Scan2 have same digest, so they are in a reusable sub-plan group.
    */
  class SubplanReuseContext(tableSourceReuseEnabled: Boolean, roots: RelNode*) {
    // mapping a relNode to its digest
    private val mapRelToDigest = Maps.newIdentityHashMap[RelNode, String]()
    // mapping the digest to RelNodes
    private val mapDigestToReusableNodes = new util.HashMap[String, util.List[RelNode]]()

    val visitor = new ReusableSubplanVisitor()
    roots.map(visitor.go)

    /**
      * Return the digest of the given rel node.
      */
    def getRelDigest(node: RelNode): String = {
      val digest = mapRelToDigest.get(node)
      if (digest != null) {
        digest
      } else {
        val newDigest = FlinkRelOptUtil.getDigest(node)
        mapRelToDigest.put(node, newDigest)
        newDigest
      }
    }

    /**
      * Returns true if the given node can reuse other node, else false.
      * The nodes with same digest are reusable,
      * and the non-head node of node-list can reuse the head node.
      */
    def reuseOtherNode(node: RelNode): Boolean = {
      val reusableNodes = getReusableNodes(node)
      if (isReusableNodes(reusableNodes)) {
        node ne reusableNodes.head
      } else {
        false
      }
    }

    /**
      * Returns reusable nodes which have same digest.
      */
    private def getReusableNodes(node: RelNode): List[RelNode] = {
      val digest = getRelDigest(node)
      mapDigestToReusableNodes.getOrDefault(digest, List.empty[RelNode]).toList
    }

    /**
      * Returns true if the given nodes can be reused, else false.
      */
    private def isReusableNodes(reusableNodes: List[RelNode]): Boolean = {
      if (reusableNodes.size() > 1) {
        // Does not reuse nodes which are reusable disabled
        !isNodeReusableDisabled(reusableNodes.head)
      } else {
        false
      }
    }

    /**
      * Returns true if the given node is reusable disabled
      */
    private def isNodeReusableDisabled(node: RelNode): Boolean = {
      node match {
        // TableSourceScan node can not be reused if reuse TableSource disabled
        case _: FlinkLogicalLegacyTableSourceScan | _: PhysicalLegacyTableSourceScan |
             _: FlinkLogicalTableSourceScan | _: CommonPhysicalTableSourceScan =>
          !tableSourceReuseEnabled
        // Exchange node can not be reused if its input is reusable disabled
        case e: Exchange => isNodeReusableDisabled(e.getInput)
        // TableFunctionScan and sink can not be reused
        case _: TableFunctionScan | _: LegacySink | _: Sink => true
        case _ => false
      }
    }

    class ReusableSubplanVisitor extends RelVisitor {
      private val visitedNodes = Sets.newIdentityHashSet[RelNode]()

      override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
        if (visitedNodes.contains(node)) {
          // does not need to visit a node which is already visited.
          return
        }
        visitedNodes.add(node)

        // the same sub-plan should have same digest value,
        // uses `explain` with `RelDigestWriterImpl` to get the digest of a sub-plan.
        val digest = getRelDigest(node)
        val nodes = mapDigestToReusableNodes.getOrElseUpdate(digest, new util.ArrayList[RelNode]())
        nodes.add(node)
        // the node will be reused if there are more than one nodes with same digest,
        // so there is no need to visit a reused node's inputs.
        if (!isReusableNodes(nodes.toList)) {
          super.visit(node, ordinal, parent)
        }
      }
    }

  }

  /**
    * Rewrite reusable sub-plans with different rel objects to same rel object.
    *
    * <p>e.g.
    * Scan1-Project1 and Scan2-Project2 have same digest, so they can be reused.
    * {{{
    *      Join                       Join
    *     /    \                     /    \
    * Filter1 Filter2            Filter1 Filter2
    *    |      |          =>        \      /
    * Project1 Project2              Project1
    *    |      |                       |
    *  Scan1   Scan2                  Scan1
    * }}}
    * After rewrote, Scan2-Project2 is replaced by Scan1-Project1.
    *
    * <p>NOTES: This is a stateful class, please use same shuttle object to reuse multiple trees.
    */
  class SubplanReuseShuttle(context: SubplanReuseContext) extends DefaultRelShuttle {
    private val mapDigestToNewNode = new util.HashMap[String, RelNode]()

    override def visit(rel: RelNode): RelNode = {
      val canReuseOtherNode = context.reuseOtherNode(rel)
      val digest = context.getRelDigest(rel)
      if (canReuseOtherNode) {
        val newNode = mapDigestToNewNode.get(digest)
        if (newNode == null) {
          throw new TableException("This should not happen")
        }
        newNode
      } else {
        val newNode = visitInputs(rel)
        mapDigestToNewNode.put(digest, newNode)
        newNode
      }
    }

    /**
      * Parent rel need not change, just replaces inputs if any inputs change.
      */
    private def visitInputs(rel: RelNode): RelNode = {
      rel.getInputs.zipWithIndex.foreach {
        case (input, index) =>
          val newInput = input.accept(this)
          if (input ne newInput) {
            rel.replaceInput(index, newInput)
          }
      }
      rel
    }
  }

}
