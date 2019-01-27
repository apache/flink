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
package org.apache.flink.table.plan.util

import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.exec.{ExecNode, ExecNodeVisitor}

import com.google.common.collect.{Maps, Sets}
import org.apache.calcite.sql.SqlExplainLevel

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.concurrent.atomic.AtomicInteger

object FlinkNodeOptUtil {

  /**
    * Converts an [[ExecNode]] tree to a string as a tree style.
    *
    * @param node               the ExecNode to convert
    * @param detailLevel        detailLevel defines detail levels for EXPLAIN PLAN.
    * @param withResource       whether including resource information of ExecNode (only apply to
    *                           BatchExecNode node at present)
    * @param withMemCost        whether including memory cost information of ExecNode (only apply to
    *                           BatchExecNode node at present)
    * @param withRelNodeId      whether including ID of the RelNode corresponding to an ExecNode
    * @param withRetractTraits  whether including Retraction Traits of RelNode corresponding to
    *                           an ExecNode (only apply to StreamPhysicalRel node at present)
    * @return                   explain plan of ExecNode
    */
  def treeToString(
      node: ExecNode[_, _],
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withResource: Boolean = false,
      withMemCost: Boolean = false, // TODO remove this arg ???
      withRelNodeId: Boolean = false,
      withRetractTraits: Boolean = false): String = {
    doConvertTreeToString(
      node,
      detailLevel = detailLevel,
      withResource = withResource,
      withMemCost = withMemCost,
      withRelNodeId = withRelNodeId,
      withRetractTraits = withRetractTraits)
  }

  /**
    * Converts an [[ExecNode]] DAG to a string as a tree style.
    *
    * @param nodes              the ExecNodes to convert
    * @param detailLevel        detailLevel defines detail levels for EXPLAIN PLAN.
    * @param withResource       whether including resource information of ExecNode (only apply to
    *                           BatchExecNode node at present)
    * @param withMemCost        whether including memory cost information of ExecNode (only apply to
    *                           BatchExecNode node at present)
    * @param withRelNodeId      whether including ID of the RelNode corresponding to an ExecNode
    * @param withRetractTraits  whether including Retraction Traits of RelNode corresponding to
    *                           an ExecNode (only apply to StreamPhyscialRel node at present)
    * @return                   explain plan of ExecNode
    */
  def dagToString(
      nodes: Seq[ExecNode[_, _]],
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withResource: Boolean = false,
      withMemCost: Boolean = false, // TODO remove this arg ???
      withRelNodeId: Boolean = false,
      withRetractTraits: Boolean = false): String = {
    if (nodes.length == 1) {
      return treeToString(
        nodes.head,
        detailLevel,
        withResource = withResource,
        withMemCost = withMemCost,
        withRelNodeId = withRelNodeId,
        withRetractTraits = withRetractTraits)
    }

    val reuseInfoBuilder = new ReuseInfoBuilder()
    nodes.foreach(reuseInfoBuilder.visit)
    // node sets that stop explain when meet them
    val stopExplainNodes = Sets.newIdentityHashSet[ExecNode[_, _]]()
    // mapping node to reuse info, the map value is a tuple2,
    // the first value of the tuple is reuse id,
    // the second value is true if the node is first visited else false.
    val reuseInfoMap = Maps.newIdentityHashMap[ExecNode[_, _], (Integer, Boolean)]()
    // mapping node object to visited times
    val mapNodeToVisitedTimes = Maps.newIdentityHashMap[ExecNode[_, _], Int]()
    val sb = new StringBuilder()
    val visitor = new ExecNodeVisitor {
      override def visit(node: ExecNode[_, _]): Unit = {
        val visitedTimes = mapNodeToVisitedTimes.getOrDefault(node, 0) + 1
        mapNodeToVisitedTimes.put(node, visitedTimes)
        if (visitedTimes == 1) {
          super.visit(node)
        }
        val reuseId = reuseInfoBuilder.getReuseId(node)
        val isReuseNode = reuseId.isDefined
        if (node.isInstanceOf[Sink] || (isReuseNode && !reuseInfoMap.containsKey(node))) {
          if (isReuseNode) {
            reuseInfoMap.put(node, (reuseId.get, true))
          }
          val reusePlan = doConvertTreeToString(
            node,
            detailLevel = detailLevel,
            withResource = withResource,
            withMemCost = withMemCost,
            withRelNodeId = withRelNodeId,
            withRetractTraits = withRetractTraits,
            stopExplainNodes = Some(stopExplainNodes),
            reuseInfoMap = Some(reuseInfoMap))
          sb.append(reusePlan).append(System.lineSeparator)
          if (isReuseNode) {
            // update visit info after the reuse node visited
            stopExplainNodes.add(node)
            reuseInfoMap.put(node, (reuseId.get, false))
          }
        }
      }
    }

    nodes.foreach(visitor.visit)

    if (sb.length() > 0) {
      // delete last line separator
      sb.deleteCharAt(sb.length - 1)
    }
    sb.toString()
  }

  private def doConvertTreeToString(
      node: ExecNode[_, _],
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withResource: Boolean = false,
      withMemCost: Boolean = false,
      withRelNodeId: Boolean = false,
      withRetractTraits: Boolean = false,
      stopExplainNodes: Option[util.Set[ExecNode[_, _]]] = None,
      reuseInfoMap: Option[util.IdentityHashMap[ExecNode[_, _], (Integer, Boolean)]] = None
  ): String = {
    // get ExecNode explain value by RelNode#explain now
    val sw = new StringWriter
    val planWriter = new NodeTreeWriterImpl(
      node,
      new PrintWriter(sw),
      explainLevel = detailLevel,
      withResource = withResource,
      withMemCost = withMemCost,
      withRelNodeId = withRelNodeId,
      withRetractTraits = withRetractTraits,
      stopExplainNodes = stopExplainNodes,
      reuseInfoMap = reuseInfoMap)
    node.getFlinkPhysicalRel.explain(planWriter)
    sw.toString
  }

  /**
    * build reuse id in an ExecNode DAG.
    */
  class ReuseInfoBuilder extends ExecNodeVisitor {
    // visited node set
    private val visitedNodes = Sets.newIdentityHashSet[ExecNode[_, _]]()
    // mapping reuse node to its reuse id
    private val mapReuseNodeToReuseId = Maps.newIdentityHashMap[ExecNode[_, _], Integer]()
    private val reuseIdGenerator = new AtomicInteger(0)

    override def visit(node: ExecNode[_, _]): Unit = {
      // if a node is visited more than once, this node is a reusable node
      if (visitedNodes.contains(node)) {
        if (!mapReuseNodeToReuseId.containsKey(node)) {
          val reuseId = reuseIdGenerator.incrementAndGet()
          mapReuseNodeToReuseId.put(node, reuseId)
        }
      } else {
        visitedNodes.add(node)
        super.visit(node)
      }
    }

    /**
      * Returns reuse id if the given node is a reuse node (that means it has multiple outputs),
      * else None.
      */
    def getReuseId(node: ExecNode[_, _]): Option[Integer] = {
      if (mapReuseNodeToReuseId.containsKey(node)) {
        Some(mapReuseNodeToReuseId.get(node))
      } else {
        None
      }
    }
  }

}
