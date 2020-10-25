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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.plan.nodes.calcite.{LegacySink, Sink}
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, ExecNodeVisitorImpl}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel

import com.google.common.collect.{Maps, Sets}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Pair

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._

/**
  * An utility class for converting an exec node plan to a string as a tree style.
  *
  * The implementation is based on RelNode#explain now.
  */
object ExecNodePlanDumper {

  /**
    * Converts an [[ExecNode]] tree to a string as a tree style.
    *
    * @param node               the ExecNode to convert
    * @param detailLevel        detailLevel defines detail levels for EXPLAIN PLAN.
    * @param withExecNodeId     whether including ID of ExecNode
    * @param withChangelogTraits  whether including changelog traits of RelNode corresponding to
    *                           an ExecNode (only apply to StreamPhysicalRel node at present)
    * @param withOutputType     whether including output rowType
    * @return                   explain plan of ExecNode
    */
  def treeToString(
      node: ExecNode[_, _],
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withExecNodeId: Boolean = false,
      withChangelogTraits: Boolean = false,
      withOutputType: Boolean = false,
      withResource: Boolean = false): String = {
    doConvertTreeToString(
      node,
      detailLevel = detailLevel,
      withExecNodeId = withExecNodeId,
      withChangelogTraits = withChangelogTraits,
      withOutputType = withOutputType,
      withResource = withResource)
  }

  /**
    * Converts an [[ExecNode]] DAG to a string as a tree style.
    *
    * @param nodes              the ExecNodes to convert
    * @param detailLevel        detailLevel defines detail levels for EXPLAIN PLAN.
    * @param withExecNodeId     whether including ID of ExecNode
    * @param withChangelogTraits  whether including changelog traits of RelNode corresponding to
    *                           an ExecNode (only apply to StreamPhysicalRel node at present)
    * @param withOutputType     whether including output rowType
    * @return                   explain plan of ExecNode
    */
  def dagToString(
      nodes: Seq[ExecNode[_, _]],
      detailLevel: SqlExplainLevel = SqlExplainLevel.DIGEST_ATTRIBUTES,
      withExecNodeId: Boolean = false,
      withChangelogTraits: Boolean = false,
      withOutputType: Boolean = false,
      withResource: Boolean = false): String = {
    if (nodes.length == 1) {
      return treeToString(
        nodes.head,
        detailLevel,
        withExecNodeId = withExecNodeId,
        withChangelogTraits = withChangelogTraits,
        withOutputType = withOutputType,
        withResource = withResource)
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
    val visitor = new ExecNodeVisitorImpl {
      override def visit(node: ExecNode[_, _]): Unit = {
        val visitedTimes = mapNodeToVisitedTimes.getOrDefault(node, 0) + 1
        mapNodeToVisitedTimes.put(node, visitedTimes)
        if (visitedTimes == 1) {
          super.visit(node)
        }
        val reuseId = reuseInfoBuilder.getReuseId(node)
        val isReuseNode = reuseId.isDefined
        if (node.isInstanceOf[LegacySink] || node.isInstanceOf[Sink] ||
            (isReuseNode && !reuseInfoMap.containsKey(node))) {
          if (isReuseNode) {
            reuseInfoMap.put(node, (reuseId.get, true))
          }
          val reusePlan = doConvertTreeToString(
            node,
            detailLevel = detailLevel,
            withExecNodeId = withExecNodeId,
            withChangelogTraits = withChangelogTraits,
            withOutputType = withOutputType,
            stopExplainNodes = Some(stopExplainNodes),
            reuseInfoMap = Some(reuseInfoMap),
            withResource = withResource)
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
      withExecNodeId: Boolean = false,
      withChangelogTraits: Boolean = false,
      withOutputType: Boolean = false,
      stopExplainNodes: Option[util.Set[ExecNode[_, _]]] = None,
      reuseInfoMap: Option[util.IdentityHashMap[ExecNode[_, _], (Integer, Boolean)]] = None,
      withResource: Boolean = false
  ): String = {
    // TODO refactor this part of code
    //  get ExecNode explain value by RelNode#explain now
    val sw = new StringWriter
    val planWriter = new NodeTreeWriterImpl(
      node,
      new PrintWriter(sw),
      explainLevel = detailLevel,
      withExecNodeId = withExecNodeId,
      withChangelogTraits = withChangelogTraits,
      withOutputType = withOutputType,
      stopExplainNodes = stopExplainNodes,
      reuseInfoMap = reuseInfoMap,
      withResource = withResource)
    node.asInstanceOf[RelNode].explain(planWriter)
    sw.toString
  }
}

/**
  * build reuse id in an ExecNode DAG.
  */
class ReuseInfoBuilder extends ExecNodeVisitorImpl {
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

/**
  * Convert node tree to string as a tree style.
  */
class NodeTreeWriterImpl(
    node: ExecNode[_, _],
    pw: PrintWriter,
    explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
    withExecNodeId: Boolean = false,
    withChangelogTraits: Boolean = false,
    withOutputType: Boolean = false,
    stopExplainNodes: Option[util.Set[ExecNode[_, _]]] = None,
    reuseInfoMap: Option[util.IdentityHashMap[ExecNode[_, _], (Integer, Boolean)]] = None,
    withResource: Boolean = false)
  extends RelWriterImpl(pw, explainLevel, false) {

  require((stopExplainNodes.isEmpty && reuseInfoMap.isEmpty) ||
    (stopExplainNodes.isDefined && reuseInfoMap.isDefined))

  // use reuse info based on `reuseInfoMap` if it's not None,
  // else rebuild it using `ReuseInfoBuilder`
  class ReuseInfo {
    // mapping node object to visited times
    var mapNodeToVisitedTimes: util.Map[ExecNode[_, _], Int] = _
    var reuseInfoBuilder: ReuseInfoBuilder = _

    if (reuseInfoMap.isEmpty) {
      reuseInfoBuilder = new ReuseInfoBuilder()
      reuseInfoBuilder.visit(node)
      mapNodeToVisitedTimes = Maps.newIdentityHashMap[ExecNode[_, _], Int]()
    }

    /**
      * Returns reuse id if the given node is a reuse node, else None.
      */
    def getReuseId(node: ExecNode[_, _]): Option[Integer] = {
      reuseInfoMap match {
        case Some(map) => if (map.containsKey(node)) Some(map.get(node)._1) else None
        case _ => reuseInfoBuilder.getReuseId(node)
      }
    }

    /**
      * Returns true if the given node is first visited, else false.
      */
    def isFirstVisited(node: ExecNode[_, _]): Boolean = {
      reuseInfoMap match {
        case Some(map) => if (map.containsKey(node)) map.get(node)._2 else true
        case _ => mapNodeToVisitedTimes.get(node) == 1
      }
    }

    /**
      * Updates visited times for given node if `reuseInfoMap` is None.
      */
    def addVisitedTimes(node: ExecNode[_, _]): Unit = {
      reuseInfoMap match {
        case Some(_) => // do nothing
        case _ =>
          val visitedTimes = mapNodeToVisitedTimes.getOrDefault(node, 0) + 1
          mapNodeToVisitedTimes.put(node, visitedTimes)
      }
    }
  }

  val reuseInfo = new ReuseInfo

  var lastChildren: Seq[Boolean] = Nil

  var depth = 0

  override def explain_(rel: RelNode, values: JList[Pair[String, AnyRef]]): Unit = {
    val node = rel.asInstanceOf[ExecNode[_, _]]
    reuseInfo.addVisitedTimes(node)
    val inputs = rel.getInputs
    // whether explain input nodes of current node
    val explainInputs = needExplainInputs(node)
    val mq = rel.getCluster.getMetadataQuery
    if (explainInputs && !mq.isVisibleInExplain(rel, explainLevel)) {
      // render children in place of this, at same level
      inputs.toSeq.foreach(_.explain(this))
      return
    }

    val s = new StringBuilder
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        s.append(if (isLast) "   " else ":  ")
      }
      s.append(if (lastChildren.last) "+- " else ":- ")
    }

    val reuseId = reuseInfo.getReuseId(node)
    val isReuseNode = reuseId.isDefined
    val isFirstVisited = reuseInfo.isFirstVisited(node)
    // whether output detail
    val printDetail = !isReuseNode || isFirstVisited

    if (isReuseNode && !isFirstVisited) {
      s.append("Reused")
    } else {
      rel.getRelTypeName match {
        case name if name.startsWith("BatchExec") => s.append(name.substring(9))
        case name if name.startsWith("StreamExec") => s.append(name.substring(10))
        case name => s.append(name)
      }
    }

    val printValues = new JArrayList[Pair[String, AnyRef]]()
    if (printDetail) {
      if (explainLevel != SqlExplainLevel.NO_ATTRIBUTES) {
        printValues.addAll(values)
      }

      if (withExecNodeId) {
        // use RelNode's id now
        printValues.add(Pair.of("__id__", rel.getId.toString))
      }

      if (withChangelogTraits) {
        rel match {
          case streamRel: StreamPhysicalRel =>
            val changelogMode = ChangelogPlanUtils.getChangelogMode(streamRel)
            printValues.add(
              Pair.of("changelogMode", ChangelogPlanUtils.stringifyChangelogMode(changelogMode)))
          case _ => // ignore
        }
      }
    }

    if (isReuseNode) {
      if (isFirstVisited) {
        printValues.add(Pair.of("reuse_id", reuseId.get))
      } else {
        printValues.add(Pair.of("reference_id", reuseId.get))
      }
    }

    if (!printValues.isEmpty) {
      var j = 0
      printValues.toSeq.foreach {
        case value if value.right.isInstanceOf[RelNode] => // do nothing
        case value =>
          if (j == 0) s.append("(") else s.append(", ")
          j = j + 1
          s.append(value.left).append("=[").append(value.right).append("]")
      }
      if (j > 0) s.append(")")
    }

    if (withOutputType) {
      s.append(", rowType=[").append(rel.getRowType.toString).append("]")
    }

    if (explainLevel == SqlExplainLevel.ALL_ATTRIBUTES && printDetail) {
      s.append(": rowcount = ")
        .append(mq.getRowCount(rel))
        .append(", cumulative cost = ")
        .append(mq.getCumulativeCost(rel))
    }
    pw.println(s)
    if (explainInputs && inputs.length > 1 && printDetail) {
      inputs.toSeq.init.foreach { rel =>
        depth = depth + 1
        lastChildren = lastChildren :+ false
        rel.explain(this)
        depth = depth - 1
        lastChildren = lastChildren.init
      }
    }
    if (explainInputs && !inputs.isEmpty && printDetail) {
      depth = depth + 1
      lastChildren = lastChildren :+ true
      inputs.toSeq.last.explain(this)
      depth = depth - 1
      lastChildren = lastChildren.init
    }
  }

  /**
    * Returns true if `stopExplainNodes` is not None and contains the given node, else false.
    */
  private def needExplainInputs(node: ExecNode[_, _]): Boolean = {
    stopExplainNodes match {
      case Some(nodes) => !nodes.contains(node)
      case _ => true
    }
  }

}
