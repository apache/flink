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

import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.table.plan.`trait`.{AccModeTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.cost.FlinkBatchCost
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecHashJoinBase, BatchExecNestedLoopJoinBase, BatchExecScan, BatchPhysicalRel}
import org.apache.flink.table.plan.nodes.physical.stream.StreamPhysicalRel
import org.apache.flink.table.plan.util.FlinkNodeOptUtil.ReuseInfoBuilder

import com.google.common.collect.Maps
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Pair

import java.io.PrintWriter
import java.util
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._

/**
  * Convert node tree to string as a tree style.
  */
class NodeTreeWriterImpl(
    node: ExecNode[_, _],
    pw: PrintWriter,
    explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
    withResource: Boolean = false,
    withMemCost: Boolean = false,
    withRelNodeId: Boolean = false,
    withRetractTraits: Boolean = false,
    stopExplainNodes: Option[util.Set[ExecNode[_, _]]] = None,
    reuseInfoMap: Option[util.IdentityHashMap[ExecNode[_, _], (Integer, Boolean)]] = None)
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
        case name if name.endsWith("BatchExec") => s.append(name.substring(0, name.length - 9))
        case name if name.startsWith("BatchExec") => s.append(name.substring(9))
        case name => s.append(name)
      }
    }

    val printValues = new JArrayList[Pair[String, AnyRef]]()
    if (printDetail) {
      if (withResource) {
        node match {
          case batchExecNode: BatchExecNode[_] =>
            batchExecNode match {
              case scan: BatchExecScan =>
                // TODO refactor
                printValues.add(Pair.of(
                  "sourceRes",
                  resourceSpecToString(batchExecNode.asInstanceOf[BatchExecScan].sourceResSpec)))
                if (scan.needInternalConversion) {
                  printValues.add(Pair.of(
                    "conversionRes",
                    resourceSpecToString(
                      batchExecNode.asInstanceOf[BatchExecScan].conversionResSpec)))
                }
              case hashJoin: BatchExecHashJoinBase => printValues.add(
                Pair.of("shuffleCount", hashJoin.shuffleBuildCount(mq).toString))
              case nestedLoopJoin: BatchExecNestedLoopJoinBase => printValues.add(
                Pair.of("shuffleCount", nestedLoopJoin.shuffleBuildCount(mq).toString))
              case _ => // do nothing
            }
            addResourceToPrint(batchExecNode, printValues)
        }
      }
      if (withMemCost || withResource) {
        rel match {
          case batchRel: BatchPhysicalRel =>
            val memCost = mq.getNonCumulativeCost(batchRel).asInstanceOf[FlinkBatchCost].memory
            printValues.add(Pair.of("memCost", memCost.asInstanceOf[AnyRef]))
            printValues.add(Pair.of("rowcount", mq.getRowCount(rel)))
        }
      }
      if (explainLevel != SqlExplainLevel.NO_ATTRIBUTES) {
        printValues.addAll(values)
      }

      if (withRelNodeId) {
        printValues.add(Pair.of("__id__", rel.getId.toString))
      }

      if (withRetractTraits) {
        rel match {
          case streamRel: StreamPhysicalRel =>
            val traitSet = streamRel.getTraitSet
            printValues.add(
              Pair.of("retract", traitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)))
            printValues.add(
              Pair.of("accMode", traitSet.getTrait(AccModeTraitDef.INSTANCE)))
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

  private def addResourceToPrint(
      batchExecNode: BatchExecNode[_],
      printValues: JArrayList[Pair[String, AnyRef]]): Unit = {
      printValues.add(Pair.of("resource", batchExecNode.getResource.toString))
  }

  private def resourceSpecToString(resourceSpec: ResourceSpec): String = {
    val s = "ResourceSpec: {cpu=" + resourceSpec.getCpuCores + ", heap=" +
        resourceSpec.getHeapMemory + ", direct=" + resourceSpec.getDirectMemory
    try {
      if (!resourceSpec.getExtendedResources.containsKey(ResourceSpec.MANAGED_MEMORY_NAME) ||
        resourceSpec.getExtendedResources
          .get(ResourceSpec.MANAGED_MEMORY_NAME).getValue.toInt == 0) {
        s + "}"
      } else {
        s + ", managed=" +
          resourceSpec.getExtendedResources
            .get(ResourceSpec.MANAGED_MEMORY_NAME).getValue.toInt + "}"
      }
    } catch {
      case _: Exception => null
    }
  }

}

