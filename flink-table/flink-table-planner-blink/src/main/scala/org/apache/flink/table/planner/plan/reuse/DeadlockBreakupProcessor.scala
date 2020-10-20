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

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.transformations.ShuffleMode
import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecEdge, ExecNode, ExecNodeVisitorImpl}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.nodes.process.{DAGProcessContext, DAGProcessor}

import com.google.common.collect.{Maps, Sets}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.util.ImmutableIntList

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A DeadlockBreakupProcessor that finds out all deadlocks in the DAG, and resolves them.
  *
  * NOTES: This processor can be only applied on [[BatchExecNode]] DAG.
  *
  * Reused node (may be a [[BatchExecNode]] which has more than one outputs or
  * a [[BatchExecBoundedStreamScan]] which transformation is used for different scan)
  * might lead to a deadlock when a HashJoin or NestedLoopJoin have same reused inputs.
  * Sets Exchange node(if it does not exist, add new one) as BATCH mode to break up the deadlock.
  *
  * e.g. SQL: WITH r AS (SELECT a, b FROM x limit 10)
  * SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a AND r1.b > 10 AND r2.b < 20
  * the physical plan is: (sub-plan reused is enabled)
  * {{{
  * Calc(select=[a, b, b0])
  * +- HashJoin(where=[=(a, a0)], join=[a, b, a0, b0], joinType=[InnerJoin], isBroadcast=[true],
  *      build=[left])
  * :- Calc(select=[a, b], where=[>(b, 10)])
  * :  +- Limit(offset=[0], limit=[10], global=[true], reuse_id=[1])
  * :     +- Exchange(distribution=[single])
  * :        +- Limit(offset=[0], limit=[10], global=[false])
  * :           +- ScanTableSource(table=[[builtin, default, x,
  *                 source: [selectedFields=[a, b]]]], fields=[a, b])
  * +- Exchange(distribution=[broadcast])
  *    +- Calc(select=[a, b], where=[<(b, 20)])
  *       +- Reused(reference_id=[1])
  * }}}
  * the HashJoin's left input is probe side which could start to read data only after
  * build side has finished, so the Exchange node in HashJoin's left input requires BATCH mode
  * to block the stream. After this handler is applied, The simplified plan is:
  * {{{
  *                   Calc(select=[a])
  *                     |
  *                 HashJoin
  *     (build side)/      \(probe side)
  *    (broadcast)Exchange Exchange(shuffle_mode=[BATCH]) add BATCH Exchange to breakup deadlock
  *                |        |
  *             Calc(b>10) Calc(b<20)
  *                 \      /
  *                 Limit(global=[true], reuse_id=[1]))
  *                     |
  *                 Exchange(single)
  *                     |
  *                 Limit(global=[false])
  *                     |
  *                ScanTableSource
  * }}}
  */
class DeadlockBreakupProcessor extends DAGProcessor {

  def process(rootNodes: util.List[ExecNode[_, _]],
      context: DAGProcessContext): util.List[ExecNode[_, _]] = {
    if (!rootNodes.forall(_.isInstanceOf[BatchExecNode[_]])) {
      throw new TableException("Only BatchExecNode DAG is supported now")
    }
    val finder = new ReuseNodeFinder()
    rootNodes.foreach(finder.visit)
    rootNodes.foreach(_.asInstanceOf[BatchExecNode[_]].accept(new DeadlockBreakupVisitor(finder)))
    rootNodes
  }

  /**
    * Find reuse node.
    * A reuse node has more than one output or is a [[BatchExecBoundedStreamScan]]
    * which [[DataStream]] object is held by different [[BatchExecBoundedStreamScan]]s.
    */
  class ReuseNodeFinder extends ExecNodeVisitorImpl {
    // map a node object to its visited times.
    // the visited times of a reused node is greater than one
    private val visitedTimes = Maps.newIdentityHashMap[ExecNode[_, _], Integer]()
    // different BatchExecBoundedStreamScans may have same DataStream object
    // map DataStream object to BatchExecBoundedStreamScans
    private val mapDataStreamToScan =
        Maps.newIdentityHashMap[DataStream[_], util.List[BatchExecBoundedStreamScan]]()

    /**
      * Return true if the visited time of the given node is greater than one,
      * or the node is a [[BatchExecBoundedStreamScan]] and its [[DataStream]] object is held
      * by different [[BatchExecBoundedStreamScan]]s. else false.
      */
    def isReusedNode(node: ExecNode[_, _]): Boolean = {
      if (visitedTimes.getOrDefault(node, 0) > 1) {
        true
      } else {
        node match {
          case scan: BatchExecBoundedStreamScan =>
            val dataStream = scan.boundedStreamTable.dataStream
            val scans = mapDataStreamToScan.get(dataStream)
            scans != null && scans.size() > 1
          case _ => false
        }
      }
    }

    override def visit(node: ExecNode[_, _]): Unit = {
      val times = visitedTimes.getOrDefault(node, 0)
      visitedTimes.put(node, times + 1)
      node match {
        case scan: BatchExecBoundedStreamScan =>
          val dataStream = scan.boundedStreamTable.dataStream
          val scans = mapDataStreamToScan.getOrElseUpdate(
            dataStream, new util.ArrayList[BatchExecBoundedStreamScan]())
          scans.add(scan)
        case _ => // do nothing
      }
      super.visit(node)
    }
  }

  class DeadlockBreakupVisitor(finder: ReuseNodeFinder) extends ExecNodeVisitorImpl {

    private def rewriteTwoInputNode(
        node: ExecNode[_, _],
        leftPriority: Int,
        rightPriority: Int,
        requiredShuffle: ExecEdge.RequiredShuffle): Unit = {
      val (higherIndex, lowerIndex) = if (leftPriority < rightPriority) (0, 1) else (1, 0)
      val higherNode = node.getInputNodes.get(higherIndex)
      val lowerNode = node.getInputNodes.get(lowerIndex)

      // 1. find all reused nodes in higher input
      val reusedNodesInHigherInput = findReusedNodesInHigherInput(higherNode, finder)
      // 2. find all nodes from lower input
      val inputPathsOfLowerInput = buildInputPathsOfLowerInput(
        lowerNode, reusedNodesInHigherInput, finder)
      // 3. check whether all input paths have a barrier node (e.g. agg, sort)
      if (inputPathsOfLowerInput.nonEmpty && !hasBarrierNodeInInputPaths(inputPathsOfLowerInput)) {
        // 4. sets Exchange node(if does not exist, add one) as BATCH mode to break up the deadlock
        val distribution = requiredShuffle.getType match {
          case ExecEdge.ShuffleType.HASH =>
            FlinkRelDistribution.hash(ImmutableIntList.of(requiredShuffle.getKeys: _*))
          case ExecEdge.ShuffleType.BROADCAST =>
            throw new IllegalStateException(
              "Trying to add an exchange node on broadcast side. This is unexpected.")
          case ExecEdge.ShuffleType.SINGLETON =>
            FlinkRelDistribution.SINGLETON
          case _ =>
            FlinkRelDistribution.ANY
        }
        lowerNode match {
          case e: BatchExecExchange =>
            // TODO create a cloned BatchExecExchange for PIPELINE output
            e.setRequiredShuffleMode(ShuffleMode.BATCH)
          case _ =>
            val lowerRel = lowerNode.asInstanceOf[RelNode]
            val traitSet = lowerRel.getTraitSet.replace(distribution)
            val e = new BatchExecExchange(
              lowerRel.getCluster,
              traitSet,
              lowerRel,
              distribution)
            e.setRequiredShuffleMode(ShuffleMode.BATCH)
            // replace node's input
            node.asInstanceOf[BatchExecNode[_]].replaceInputNode(lowerIndex, e)
        }
      }
    }

    override def visit(node: ExecNode[_, _]): Unit = {
      super.visit(node)
      val inputEdges = node.getInputEdges
      if (inputEdges.size() == 2) {
        val leftPriority = inputEdges.get(0).getPriority
        val rightPriority = inputEdges.get(1).getPriority
        val requiredShuffle = if (leftPriority > rightPriority) {
          inputEdges.get(0).getRequiredShuffle
        } else {
          inputEdges.get(1).getRequiredShuffle
        }
        if (leftPriority != rightPriority) {
          rewriteTwoInputNode(node, leftPriority, rightPriority, requiredShuffle)
        }
      }
    }
  }

  /**
    * Find all reused nodes in higher input.
    */
  private def findReusedNodesInHigherInput(
      higherNode: ExecNode[_, _],
      finder: ReuseNodeFinder): Set[ExecNode[_, _]] = {
    val nodesInHigherInput = Sets.newIdentityHashSet[ExecNode[_, _]]()
    higherNode.accept(new ExecNodeVisitorImpl {
      override def visit(node: ExecNode[_, _]): Unit = {
        if (finder.isReusedNode(node)) {
          nodesInHigherInput.add(node)
        }
        super.visit(node)
      }
    })
    nodesInHigherInput.toSet
  }

  /**
    * Visit all nodes in lower input until to the reused nodes
    * which are in `reusedNodesInHigherInput` collection.
    * e.g. (sub-plan reused is enabled)
    * {{{
    *            hash join
    * (build side) /   \ (probe side)
    *           calc2 agg2
    *             |    |
    *           agg1  reused
    *              \   /
    *             calc1 (reuse_id=1)
    *                |
    *           table source
    * }}}
    * the input-path of join's probe side is [agg2, reused, calc1].
    *
    * e.g. (sub-plan reused is disabled)
    * {{{
    *            hash join
    * (build side) /   \ (probe side)
    *           calc1   calc2
    *              \   /
    *             scan table
    * }}}
    * the input-path of join's probe side is [calc2, scan].
    */
  private def buildInputPathsOfLowerInput(
      lowerNode: ExecNode[_, _],
      reusedNodesInHigherInput: Set[ExecNode[_, _]],
      finder: ReuseNodeFinder): List[Array[ExecNode[_, _]]] = {
    val result = new mutable.ListBuffer[Array[ExecNode[_, _]]]()
    val stack = new mutable.Stack[ExecNode[_, _]]()

    if (reusedNodesInHigherInput.isEmpty) {
      return result.toList
    }

    lowerNode.accept(new ExecNodeVisitorImpl {
      override def visit(node: ExecNode[_, _]): Unit = {
        stack.push(node)
        if (finder.isReusedNode(node) &&
          isReusedNodeInHigherInput(node, reusedNodesInHigherInput)) {
          result.add(stack.toArray.reverse)
        } else {
          super.visit(node)
        }
        stack.pop()
      }
    })

    require(stack.isEmpty)
    result.toList
  }

  /**
    * Returns true if the given node is in `reusedNodesInHigherInput`, else false.
    * NOTES: We treat different [[BatchExecBoundedStreamScan]]s with same [[DataStream]]
    * object as the same.
    */
  private def isReusedNodeInHigherInput(
      execNode: ExecNode[_, _],
      reusedNodesInHigherInput: Set[ExecNode[_, _]]): Boolean = {
    if (reusedNodesInHigherInput.contains(execNode)) {
      true
    } else {
      execNode match {
        case scan: BatchExecBoundedStreamScan =>
          reusedNodesInHigherInput.exists {
            case reusedScan: BatchExecBoundedStreamScan =>
              reusedScan.boundedStreamTable.dataStream eq scan.boundedStreamTable.dataStream
            case _ => false
          }
        case _ => false
      }
    }
  }

  /**
    * Returns true if all input-paths have barrier node (e.g. agg, sort), otherwise false.
    */
  private def hasBarrierNodeInInputPaths(
      inputPathsOfLowerInput: List[Array[ExecNode[_, _]]]): Boolean = {
    require(inputPathsOfLowerInput.nonEmpty)

    /** Return true if the successor in the input-path is also in higher input, otherwise false */
    def checkHigherInput(
        higherNode: ExecNode[_, _],
        idx: Int,
        inputPath: Array[ExecNode[_, _]]): Boolean = {
      if (idx < inputPath.length - 1) {
        val nextNode = inputPath(idx + 1)
        // next node is higher input
        higherNode eq nextNode
      } else {
        false
      }
    }

    inputPathsOfLowerInput.forall {
      inputPath =>
        var idx = 0
        var hasFullDamNode = false
        // should exclude the reused node (at last position in path)
        while (!hasFullDamNode && idx < inputPath.length - 1) {
          val node = inputPath(idx)
          val atLeastEndInput = node.getInputEdges.forall(
            e => e.getDamBehavior.stricterOrEqual(ExecEdge.DamBehavior.END_INPUT))
          hasFullDamNode = if (atLeastEndInput) {
            true
          } else {
            val inputEdges = node.getInputEdges
            if (inputEdges.size() == 2) {
              val leftPriority = inputEdges.get(0).getPriority
              val rightPriority = inputEdges.get(1).getPriority
              if (leftPriority != rightPriority) {
                val higherIndex = if (leftPriority < rightPriority) 0 else 1
                val higherNode = node.getInputNodes.get(higherIndex)
                checkHigherInput(higherNode, idx, inputPath)
              } else {
                false
              }
            } else {
              false
            }
          }
          idx += 1
        }
        hasFullDamNode
    }
  }

}
