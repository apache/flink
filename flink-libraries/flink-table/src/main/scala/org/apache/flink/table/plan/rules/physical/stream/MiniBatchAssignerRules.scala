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
package org.apache.flink.table.plan.rules.physical.stream

import java.util
import java.util.Collections
import org.apache.calcite.plan.RelOptRule.{any, none, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.{TableScan, Union}
import org.apache.calcite.rel.{BiRel, RelNode, SingleRel}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecMiniBatchAssigner, StreamExecTableSourceScan}
import org.apache.flink.table.plan.schema.IntermediateDataStreamTable

import scala.collection.JavaConversions._

/**
  * A MiniBatchAssignerRule is used to add a MiniBatchAssigner node to generate batch marker.
  */
object MiniBatchAssignerRules {

  val UNARY = new MiniBatchAssignerRuleForUnary
  val BINARY = new MiniBatchAssignerRuleForBinary
  val UNION = new MiniBatchAssignerRuleForUnion

  class MiniBatchAssignerRuleForUnary
    extends RelOptRule(
      operand(classOf[SingleRel], operand(classOf[TableScan], none())),
      "MiniBatchAssignerRuleForUnary") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val parent = call.rel[SingleRel](0)
      val scan = call.rel[TableScan](1)
      if (parent.isInstanceOf[StreamExecMiniBatchAssigner]) {
        return false
      }
      isScan(scan)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val parent = call.rel[SingleRel](0)
      val scan = call.rel[TableScan](1)
      val config = scan.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])
      val miniBatchNode = new StreamExecMiniBatchAssigner(
        scan.getCluster,
        scan.getTraitSet,
        scan,
        config.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY))
      val newParent = parent.copy(parent.getTraitSet, Collections.singletonList(miniBatchNode))
      call.transformTo(newParent)
    }
  }

  class MiniBatchAssignerRuleForBinary
    extends RelOptRule(
      operand(classOf[BiRel],
              operand(classOf[RelNode], any()),
              operand(classOf[RelNode], any())),
      "MiniBatchAssignerRuleForBinary") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val node1 = call.rel[RelNode](1)
      val node2 = call.rel[RelNode](2)
      isScan(node1) || isScan(node2)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val biRel = call.rel[BiRel](0)
      val node1 = call.rel[RelNode](1)
      val node2 = call.rel[RelNode](2)
      val config = biRel.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])
      val newNode1 = if (isScan(node1)) {
        new StreamExecMiniBatchAssigner(
          node1.getCluster,
          node1.getTraitSet,
          node1,
          config.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY))
      } else {
        node1
      }

      val newNode2 = if (isScan(node2)) {
        new StreamExecMiniBatchAssigner(
          node2.getCluster,
          node2.getTraitSet,
          node2,
          config.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY))
      } else {
        node2
      }

      val newBiRel = biRel.copy(biRel.getTraitSet, util.Arrays.asList(newNode1, newNode2))
      call.transformTo(newBiRel)
    }
  }

  /**
    * NOTE: MiniBatchAssignerRuleForUnion only support HepPlanner currently
    */
  class MiniBatchAssignerRuleForUnion
    extends RelOptRule(
      operand(classOf[Union], any()),
      "MiniBatchAssignerRuleForUnion") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val union = call.rel[Union](0)
      union.getInputs.exists {
        case vertex: HepRelVertex if isScan(vertex.getCurrentRel) => true
        case node: RelNode if isScan(node) => true
        case _ => false
      }
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val union = call.rel[Union](0)
      val config = union.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])

      val newNodes = union.getInputs.map {
        case vertex: HepRelVertex =>
          val curNode = vertex.getCurrentRel
          if (isScan(curNode)) {
            new StreamExecMiniBatchAssigner(
              curNode.getCluster,
              curNode.getTraitSet,
              curNode,
              config.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY))
          } else {
            curNode
          }
        case node => node
      }

      val newUnion = union.copy(union.getTraitSet, newNodes)
      call.transformTo(newUnion)
    }
  }

  private def isScan(node: RelNode): Boolean = node match {
    case scan: StreamExecDataStreamScan =>
      // scan is not an intermediate datastream
      !scan.dataStreamTable.isInstanceOf[IntermediateDataStreamTable[_]]
    case _: StreamExecTableSourceScan => true
    case _ => false
  }

}
