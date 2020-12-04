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

package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.utils.ExecNodePlanDumper

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{AbstractRelNode, RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
 * Base physical RelNode for multiple input rel which contains a sub-graph.
 * The root node of the sub-graph is [[outputRel]], and the leaf nodes of the sub-graph are
 * the output nodes of the [[inputRels]], which means the multiple input rel does not
 * contain [[inputRels]].
 *
 * TODO this is a temporary solution, this class should be removed once we split the
 *   implementation of physical rel and exec node.
 */
class MultipleInputRel(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    val outputRel: RelNode,
    val readOrders: Array[Int])
  extends AbstractRelNode(cluster, traitSet)
  with FlinkPhysicalRel {

  override def getInputs: util.List[RelNode] = inputRels.toList

  override def deriveRowType(): RelDataType = outputRel.getRowType

  override def explainTerms(pw: RelWriter): RelWriter = {
    inputRels.zipWithIndex.map {
      case (rel, index) =>
        pw.input("input" + index, rel)
    }
    val hasDiffReadOrder = readOrders.slice(1, readOrders.length).exists(readOrders.head != _)
    pw.itemIf("readOrder", readOrders.mkString(","), hasDiffReadOrder)
    pw.item("members", "\\n" + getExplainTermsOfMembers.replace("\n", "\\n"))
  }

  private def getExplainTermsOfMembers: String = {
    ExecNodePlanDumper.treeToString(
      outputRel.asInstanceOf[ExecNode[_]],
      inputRels.map(_.asInstanceOf[ExecNode[_]]).toList,
      true)
  }
}
