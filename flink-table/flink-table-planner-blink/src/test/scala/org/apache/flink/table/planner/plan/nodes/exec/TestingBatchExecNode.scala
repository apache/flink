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

package org.apache.flink.table.planner.plan.nodes.exec

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.calcite.{FlinkContextImpl, FlinkRelOptClusterFactory, FlinkRexBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.plan.{RelOptCluster, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.AbstractRelNode
import org.apache.calcite.rel.`type`.RelDataTypeSystem

import java.util

/**
 * [[LegacyBatchExecNode]] for testing purpose.
 */
class TestingBatchExecNode
    extends AbstractRelNode(TestingBatchExecNode.cluster, TestingBatchExecNode.traitSet)
    with BatchPhysicalRel
    with LegacyBatchExecNode[BatchPlanner]  {

  val inputNodes: util.List[ExecNode[_]] =
    new util.ArrayList[ExecNode[_]]()
  val inputEdges: util.List[ExecEdge] = new util.ArrayList[ExecEdge]()

  def addInput(node: ExecNode[_]): Unit =
    addInput(node, ExecEdge.builder().build())

  def addInput(node: ExecNode[_], edge: ExecEdge): Unit = {
    inputNodes.add(node)
    inputEdges.add(edge)
  }

  override def getOutputType: LogicalType = RowType.of()

  override def getInputNodes: util.List[ExecNode[_]] = inputNodes

  override def getInputEdges: util.List[ExecEdge] = inputEdges

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[_]): Unit =
    inputNodes.set(ordinalInParent, newInputNode)

  override def getTraitSet: RelTraitSet = TestingBatchExecNode.traitSet

  override protected def translateToPlanInternal(
    planner: BatchPlanner): Transformation[BatchPlanner] = ???
}

object TestingBatchExecNode {
  val typeSystem: RelDataTypeSystem = new FlinkTypeSystem
  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(typeSystem)
  val context: FlinkContextImpl = new FlinkContextImpl(new TableConfig, null, null, null)
  val planner: RelOptPlanner = new HepPlanner(HepProgram.builder().build(), context)
  val cluster: RelOptCluster =
    FlinkRelOptClusterFactory.create(planner, new FlinkRexBuilder(typeFactory))
  val traitSet: RelTraitSet = RelTraitSet.createEmpty()
}
