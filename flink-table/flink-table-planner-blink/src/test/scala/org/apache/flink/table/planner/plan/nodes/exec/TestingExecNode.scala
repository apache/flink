/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptCost, RelOptPlanner, RelOptQuery, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.{RelCollation, RelNode, RelShuttle, RelVisitor, RelWriter}
import org.apache.calcite.rel.metadata.{Metadata, RelMetadataQuery}
import org.apache.calcite.rex.{RexNode, RexShuttle}
import org.apache.calcite.util.{ImmutableBitSet, Litmus}

import java.util

/**
 * [[BatchExecNode]] for testing purpose.
 */
class TestingExecNode extends BatchExecNode[BatchPlanner] with BatchPhysicalRel {

  val inputNodes: util.List[ExecNode[BatchPlanner, _]] = new util.ArrayList[ExecNode[BatchPlanner, _]]()
  val inputEdges: util.List[ExecEdge] = new util.ArrayList[ExecEdge]()

  def addInput(node: ExecNode[BatchPlanner, _]): Unit =
    addInput(node, ExecEdge.builder().build())

  def addInput(node: ExecNode[BatchPlanner, _], edge: ExecEdge): Unit = {
    inputNodes.add(node)
    inputEdges.add(edge)
  }

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] = inputNodes

  override def getInputEdges: util.List[ExecEdge] = inputEdges

  override def replaceInputNode(ordinalInParent: Int, newInputNode: ExecNode[BatchPlanner, _]): Unit =
    inputNodes.set(ordinalInParent, newInputNode)

  override def getCluster: RelOptCluster = TestingExecNode.cluster

  override def getTraitSet: RelTraitSet = TestingExecNode.traitSet

  override protected def translateToPlanInternal(planner: BatchPlanner): Transformation[BatchPlanner] = ???

  override def getChildExps: util.List[RexNode] = ???

  override def getConvention: Convention = ???

  override def getCorrelVariable: String = ???

  override def isDistinct: Boolean = ???

  override def getInput(i: Int): RelNode = ???

  override def getQuery: RelOptQuery = ???

  override def getRowType: RelDataType = ???

  override def getExpectedInputRowType(i: Int): RelDataType = ???

  override def getInputs: util.List[RelNode] = ???

  override def estimateRowCount(relMetadataQuery: RelMetadataQuery): Double = ???

  override def getRows: Double = ???

  override def getVariablesStopped: util.Set[String] = ???

  override def getVariablesSet: util.Set[CorrelationId] = ???

  override def collectVariablesUsed(set: util.Set[CorrelationId]): Unit = ???

  override def collectVariablesSet(set: util.Set[CorrelationId]): Unit = ???

  override def childrenAccept(relVisitor: RelVisitor): Unit = ???

  override def computeSelfCost(relOptPlanner: RelOptPlanner, relMetadataQuery: RelMetadataQuery): RelOptCost = ???

  override def computeSelfCost(relOptPlanner: RelOptPlanner): RelOptCost = ???

  override def metadata[M <: Metadata](aClass: Class[M], relMetadataQuery: RelMetadataQuery): M = ???

  override def explain(relWriter: RelWriter): Unit = ???

  override def onRegister(relOptPlanner: RelOptPlanner): RelNode = ???

  override def recomputeDigest(): String = ???

  override def replaceInput(i: Int, relNode: RelNode): Unit = ???

  override def getTable: RelOptTable = ???

  override def getRelTypeName: String = ???

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = ???

  override def isValid(b: Boolean): Boolean = ???

  override def getCollationList: util.List[RelCollation] = ???

  override def copy(relTraitSet: RelTraitSet, list: util.List[RelNode]): RelNode = ???

  override def register(relOptPlanner: RelOptPlanner): Unit = ???

  override def isKey(immutableBitSet: ImmutableBitSet): Boolean = ???

  override def accept(relShuttle: RelShuttle): RelNode = ???

  override def accept(rexShuttle: RexShuttle): RelNode = ???

  override def getId: Int = ???

  override def getDigest: String = ???

  override def getDescription: String = ???
}

object TestingExecNode {
  val typeSystem: RelDataTypeSystem = new FlinkTypeSystem
  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(typeSystem)
  val context: FlinkContextImpl = new FlinkContextImpl(new TableConfig, null, null, null)
  val planner: RelOptPlanner = new HepPlanner(HepProgram.builder().build(), context)
  val cluster: RelOptCluster = FlinkRelOptClusterFactory.create(planner, new FlinkRexBuilder(typeFactory))
  val traitSet: RelTraitSet = RelTraitSet.createEmpty()
}
