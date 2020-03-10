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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.PhysicalCollectionScan
import org.apache.flink.table.planner.plan.utils.ScanUtil
import org.apache.flink.table.runtime.operators.AbstractProcessStreamOperator

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode to read data from an external source defined by a
  * java [[util.Collection]].
  */
class StreamExecCollectionScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends PhysicalCollectionScan(cluster, traitSet, table)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false


  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecCollectionScan(cluster, traitSet, getTable)
  }

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = List()

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {
    val config = planner.getTableConfig

    val transform = getSourceTransformation(planner.getExecEnv)

    if (ScanUtil.needsConversion(collectionTable.dataType)) {
      val ctx = CodeGeneratorContext(config).setOperatorBaseClass(
        classOf[AbstractProcessStreamOperator[BaseRow]])

      val fieldCnt = getRowType.getFieldCount
      ScanUtil.convertToInternalRow(
        ctx,
        transform.asInstanceOf[Transformation[Any]],
        Array.range(0, fieldCnt),
        collectionTable.dataType,
        getRowType,
        getTable.getQualifiedName,
        config)
    } else {
      transform.asInstanceOf[Transformation[BaseRow]]
    }
  }
}
