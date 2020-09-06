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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalSink
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode to to write data into an external sink defined by a
 * [[DynamicTableSink]].
  */
class BatchExecSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    tableIdentifier: ObjectIdentifier,
    catalogTable: CatalogTable,
    tableSink: DynamicTableSink)
  extends CommonPhysicalSink(cluster, traitSet, inputRel, tableIdentifier, catalogTable, tableSink)
  with BatchPhysicalRel
  with BatchExecNode[Any] {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecSink(cluster, traitSet, inputs.get(0), tableIdentifier, catalogTable, tableSink)
  }

  //~ ExecNode methods -----------------------------------------------------------

  /**
    * For sink operator, the records will not pass through it, so it's DamBehavior is FULL_DAM.
    *
    * @return Returns [[DamBehavior]] of Sink.
    */
  override def getDamBehavior: DamBehavior = DamBehavior.FULL_DAM

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] = {
    List(getInput.asInstanceOf[ExecNode[BatchPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[Any] = {
    val inputTransformation = getInputNodes.get(0) match {
      // Sink's input must be BatchExecNode[RowData] now.
      case node: BatchExecNode[RowData] => node.translateToPlan(planner)
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }

    // tell sink the ChangelogMode of input, batch only supports INSERT only.
    tableSink.getChangelogMode(ChangelogMode.insertOnly())
    createSinkTransformation(
      planner.getExecEnv,
      inputTransformation,
      planner.getTableConfig,
      -1 /* not rowtime field */,
      isBounded = true)
  }
}
