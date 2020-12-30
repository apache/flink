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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalSink
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode to to write data into an external sink defined by a
 * [[DynamicTableSink]].
  */
class StreamExecSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    tableIdentifier: ObjectIdentifier,
    catalogTable: CatalogTable,
    tableSink: DynamicTableSink)
  extends CommonPhysicalSink(cluster, traitSet, inputRel, tableIdentifier, catalogTable, tableSink)
  with StreamPhysicalRel
  with LegacyStreamExecNode[Any] {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecSink(cluster, traitSet, inputs.get(0), tableIdentifier, catalogTable, tableSink)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[Any] = {

    // get RowData plan
    val inputTransformation = getInputNodes.get(0) match {
      // Sink's input must be LegacyStreamExecNode[RowData] or StreamExecNode[RowData] now.
      case legacyNode: LegacyStreamExecNode[RowData] => legacyNode.translateToPlan(planner)
      case node: StreamExecNode[RowData] => node.translateToPlan(planner)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
    val inputLogicalType = getInput.getRowType
    val rowtimeFields = inputLogicalType.getFieldList.zipWithIndex
      .filter { case (f, _) =>
        FlinkTypeFactory.isRowtimeIndicatorType(f.getType)
      }
    if (rowtimeFields.size > 1) {
      throw new TableException(
        s"Found more than one rowtime field: [${rowtimeFields.map(_._1.getName).mkString(", ")}]" +
          s" in the query when insert into '${tableIdentifier.asSummaryString()}'.\n" +
          s"Please select the rowtime field that should be used as event-time timestamp for the " +
          s"DataStream by casting all other fields to TIMESTAMP.")
    }

    val inputChangelogMode = ChangelogPlanUtils.getChangelogMode(
      getInput.asInstanceOf[StreamPhysicalRel]).get
    // tell sink the ChangelogMode of input
    val changelogMode = tableSink.getChangelogMode(inputChangelogMode)
    val rowtimeFieldIndex: Int = rowtimeFields.map(_._2).headOption.getOrElse(-1)

    createSinkTransformation(
      planner.getExecEnv,
      inputTransformation,
      planner.getTableConfig,
      rowtimeFieldIndex,
      isBounded = false,
      changelogMode = changelogMode)
  }
}
