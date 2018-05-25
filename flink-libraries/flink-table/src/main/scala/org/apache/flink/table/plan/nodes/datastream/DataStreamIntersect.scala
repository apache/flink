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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowKeySelector
import org.apache.flink.table.runtime.setop.NonWindowIntersect
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConverters._

/**
  * RelNode for non-window stream intersect
  */
class DataStreamIntersect(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  leftNode: RelNode,
  rightNode: RelNode,
  rowRelDataType: RelDataType,
  all: Boolean)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
    with DataStreamRel {

  private lazy val intersectType = if (all) {
    "IntersectAll"
  } else {
    "Intersect"
  }

  override def needsUpdatesAsRetraction: Boolean = true

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamIntersect(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      all
    )
  }

  override def toString: String = {
    s"$intersectType($intersectSelectionToString)"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item(s"$intersectType", intersectSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val children = this.getInputs
    children.asScala.foldLeft(planner.getCostFactory.makeCost(0, 0, 0)) { (cost, child) =>
      val rowCnt = metadata.getRowCount(child)
      val rowSize = this.estimateRowSize(child.getRowType)
      cost.plus(planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize))
    }
  }

  override def translateToPlan(
    tableEnv: StreamTableEnvironment,
    queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val leftDataStream = left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val rightDataStream = right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    // key by all fields
    val keys = leftNode.getRowType.getFieldList.asScala.indices.toArray

    val rowSchema = new RowSchema(rowRelDataType).projectedTypeInfo(keys)

    val coFunc = new NonWindowIntersect(
      rowSchema.asInstanceOf[RowTypeInfo],
      queryConfig,
      this.all
    )

    val opName = this.toString

    leftDataStream
      .connect(rightDataStream)
      .keyBy(
        new CRowKeySelector(keys, rowSchema),
        new CRowKeySelector(keys, rowSchema))
      .process(coFunc)
      .name(opName)
      .returns(new CRowTypeInfo(rowSchema.asInstanceOf[RowTypeInfo]))
  }

  private def intersectSelectionToString: String = {
    getRowType.getFieldNames.asScala.mkString(", ")
  }

}
