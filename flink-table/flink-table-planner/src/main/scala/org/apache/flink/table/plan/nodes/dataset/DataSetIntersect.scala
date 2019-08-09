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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchQueryConfig
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.runtime.IntersectCoGroupFunction
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which translates Intersect into CoGroup Operator.
  *
  */
class DataSetIntersect(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    rowRelDataType: RelDataType,
    all: Boolean)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
    with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetIntersect(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      all
    )
  }

  override def toString: String = {
    s"Intersect(intersect: ($intersectSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("intersect", intersectSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val children = this.getInputs
    children.foldLeft(planner.getCostFactory.makeCost(0, 0, 0)) { (cost, child) =>
      val rowCnt = metadata.getRowCount(child)
      val rowSize = this.estimateRowSize(child.getRowType)
      cost.plus(planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize))
    }
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvImpl,
      queryConfig: BatchQueryConfig): DataSet[Row] = {

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)

    val coGroupedDs = leftDataSet.coGroup(rightDataSet)

    val coGroupOpName = s"intersect: ($intersectSelectionToString)"
    val coGroupFunction = new IntersectCoGroupFunction[Row](all)

    coGroupedDs
      .where("*")
      .equalTo("*")
      .`with`(coGroupFunction)
      .name(coGroupOpName)
  }

  private def intersectSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

}
