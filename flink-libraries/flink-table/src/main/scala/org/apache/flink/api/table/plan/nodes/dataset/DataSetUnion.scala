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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.BatchTableEnvironment

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
* Flink RelNode which matches along with UnionOperator.
*
*/
class DataSetUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType)
  extends BiRel(cluster, traitSet, left, right)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetUnion(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType
    )
  }

  override def toString: String = {
    s"Union(union: ($unionSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union", unionSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val children = this.getInputs
    val rowCnt = children.foldLeft(0D) { (rows, child) =>
      rows + metadata.getRowCount(child)
    }

    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    var leftDataSet: DataSet[Any] = null
    var rightDataSet: DataSet[Any] = null

    expectedType match {
      case None =>
        leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
        rightDataSet =
          right.asInstanceOf[DataSetRel].translateToPlan(tableEnv, Some(leftDataSet.getType))
      case _ =>
        leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv, expectedType)
        rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv, expectedType)
    }

    leftDataSet.union(rightDataSet).asInstanceOf[DataSet[Any]]
  }

  private def unionSelectionToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

}
