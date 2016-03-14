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

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelWriter, BiRel, RelNode}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.TableConfig

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

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

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union", unionSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner): RelOptCost = {

    val children = this.getInputs
    val rowCnt = children.foldLeft(0D) { (rows, child) =>
      rows + RelMetadataQuery.getRowCount(child)
    }

    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }

  override def translateToPlan(
      config: TableConfig,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(config)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(config)
    leftDataSet.union(rightDataSet).asInstanceOf[DataSet[Any]]
  }

  private def unionSelectionToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

}
