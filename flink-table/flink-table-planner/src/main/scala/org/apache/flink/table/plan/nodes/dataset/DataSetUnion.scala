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

import java.util.{List => JList}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.TableException
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
* Flink RelNode which matches along with UnionOperator.
*
*/
class DataSetUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: JList[RelNode],
    rowRelDataType: RelDataType)
  extends Union(cluster, traitSet, inputs, true)
  with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode], all: Boolean): SetOp = {

    if (!all) {
      throw new TableException("DataSetUnion only supports UNION ALL.")
    }

    new DataSetUnion(
      cluster,
      traitSet,
      inputs,
      rowRelDataType)
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

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    // adopted from org.apache.calcite.rel.metadata.RelMdUtil.getUnionAllRowCount
    getInputs.foldLeft(0.0)(_ + mq.getRowCount(_))
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl): DataSet[Row] = {

    getInputs
      .asScala
      .map(_.asInstanceOf[DataSetRel].translateToPlan(tableEnv))
      .reduce((dataSetLeft, dataSetRight) => dataSetLeft.union(dataSetRight))
  }

  private def unionSelectionToString: String = {
    rowRelDataType.getFieldNames.asScala.toList.mkString(", ")
  }
}
