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
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.runtime.aggregate.DistinctReduce
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
  * DataSet RelNode for a Distinct (LogicalAggregate without aggregation functions).
  *
  */
class DataSetDistinct(
   cluster: RelOptCluster,
   traitSet: RelTraitSet,
   input: RelNode,
   rowRelDataType: RelDataType,
   ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetDistinct(
      cluster,
      traitSet,
      inputs.get(0),
      rowRelDataType,
      ruleDescription
    )
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    val rowSize = this.estimateRowSize(child.getRowType)
    // less expensive than DataSetAggregate without aggregates
    planner.getCostFactory.makeCost(rowCnt, 0, rowCnt * rowSize * 0.9)
  }

  override def toString: String = {
    s"Distinct(distinct: (${rowTypeToString(rowRelDataType)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("distinct", rowTypeToString(rowRelDataType))
  }

  def rowTypeToString(rowType: RelDataType): String = {
    rowType.getFieldList.asScala.map(_.getName).mkString(", ")
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl): DataSet[Row] = {

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val groupKeys = (0 until rowRelDataType.getFieldCount).toArray // group on all fields

    inputDS
      .groupBy(groupKeys: _*)
      .reduce(new DistinctReduce)
      .setCombineHint(CombineHint.HASH) // use hash-combiner
      .name("distinct")
      .returns(inputDS.getType)
  }

}


