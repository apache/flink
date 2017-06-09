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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow

/**
  * Flink RelNode which matches along with Union.
  *
  */
class DataStreamUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    schema: RowSchema)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with DataStreamRel {

  override def deriveRowType() = schema.logicalType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamUnion(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      schema
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union all", unionSelectionToString)
  }

  override def toString = {
    s"Union All(union: (${schema.logicalFieldNames.mkString(", ")}))"
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val leftDataSet = left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val rightDataSet = right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    leftDataSet.union(rightDataSet)
  }

  private def unionSelectionToString: String = {
    schema.logicalFieldNames.mkString(", ")
  }
}
