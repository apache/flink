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

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner, RelTraitSet, RelOptCluster}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelWriter, RelNode, SingleRel}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.Row

/**
  * Flink RelNode which matches along with FlatMapOperator.
  *
  */
class DataSetFlatMap(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowType: RelDataType,
    opName: String,
    func: FlatMapFunction[Row, Row])
  extends SingleRel(cluster, traitSet, input)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetFlatMap(
      cluster,
      traitSet,
      inputs.get(0),
      rowType,
      opName,
      func
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("name", opName)
  }

  override def translateToPlan: DataSet[Any] = {
    ???
  }
}
