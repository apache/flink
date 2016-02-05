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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.TableConfig

/**
  * Flink RelNode which matches along with SortPartitionOperator.
  */
class DataSetSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowType: RelDataType,
    opName: String,
    sortKey: Array[Int],
    sortOrder: Array[Boolean])
  extends SingleRel(cluster, traitSet, input)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetSort(
      cluster,
      traitSet,
      inputs.get(0),
      rowType,
      opName,
      sortKey,
      sortOrder
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("name", opName)
  }

  override def translateToPlan(
      config: TableConfig,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {
    ???
  }
}
