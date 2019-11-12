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

package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetSort
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSort

class DataSetSortRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetSortRule") {

  override def convert(rel: RelNode): RelNode = {
    val sort: FlinkLogicalSort = rel.asInstanceOf[FlinkLogicalSort]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val convInput: RelNode = RelOptRule.convert(sort.getInput, FlinkConventions.DATASET)

    new DataSetSort(
      rel.getCluster,
      traitSet,
      convInput,
      sort.getCollation,
      rel.getRowType,
      sort.offset,
      sort.fetch
    )
  }
}

object DataSetSortRule {
  val INSTANCE: RelOptRule = new DataSetSortRule
}
