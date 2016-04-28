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

package org.apache.flink.api.table.plan.rules.dataSet

import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.{LogicalJoin, LogicalSort}
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention, DataSetSort}

class DataSetSortRule
  extends ConverterRule(
    classOf[LogicalSort],
    Convention.NONE,
    DataSetConvention.INSTANCE,
    "DataSetSortRule") {

  /**
    * Only translate when no OFFSET or LIMIT specified
    */
  override def matches(call: RelOptRuleCall): Boolean = {
    val sort = call.rel(0).asInstanceOf[LogicalSort]
    sort.offset == null && sort.fetch == null
  }

  override def convert(rel: RelNode): RelNode = {

    val sort: LogicalSort = rel.asInstanceOf[LogicalSort]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(sort.getInput, DataSetConvention.INSTANCE)

    new DataSetSort(
      rel.getCluster,
      traitSet,
      convInput,
      sort.getCollation,
      rel.getRowType
    )
  }
}

object DataSetSortRule {
  val INSTANCE: RelOptRule = new DataSetSortRule
}
