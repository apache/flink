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

package org.apache.flink.api.table.plan.rules.logical

import org.apache.calcite.plan.{Convention, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention, DataSetSource}
import org.apache.flink.api.table.plan.schema.DataSetTable

class FlinkScanRule
  extends ConverterRule(
      classOf[LogicalTableScan],
      Convention.NONE,
      DataSetConvention.INSTANCE,
      "FlinkScanRule")
  {
    def convert(rel: RelNode): RelNode = {
      val scan: TableScan = rel.asInstanceOf[TableScan]
      val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)

      new DataSetSource(
        rel.getCluster,
        traitSet,
        scan.getTable,
        rel.getRowType
      )
    }
  }

object FlinkScanRule {
  val INSTANCE: RelOptRule = new FlinkScanRule
}
