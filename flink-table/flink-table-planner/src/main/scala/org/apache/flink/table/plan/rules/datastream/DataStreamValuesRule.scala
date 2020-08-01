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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamValues
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalValues
import org.apache.flink.table.plan.schema.RowSchema

class DataStreamValuesRule
  extends ConverterRule(
    classOf[FlinkLogicalValues],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamValuesRule")
{

  def convert(rel: RelNode): RelNode = {
    val values: FlinkLogicalValues = rel.asInstanceOf[FlinkLogicalValues]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)

    new DataStreamValues(
      rel.getCluster,
      traitSet,
      new RowSchema(rel.getRowType),
      values.getTuples,
      "DataStreamValuesRule")
  }
}

object DataStreamValuesRule {
  val INSTANCE: RelOptRule = new DataStreamValuesRule
}
