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

package org.apache.flink.api.table.plan.rules.dataset

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention, DataSetFlatMap}
import org.apache.flink.api.table.plan.nodes.logical.{FlinkCalc, FlinkConvention}

class DataSetCalcRule
  extends ConverterRule(
    classOf[FlinkCalc],
    FlinkConvention.INSTANCE,
    DataSetConvention.INSTANCE,
    "DataSetCalcRule")
{

  def convert(rel: RelNode): RelNode = {
    val calc: FlinkCalc = rel.asInstanceOf[FlinkCalc]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(calc.getInput, DataSetConvention.INSTANCE)

    new DataSetFlatMap(
      rel.getCluster,
      traitSet,
      convInput,
      rel.getRowType,
      calc.toString,
      null)
  }
}

object DataSetCalcRule {
  val INSTANCE: RelOptRule = new DataSetCalcRule
}
