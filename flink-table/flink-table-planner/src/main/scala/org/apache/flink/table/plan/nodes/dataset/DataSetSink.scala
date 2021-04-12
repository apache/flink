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

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.plan.nodes.Sink
import org.apache.flink.table.sinks.{BatchTableSink, TableSink}
import org.apache.flink.types.Row

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

/**
  * A special [[DataSetRel]] which make explain result more pretty.
  *
  * <p>NOTES: We can't move the [[BatchTableSink#consumeDataSet]]/[[DataSet#output]] logic
  * from [[BatchTableEnvImpl]] to this node, because the return types of
  * [[DataSetRel#translateToPlan]] (which returns [[DataSet]]) and
  * [[BatchTableSink#consumeDataSet]]/[[DataSet#output]] (which returns [[DataSink]]) are
  * different. [[DataSetSink#translateToPlan]] just returns the input's translated result.
  */
class DataSetSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[_],
    sinkName: String)
  extends Sink(cluster, traitSet, inputRel, sink, sinkName)
    with DataSetRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl): DataSet[Row] = {
    getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
  }

}
