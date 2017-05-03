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
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.runtime.aggregate.DataStreamDistinctReduce
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class DataStreamDistinct(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowRelDataType: RelDataType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input) with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamDistinct(
      cluster,
      traitSet,
      inputs.get(0),
      rowRelDataType,
      ruleDescription
    )
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

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val inputDS = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    val groupKeys = (0 until rowRelDataType.getFieldCount).toArray
    // group on all fields
    inputDS
      .keyBy(groupKeys: _*)
      .flatMap(new DataStreamDistinctReduce(inputDS.getType))
      .name("distinct")
      .returns(inputDS.getType)
  }

}
