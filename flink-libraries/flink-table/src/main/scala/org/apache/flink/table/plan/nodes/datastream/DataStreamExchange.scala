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
import org.apache.calcite.rel.core.Exchange
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow

class DataStreamExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    distribution: RelDistribution,
    schema: RowSchema)
  extends Exchange(cluster, traitSet, inputNode, distribution)
  with DataStreamRel {

  override def deriveRowType() = schema.relDataType

  override def copy(traitSet: RelTraitSet, input: RelNode,
    distribution: RelDistribution): Exchange = {
    new DataStreamExchange(
      cluster,
      traitSet,
      input,
      distribution,
      schema
    )
  }

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv    The [[StreamTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    * @return DataStream of type [[CRow]]
    */
  override def translateToPlan(tableEnv: StreamTableEnvironment,
    queryConfig: StreamQueryConfig): DataStream[CRow] = {

    // we do not need to specify input type
    val inputDS = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    distribution.getType match {
      case RelDistribution.Type.SINGLETON =>
        inputDS.global()
      case RelDistribution.Type.BROADCAST_DISTRIBUTED =>
        inputDS.broadcast()
      case RelDistribution.Type.RANDOM_DISTRIBUTED =>
        inputDS.shuffle()
      case RelDistribution.Type.HASH_DISTRIBUTED =>
        if(distribution.getKeys != null || distribution.getKeys.size() != 0){
          inputDS.hash(distribution.getKeys.get(0))
        }else{
          inputDS.hash();
        }
      case RelDistribution.Type.RANGE_DISTRIBUTED =>
        throw new TableException(
          "Exchange distribution range is not supported. ")
      case RelDistribution.Type.ROUND_ROBIN_DISTRIBUTED =>
        throw new TableException(
          "Exchange distribution round robin is not supported. ")
      case RelDistribution.Type.ANY =>
        throw new TableException(
          "Exchange distribution any is not supported. ")
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("distribution", distribution)
  }

  override def toString = s"Exchange(distribution: (${distribution.getType().name()}))"

}
