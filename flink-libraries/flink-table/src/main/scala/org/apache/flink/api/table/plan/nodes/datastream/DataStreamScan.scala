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

package org.apache.flink.api.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.StreamTableEnvironment
import org.apache.flink.api.table.plan.schema.DataStreamTable
import org.apache.flink.streaming.api.datastream.DataStream

/**
  * Flink RelNode which matches along with DataStreamSource.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class DataStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowType: RelDataType)
  extends StreamScan(cluster, traitSet, table, rowType) {

  val dataStreamTable: DataStreamTable[Any] = table.unwrap(classOf[DataStreamTable[Any]])

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamScan(
      cluster,
      traitSet,
      table,
      rowType
    )
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataStream[Any] = {

    val config = tableEnv.getConfig
    val inputDataStream: DataStream[Any] = dataStreamTable.dataStream

    convertToExpectedType(inputDataStream, dataStreamTable, expectedType, config)
  }

}
