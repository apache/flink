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

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with DataStreamSource.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class DataStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowRelDataType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with StreamScan {

  val dataStreamTable: DataStreamTable[Any] = getTable.unwrap(classOf[DataStreamTable[Any]])

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamScan(
      cluster,
      traitSet,
      getTable,
      getRowType
    )
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {
    val config = tableEnv.getConfig
    val inputDataStream: DataStream[Any] = dataStreamTable.dataStream
    convertToInternalRow(inputDataStream, dataStreamTable, config)
  }

}
