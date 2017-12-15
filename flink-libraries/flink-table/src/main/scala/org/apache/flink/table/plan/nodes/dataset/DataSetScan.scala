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

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment}
import org.apache.flink.table.plan.schema.{DataSetTable, RowSchema}
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with DataSource.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class DataSetScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    rowRelDataType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with BatchScan {

  val dataSetTable: DataSetTable[Any] = getTable.unwrap(classOf[DataSetTable[Any]])

  override def deriveRowType(): RelDataType = rowRelDataType

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetScan(
      cluster,
      traitSet,
      getTable,
      getRowType
    )
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): DataSet[Row] = {
    val schema = new RowSchema(rowRelDataType)
    val inputDataSet: DataSet[Any] = dataSetTable.dataSet
    val fieldIdxs = dataSetTable.fieldIndexes
    val config = tableEnv.getConfig
    convertToInternalRow(schema, inputDataSet, fieldIdxs, config, None)
  }

}
