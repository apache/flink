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
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.{BatchQueryConfig, BatchTableEnvironment, TableException, Types}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

/** Flink RelNode to read data from an external source defined by a [[BatchTableSource]]. */
class BatchTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSource: BatchTableSource[_],
    selectedFields: Option[Array[Int]])
  extends PhysicalTableSourceScan(cluster, traitSet, table, tableSource, selectedFields)
  with BatchScan {

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    TableSourceUtil.getRelDataType(
      tableSource,
      selectedFields,
      streaming = false,
      flinkTypeFactory)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSource,
      selectedFields
    )
  }

  override def copy(
      traitSet: RelTraitSet,
      newTableSource: TableSource[_]): PhysicalTableSourceScan = {

    new BatchTableSourceScan(
      cluster,
      traitSet,
      getTable,
      newTableSource.asInstanceOf[BatchTableSource[_]],
      selectedFields
    )
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      queryConfig: BatchQueryConfig): DataSet[Row] = {

    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      selectedFields)

    val config = tableEnv.getConfig
    val inputDataSet = tableSource.getDataSet(tableEnv.execEnv).asInstanceOf[DataSet[Any]]
    val outputSchema = new RowSchema(this.getRowType)

    // check that declared and actual type of table source DataSet are identical
    if (inputDataSet.getType != tableSource.getReturnType) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataSet of type ${inputDataSet.getType} that does not match with the " +
        s"type ${tableSource.getReturnType} declared by the TableSource.getReturnType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeExtractionExpression(
      tableSource,
      selectedFields,
      cluster,
      tableEnv.getRelBuilder,
      Types.SQL_TIMESTAMP
    )

    // ingest table and convert and extract time attributes if necessary
    convertToInternalRow(
      outputSchema,
      inputDataSet,
      fieldIndexes,
      config,
      rowtimeExpression)
  }
}
