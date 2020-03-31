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
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.core.io.InputSplit
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.{BatchQueryConfig, TableException, Types}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.sources._
import org.apache.flink.table.types.utils.TypeConversions.{fromDataTypeToLegacyInfo, fromLegacyInfoToDataType}
import org.apache.flink.types.Row

/** Flink RelNode to read data from an external source defined by a [[BatchTableSource]]. */
class BatchTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSource: TableSource[_],
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
      tableSource,
      selectedFields
    )
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvImpl,
      queryConfig: BatchQueryConfig): DataSet[Row] = {

    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      selectedFields)

    val config = tableEnv.getConfig
    val inputDataSet = tableSource match {
      case batchSource: BatchTableSource[_] =>
        batchSource.getDataSet(tableEnv.execEnv)
      case boundedSource: InputFormatTableSource[_] =>
        val resultType = fromDataTypeToLegacyInfo(boundedSource.getProducedDataType)
            .asInstanceOf[TypeInformation[Any]]
        val inputFormat = boundedSource.getInputFormat
          .asInstanceOf[InputFormat[Any, _ <: InputSplit]]
        tableEnv.execEnv
          .createInput(inputFormat, resultType)
          .name(boundedSource.explainSource)
          .asInstanceOf[DataSet[_]]
      case _ => throw new TableException("Only BatchTableSource and InputFormatTableSource are " +
        "supported in BatchTableEnvironment.")
    }
    val outputSchema = new RowSchema(this.getRowType)

    val inputDataType = fromLegacyInfoToDataType(inputDataSet.getType)
    val producedDataType = tableSource.getProducedDataType

    // check that declared and actual type of table source DataSet are identical
    if (inputDataType != producedDataType) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataSet of data type $inputDataType that does not match with the " +
        s"data type $producedDataType declared by the TableSource.getProducedDataType() method. " +
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
