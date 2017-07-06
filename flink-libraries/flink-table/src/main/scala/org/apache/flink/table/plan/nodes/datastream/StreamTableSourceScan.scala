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
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.sources._
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.runtime.operators.AdjustWatermark
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sources.{StreamTableSource, TableSource}

/** Flink RelNode to read data from an external source defined by a [[StreamTableSource]]. */
class StreamTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSource: StreamTableSource[_])
  extends PhysicalTableSourceScan(cluster, traitSet, table, tableSource)
  with StreamScan {

  override def deriveRowType() = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val fieldNames = TableEnvironment.getFieldNames(tableSource).toList
    val fieldTypes = TableEnvironment.getFieldTypes(tableSource.getReturnType).toList

    val fieldCnt = fieldNames.length

    val rowtime = tableSource match {
      case timeSource: DefinedRowtimeAttribute if timeSource.getRowtimeAttribute != null =>
        val rowtimeAttribute = timeSource.getRowtimeAttribute
        Some((fieldCnt, rowtimeAttribute))
      case _ =>
        None
    }

    val proctime = tableSource match {
      case timeSource: DefinedProctimeAttribute if timeSource.getProctimeAttribute != null =>
        val proctimeAttribute = timeSource.getProctimeAttribute
        Some((fieldCnt + (if (rowtime.isDefined) 1 else 0), proctimeAttribute))
      case _ =>
        None
    }

    flinkTypeFactory.buildLogicalRowType(
      fieldNames,
      fieldTypes,
      rowtime,
      proctime)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSource
    )
  }

  override def copy(
      traitSet: RelTraitSet,
      newTableSource: TableSource[_])
    : PhysicalTableSourceScan = {

    new StreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      newTableSource.asInstanceOf[StreamTableSource[_]]
    )
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig
    val ds = tableSource.getDataStream(tableEnv.execEnv).asInstanceOf[DataStream[Any]]
    val offset = queryConfig.getLateDataTimeOffset

    // injecting an operator, which adjust watermark according queryConfig
    val inputDataStream: DataStream[Any] = if (offset != 0) {
      ds.transform(
        s"AdjustWatermark(${offset})",
        ds.getType,
        AdjustWatermark.of[Any](offset))
    } else {
      ds
    }

    convertToInternalRow(
      new RowSchema(getRowType),
      inputDataStream,
      new TableSourceTable(tableSource),
      config)
  }
}
