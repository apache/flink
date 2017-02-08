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
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamTableEnvironment, TableEnvironment}

/** Flink RelNode to read data from an external source defined by a [[StreamTableSource]]. */
class StreamTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    val tableSource: StreamTableSource[_])
  extends StreamScan(cluster, traitSet, table) {

  override def deriveRowType() = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    flinkTypeFactory.buildRowDataType(
      TableEnvironment.getFieldNames(tableSource),
      TableEnvironment.getFieldTypes(tableSource.getReturnType))
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

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", TableEnvironment.getFieldNames(tableSource).mkString(", "))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataStream[Any] = {

    val config = tableEnv.getConfig
    val inputDataStream: DataStream[Any] = tableSource
      .getDataStream(tableEnv.execEnv).asInstanceOf[DataStream[Any]]

    convertToExpectedType(inputDataStream, new TableSourceTable(tableSource), expectedType, config)
  }

}
