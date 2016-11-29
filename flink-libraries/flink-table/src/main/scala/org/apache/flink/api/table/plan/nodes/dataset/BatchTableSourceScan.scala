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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.{BatchTableEnvironment, FlinkTypeFactory}
import org.apache.flink.api.table.plan.schema.TableSourceTable
import org.apache.flink.api.table.sources.BatchTableSource

/** Flink RelNode to read data from an external source defined by a [[BatchTableSource]]. */
class BatchTableSourceScan(
                            cluster: RelOptCluster,
                            traitSet: RelTraitSet,
                            table: RelOptTable,
                            tableSource: Option[BatchTableSource[_]] = None)
  extends BatchScan(cluster, traitSet, table) {

  override def deriveRowType() = {
    tableSource match {
      case Some(ts) =>
        val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
        val builder = flinkTypeFactory.builder
        ts.getFieldsNames
          .zip(ts.getFieldTypes)
          .foreach { f =>
            builder.add(f._1, flinkTypeFactory.createTypeFromTypeInfo(f._2)).nullable(true)
          }
        builder.build
      case None => getTable.getRowType
    }
  }


  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSource
    )
  }

  override def translateToPlan(
                                tableEnv: BatchTableEnvironment,
                                expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val config = tableEnv.getConfig

    tableSource match {
      case Some(ts) =>
        val inputDs = ts.getDataSet(tableEnv.execEnv).asInstanceOf[DataSet[Any]]
        convertToExpectedType(inputDs, new TableSourceTable(ts), expectedType, config)
      case None =>
        val originTableSourceTable = getTable.unwrap(classOf[TableSourceTable])
        val originTableSource = originTableSourceTable.tableSource.asInstanceOf[BatchTableSource[_]]
        val originDs = originTableSource.getDataSet(tableEnv.execEnv).asInstanceOf[DataSet[Any]]
        convertToExpectedType(originDs, originTableSourceTable, expectedType, config)

    }
  }
}
