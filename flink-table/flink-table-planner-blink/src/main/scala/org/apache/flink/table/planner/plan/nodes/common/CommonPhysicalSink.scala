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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.InputTypeConfigurable
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.streaming.api.transformations.SinkTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.sink.{DynamicTableSink, OutputFormatProvider, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.sinks.TableSinkUtils
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext
import org.apache.flink.table.runtime.operators.sink.SinkOperator
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConversions._

/**
 * Base physical RelNode to write data to an external sink defined by a [[DynamicTableSink]].
 */
class CommonPhysicalSink (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    tableIdentifier: ObjectIdentifier,
    catalogTable: CatalogTable,
    tableSink: DynamicTableSink)
  extends Sink(cluster, traitSet, inputRel, tableIdentifier, catalogTable, tableSink)
  with FlinkPhysicalRel {

  /**
   * Common implementation to create sink transformation for both batch and streaming.
   */
  protected def createSinkTransformation(
      env: StreamExecutionEnvironment,
      inputTransformation: Transformation[RowData],
      tableConfig: TableConfig,
      rowtimeFieldIndex: Int,
      isBounded: Boolean): Transformation[Any] = {
    val inputTypeInfo = new RowDataTypeInfo(FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val runtimeProvider = tableSink.getSinkRuntimeProvider(
      new SinkRuntimeProviderContext(isBounded))
    val sinkFunction = runtimeProvider match {
      case provider: SinkFunctionProvider => provider.createSinkFunction()
      case provider: OutputFormatProvider =>
        val outputFormat = provider.createOutputFormat()
        new OutputFormatSinkFunction(outputFormat)
    }

    sinkFunction match {
      case itc: InputTypeConfigurable =>
        // configure the type if needed
        itc.setInputType(inputTypeInfo, env.getConfig)
      case _ => // nothing to do
    }

    val notNullEnforcer = tableConfig.getConfiguration
      .get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER)
    val notNullFieldIndices = TableSinkUtils.getNotNullFieldIndices(catalogTable)
    val fieldNames = catalogTable.getSchema.toPhysicalRowDataType
      .getLogicalType.asInstanceOf[RowType]
      .getFieldNames
      .toList.toArray
    val operator = new SinkOperator(
      env.clean(sinkFunction),
      rowtimeFieldIndex,
      notNullEnforcer,
      notNullFieldIndices,
      fieldNames
    )

    new SinkTransformation(
      inputTransformation,
      getRelDetailedDescription,
      SimpleOperatorFactory.of(operator),
      inputTransformation.getParallelism).asInstanceOf[Transformation[Any]]
  }

}
