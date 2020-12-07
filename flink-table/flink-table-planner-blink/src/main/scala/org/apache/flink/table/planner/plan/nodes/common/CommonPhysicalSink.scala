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
import org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory
import org.apache.flink.streaming.api.transformations.{LegacySinkTransformation, PartitionTransformation}
import org.apache.flink.streaming.runtime.partitioner.{KeyGroupStreamPartitioner, StreamPartitioner}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.{ChangelogMode, ParallelismProvider}
import org.apache.flink.table.connector.sink.{DataStreamSinkProvider, DynamicTableSink, OutputFormatProvider, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.types.RowKind
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil
import org.apache.flink.table.planner.sinks.TableSinkUtils
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext
import org.apache.flink.table.runtime.operators.sink.{SinkNotNullEnforcer, SinkOperator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.utils.TableSchemaUtils

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
      isBounded: Boolean,
      changelogMode:ChangelogMode): Transformation[Any] = {
    val inputTypeInfo = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val runtimeProvider = tableSink.getSinkRuntimeProvider(
      new SinkRuntimeProviderContext(isBounded))

    val notNullEnforcer = tableConfig.getConfiguration
      .get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER)
    val notNullFieldIndices = TableSinkUtils.getNotNullFieldIndices(catalogTable)
    val fieldNames = catalogTable.getSchema.toPhysicalRowDataType
        .getLogicalType.asInstanceOf[RowType]
        .getFieldNames
        .toList.toArray
    val enforcer = new SinkNotNullEnforcer(notNullEnforcer, notNullFieldIndices, fieldNames)

    runtimeProvider match {
      case _: DataStreamSinkProvider with ParallelismProvider =>
        throw new TableException("`DataStreamSinkProvider` is not allowed to work with" +
          " `ParallelismProvider`, " + "please see document of `ParallelismProvider`")
      case provider: DataStreamSinkProvider =>
        val dataStream = new DataStream(env, inputTransformation).filter(enforcer)
        provider.consumeDataStream(dataStream).getTransformation.asInstanceOf[Transformation[Any]]
      case _ =>
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

        val operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex, enforcer)

        assert(runtimeProvider.isInstanceOf[ParallelismProvider],
          "runtimeProvider with `ParallelismProvider` implementation is required")

        val inputParallelism = inputTransformation.getParallelism
        val parallelism =  {
          val parallelismOptional = runtimeProvider.asInstanceOf[ParallelismProvider].getParallelism
          if (parallelismOptional.isPresent) {
            val parallelismPassedIn = parallelismOptional.get().intValue()
            if (parallelismPassedIn <= 0) {
              throw new TableException(s"Table: $tableIdentifier configured sink parallelism: " +
                s"$parallelismPassedIn should not be less than zero or equal to zero")
            }
            parallelismPassedIn
          } else {
            inputParallelism
          }
        }

        val primaryKeys = TableSchemaUtils.getPrimaryKeyIndices(catalogTable.getSchema)
        val theFinalInputTransformation = if (inputParallelism == parallelism ||
            changelogMode.containsOnly(RowKind.INSERT)) {
          // if the inputParallelism is equals to the parallelism or insert-only mode, do nothing.
          inputTransformation
        } else if (primaryKeys.isEmpty) {
            throw new TableException(s"Table: $tableIdentifier configured sink parallelism is: " +
              s"$parallelism, while the input parallelism is: $inputParallelism. Since " +
              s"configured parallelism is different from input parallelism and the changelog " +
              s"mode contains [${changelogMode.getContainedKinds.toList.mkString(",")}], which " +
              s"is not INSERT_ONLY mode, primary key is required but no primary key is found")
        } else {
          //key by before sink
          //according to [[StreamExecExchange]]
          val selector = KeySelectorUtil.getRowDataSelector(primaryKeys, inputTypeInfo)
          val partitioner = new KeyGroupStreamPartitioner(selector,
            DEFAULT_LOWER_BOUND_MAX_PARALLELISM)
          val transformation = new PartitionTransformation(
            inputTransformation,
            partitioner.asInstanceOf[StreamPartitioner[RowData]])
          transformation.setParallelism(parallelism)
          transformation
        }

        new LegacySinkTransformation(
          theFinalInputTransformation,
          getRelDetailedDescription,
          SimpleOperatorFactory.of(operator),
          parallelism).asInstanceOf[Transformation[Any]]
    }
  }

}
