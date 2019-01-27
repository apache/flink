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

package org.apache.flink.table.temptable

import org.apache.flink.configuration.Configuration
import org.apache.flink.service.ServiceRegistryFactory
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType, TypeConverters}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource}
import org.apache.flink.table.temptable.rpc.TableServiceClient
import org.apache.flink.table.temptable.util.TableServiceUtil
import org.apache.flink.table.typeutils.BaseRowSerializer
import org.apache.flink.table.util.{TableProperties, TableSchemaUtil}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class FlinkTableServiceSource(
  tableProperties: TableProperties,
  tableName: String,
  resultType: RowType) extends BatchTableSource[BaseRow] with StreamTableSource[BaseRow] {

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    streamEnv.addSource(
      new FlinkTableServiceSourceFunction(
        tableProperties,
        tableName,
        resultType)
    ).returns(
      TypeConverters.toBaseRowTypeInfo(resultType)
    )
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    execEnv.addSource(new FlinkTableServiceSourceFunction(
      tableProperties,
      tableName,
      resultType)
    ).returns(
      TypeConverters.toBaseRowTypeInfo(resultType)
    )
  }

  override def getReturnType: DataType = resultType

  override def getTableSchema: TableSchema = TableSchemaUtil.fromDataType(resultType)
}

/**
  * Built-in SourceFunction for FlinkTableServiceSource.
  */
class FlinkTableServiceSourceFunction(
  tableProperties: TableProperties,
  tableName: String,
  resultType: RowType) extends RichParallelSourceFunction[BaseRow] {

  private var flinkTableServiceClient: TableServiceClient = _

  private var tablePartionReadRange: Seq[Int] = _

  private var baseRowSerializer: BaseRowSerializer[_ <: BaseRow] = _

  override def open(parameters: Configuration): Unit = {
    flinkTableServiceClient = new TableServiceClient
    flinkTableServiceClient.setRegistry(ServiceRegistryFactory.getRegistry)
    flinkTableServiceClient.open(tableProperties)
    baseRowSerializer =
      DataTypes.createInternalSerializer(resultType).asInstanceOf[BaseRowSerializer[BaseRow]]
    val maxRetry = parameters
      .getInteger(TableServiceOptions.TABLE_SERVICE_READY_RETRY_TIMES)
    val backOffMs = parameters
      .getLong(TableServiceOptions.TABLE_SERVICE_READY_RETRY_BACKOFF_MS)
    TableServiceUtil.checkTableServiceReady(flinkTableServiceClient, maxRetry, backOffMs)
    assignReadRange()
  }

  override def close(): Unit = {
    if (flinkTableServiceClient != null) {
      flinkTableServiceClient.close()
    }
  }

  override def run(ctx: SourceFunction.SourceContext[BaseRow]): Unit = {

    tablePartionReadRange.foreach {
      partitionId => {
        var reachEnd = false
        while (!reachEnd) {
          val row = flinkTableServiceClient.readNext(tableName, partitionId, baseRowSerializer)
          if (row != null) {
            ctx.collect(row)
          } else {
            reachEnd = true
          }
        }
      }
    }
  }

  override def cancel(): Unit = {}

  private def assignReadRange(): Unit = {
    val partitions = flinkTableServiceClient.getPartitions(tableName).asScala.sorted
    val workerCount = getRuntimeContext.getNumberOfParallelSubtasks
    var startIndex = getRuntimeContext.getIndexOfThisSubtask
    val result = new ListBuffer[Int]
    while (startIndex < partitions.size) {
      result += partitions(startIndex)
      startIndex += workerCount
    }
    tablePartionReadRange = result
    if (tablePartionReadRange.isEmpty) {
      throw new TableServiceException(new RuntimeException("Table Cache do not exists."))
    }
  }
}
