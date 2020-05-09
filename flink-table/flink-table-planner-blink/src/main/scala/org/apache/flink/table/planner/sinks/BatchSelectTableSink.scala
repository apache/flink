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

package org.apache.flink.table.planner.sinks

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.java.Utils
import org.apache.flink.core.execution.JobClient
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.internal.SelectTableSink
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.sinks.StreamTableSink
import org.apache.flink.table.types.DataType
import org.apache.flink.types
import org.apache.flink.types.Row
import org.apache.flink.util.{AbstractID, Preconditions}

import java.util

/**
  * A [[SelectTableSink]] for batch job.
  *
  * <p><strong>NOTES:</strong> This is a temporary solution,
  * once FLINK-14807 is finished, this class can be removed.
  */
class BatchSelectTableSink(tableSchema: TableSchema)
  extends StreamTableSink[Row]
  with SelectTableSink {

  private val accumulatorName = new AbstractID().toString
  private val typeSerializer = tableSchema.toRowType.createSerializer(new ExecutionConfig)

  private var jobClient: JobClient = _

  override def getTableSchema: TableSchema = tableSchema

  override def getConsumedDataType: DataType = tableSchema.toRowDataType

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    dataStream.writeUsingOutputFormat(new Utils.CollectHelper[Row](accumulatorName, typeSerializer))
      .name("Batch select table sink")
      .setParallelism(1)
  }

  override def setJobClient(jobClient: JobClient): Unit = {
    this.jobClient = Preconditions.checkNotNull(jobClient, "jobClient should not be null")
  }

  override def getResultIterator: util.Iterator[types.Row] = {
    Preconditions.checkNotNull(jobClient, "jobClient is null, please call setJobClient first.")
    val jobExecutionResult = jobClient.getJobExecutionResult(
      Thread.currentThread().getContextClassLoader)
      .get()
    val accResult: util.ArrayList[Array[Byte]] =
      jobExecutionResult.getAccumulatorResult(accumulatorName)
    if (accResult == null) {
      throw new TableException("Failed to get result.")
    }
    val rowList = SerializedListAccumulator.deserializeList(accResult, typeSerializer)
    rowList.iterator()
  }
}
