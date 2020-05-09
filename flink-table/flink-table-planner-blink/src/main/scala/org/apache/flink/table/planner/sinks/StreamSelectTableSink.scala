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
import org.apache.flink.core.execution.JobClient
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.experimental.{CollectSink, SocketStreamIterator}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.internal.SelectTableSink
import org.apache.flink.table.sinks.AppendStreamTableSink
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import java.net.InetAddress
import java.util

/**
  * A [[SelectTableSink]] for streaming select job.
  *
  * <p><strong>NOTES:</strong> This is a temporary solution,
  * once FLINK-14807 is finished, the implementation should be changed.
  * Currently, only insert changes (AppendStreamTableSink) is supported.
  * Once FLINK-16998 is finished, all kinds of changes will be supported.
  */
class StreamSelectTableSink(tableSchema: TableSchema)
  extends AppendStreamTableSink[Row]
  with SelectTableSink {

  private val typeSerializer = tableSchema.toRowType.createSerializer(new ExecutionConfig)
  // socket server should be started before running the job
  private val iterator = new SocketStreamIterator[Row](0, InetAddress.getLocalHost, typeSerializer)

  override def getTableSchema: TableSchema = tableSchema

  override def getConsumedDataType: DataType = tableSchema.toRowDataType

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    dataStream
      .addSink(new CollectSink[Row](iterator.getBindAddress, iterator.getPort, typeSerializer))
      .name("Streaming select table sink")
      .setParallelism(1)
  }

  override def setJobClient(jobClient: JobClient): Unit = {
  }

  override def getResultIterator: util.Iterator[Row] = iterator
}
