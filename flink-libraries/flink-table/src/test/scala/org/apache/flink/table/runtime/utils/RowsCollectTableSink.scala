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

package org.apache.flink.table.runtime.utils

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.sinks.{BatchTableSink, TableSink, TableSinkBase}
import org.apache.flink.types.Row

import scala.collection.mutable

/**
  * A simple [[TableSink]] to collect rows of every partition.
  *
  */
@Internal
class RowsCollectTableSink extends TableSinkBase[Row] with BatchTableSink[Row] {

  private var rowsCollectOutputFormat: RowsCollectOutputFormat = _

  override def emitBoundedStream(boundedStream: DataStream[Row],
                                  tableConfig: TableConfig,
                                  executionConfig: ExecutionConfig): DataStreamSink[Row] = {
    boundedStream.writeUsingOutputFormat(rowsCollectOutputFormat).name("collect")
  }

  override protected def copy: TableSinkBase[Row] = {
    val sink = new RowsCollectTableSink()
    sink.rowsCollectOutputFormat = rowsCollectOutputFormat
    sink
  }

  override def getOutputType: DataType = {
    DataTypes.createRowType(getFieldTypes: _*)
  }

  def init(typeSerializer: TypeSerializer[Seq[Row]], id: String): Unit = {
    this.rowsCollectOutputFormat = new RowsCollectOutputFormat(typeSerializer, id)
  }
}

class RowsCollectOutputFormat(typeSerializer: TypeSerializer[Seq[Row]], id: String)
    extends RichOutputFormat[Row] {

  private var accumulator: SerializedListAccumulator[Seq[Row]] = _
  private var rows: mutable.ArrayBuffer[Row] = _

  override def writeRecord(record: Row): Unit = {
    this.rows += Row.copy(record)
  }

  override def configure(parameters: Configuration): Unit = {
  }

  override def close(): Unit = {
    val rowSeq: Seq[Row] = rows
    accumulator.add(rowSeq, typeSerializer)
    getRuntimeContext.addAccumulator(id, accumulator)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    this.accumulator = new SerializedListAccumulator[Seq[Row]]
    this.rows = new mutable.ArrayBuffer[Row]
  }
}
