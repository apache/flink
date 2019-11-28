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

import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.sinks.{StreamTableSink, TableSink, TableSinkBase}
import org.apache.flink.types.Row

/**
  * A simple [[TableSink]] to emit data as T to a collection.
  */
class CollectTableSink[T](produceOutputType: (Array[TypeInformation[_]] => TypeInformation[T]))
  extends TableSinkBase[T] with StreamTableSink[T] {

  private var collectOutputFormat: CollectOutputFormat[T] = _

  override def consumeDataStream(dataStream: DataStream[T]): DataStreamSink[_] = {
    dataStream.writeUsingOutputFormat(collectOutputFormat)
      .setParallelism(1)
      .name("collect")
  }

  override def emitDataStream(dataStream: DataStream[T]): Unit = {
    consumeDataStream(dataStream)
  }

  override protected def copy: TableSinkBase[T] = {
    new CollectTableSink(produceOutputType)
  }

  override def getOutputType: TypeInformation[T] = {
    produceOutputType(getTableSchema.getFieldTypes)
  }

  def init(typeSerializer: TypeSerializer[T], id: String): Unit = {
    collectOutputFormat = new CollectOutputFormat(id, typeSerializer)
  }
}

class CollectOutputFormat[T](id: String, typeSerializer: TypeSerializer[T])
  extends RichOutputFormat[T] {

  private var accumulator: SerializedListAccumulator[T] = _

  override def writeRecord(record: T): Unit = {
    accumulator.add(record, typeSerializer)
  }

  override def configure(parameters: Configuration): Unit = {
  }

  override def close(): Unit = {
    // Important: should only be added in close method to minimize traffic of accumulators
    getRuntimeContext.addAccumulator(id, accumulator)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    this.accumulator = new SerializedListAccumulator[T]
  }
}

class CollectRowTableSink extends CollectTableSink[Row](new RowTypeInfo(_: _*))
