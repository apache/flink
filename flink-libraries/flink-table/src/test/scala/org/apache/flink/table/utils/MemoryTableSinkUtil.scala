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

package org.apache.flink.table.utils

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchTableSink, TableSinkBase}
import org.apache.flink.table.util.TableConnectorUtil
import org.apache.flink.types.Row

import scala.collection.mutable

object MemoryTableSinkUtil {
  var results: mutable.MutableList[String] = mutable.MutableList.empty[String]

  def clear = {
    MemoryTableSinkUtil.results.clear()
  }

  final class UnsafeMemoryAppendTableSink
    extends TableSinkBase[Row] with BatchTableSink[Row]
    with AppendStreamTableSink[Row] {

    override def getOutputType: TypeInformation[Row] = {
      new RowTypeInfo(getFieldTypes, getFieldNames)
    }

    override protected def copy: TableSinkBase[Row] = {
      new UnsafeMemoryAppendTableSink
    }

    override def emitDataSet(dataSet: DataSet[Row]): Unit = {
      dataSet
        .output(new MemoryCollectionOutputFormat)
        .name(TableConnectorUtil.generateRuntimeName(this.getClass, getFieldNames))
    }

    override def emitDataStream(dataStream: DataStream[Row]): Unit = {
      dataStream
        .addSink(new MemoryAppendSink)
        .name(TableConnectorUtil.generateRuntimeName(this.getClass, getFieldNames))
    }
  }

  private class MemoryAppendSink extends RichSinkFunction[Row]() {

    override def invoke(value: Row): Unit = {
      results.synchronized {
        results += value.toString
      }
    }
  }

  private class MemoryCollectionOutputFormat extends RichOutputFormat[Row] {

    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = {}

    override def writeRecord(record: Row): Unit = {
      results.synchronized {
        results += record.toString
      }
    }

    override def close(): Unit = {}
  }
}
