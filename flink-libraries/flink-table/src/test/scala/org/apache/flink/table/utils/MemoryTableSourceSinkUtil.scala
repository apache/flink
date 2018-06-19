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

import java.sql.Timestamp
import java.util
import java.util.Collections

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchTableSink, TableSinkBase}
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps
import org.apache.flink.table.sources._
import org.apache.flink.table.util.TableConnectorUtil
import org.apache.flink.types.Row

import scala.collection.mutable
import scala.collection.JavaConverters._

object MemoryTableSourceSinkUtil {
  var tableData: mutable.ListBuffer[Row] = mutable.ListBuffer[Row]()

  def clear = {
    MemoryTableSourceSinkUtil.tableData.clear()
  }

  class UnsafeMemoryTableSource(tableSchema: TableSchema,
                                returnType: TypeInformation[Row],
                                rowtime: String,
                                val rowCount: Integer)
    extends BatchTableSource[Row]
      with StreamTableSource[Row] with DefinedProctimeAttribute with DefinedRowtimeAttributes {

    override def getReturnType: TypeInformation[Row] = returnType

    override def getTableSchema: TableSchema = tableSchema

    override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
      execEnv.fromCollection(tableData.asJava, returnType)
    }

    final class InMemorySourceFunction(var count: Int = rowCount) extends SourceFunction[Row] {
      override def cancel(): Unit = ???

      override def run(ctx: SourceContext[Row]): Unit = {
        while (count > 0) {
          tableData.synchronized {
            if (tableData.size > 0) {
              val r = tableData.remove(0)
              ctx.collectWithTimestamp(r, r.getField(3).asInstanceOf[Timestamp].getTime)
              count -= 1
            }
          }
        }
      }
    }

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
      execEnv.addSource(new InMemorySourceFunction, returnType)
    }

    override def getProctimeAttribute: String = "proctime"

    override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
      // return a RowtimeAttributeDescriptor if rowtime attribute is defined
      if (rowtime != null) {
        Collections.singletonList(new RowtimeAttributeDescriptor(
          rowtime,
          new StreamRecordTimestamp,
          new AscendingTimestamps))
      } else {
        Collections.EMPTY_LIST.asInstanceOf[util.List[RowtimeAttributeDescriptor]]
      }
    }
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
      tableData.synchronized {
        tableData += Row.copy(value)
      }
    }
  }

  private class MemoryCollectionOutputFormat extends RichOutputFormat[Row] {

    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = {}

    override def writeRecord(record: Row): Unit = {
      tableData.synchronized {
        tableData += Row.copy(record)
      }
    }

    override def close(): Unit = {}
  }
}
