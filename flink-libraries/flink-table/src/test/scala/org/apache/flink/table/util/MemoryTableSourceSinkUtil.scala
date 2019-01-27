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

package org.apache.flink.table.util

import java.util
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.table.api.types.{DataType, DataTypes, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableSchema}
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchTableSink, TableSinkBase}
import org.apache.flink.table.sources._
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

object MemoryTableSourceSinkUtil {
  val tableData: mutable.ListBuffer[Row] = mutable.ListBuffer[Row]()
  var results: mutable.MutableList[String] = mutable.MutableList.empty[String]

  def clear(): Unit = {
    MemoryTableSourceSinkUtil.results.clear()
  }

  class UnsafeMemoryTableSource(
    tableSchema: TableSchema,
    returnType: DataType,
    rowtimeAttributeDescriptor: util.List[RowtimeAttributeDescriptor],
    proctime: String,
    val terminationCount: Int)
    extends BatchTableSource[Row]
      with StreamTableSource[Row]
      with DefinedProctimeAttribute
      with DefinedRowtimeAttributes {

    override def getReturnType: DataType = returnType

    override def getTableSchema: TableSchema = tableSchema

    override def getBoundedStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
      execEnv.fromCollection(tableData.asJava,
        TypeConverters.createExternalTypeInfoFromDataType(returnType)
            .asInstanceOf[TypeInformation[Row]])
    }

    final class InMemorySourceFunction(var count: Int = terminationCount)
      extends SourceFunction[Row] {

      override def cancel(): Unit = throw new UnsupportedOperationException()

      override def run(ctx: SourceContext[Row]): Unit = {
        while (count > 0) {
          tableData.synchronized {
            if (tableData.nonEmpty) {
              val r = tableData.remove(0)
              ctx.collect(r)
              count -= 1
            }
          }
        }
      }
    }

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
      execEnv.addSource(new InMemorySourceFunction,
        TypeConverters.createExternalTypeInfoFromDataType(returnType)
            .asInstanceOf[TypeInformation[Row]])
    }

    override def getProctimeAttribute: String = proctime

    override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
      rowtimeAttributeDescriptor
    }
  }

  final class UnsafeMemoryAppendTableSink
    extends TableSinkBase[Row]
    with BatchTableSink[Row]
    with AppendStreamTableSink[Row]
    with DefinedDistribution {

    override def getOutputType: DataType = {
      DataTypes.createRowType(getFieldTypes, getFieldNames)
    }

    override protected def copy: TableSinkBase[Row] = {
      val sink = new UnsafeMemoryAppendTableSink
      sink.pk = this.pk
      sink
    }

    override def emitDataStream(dataStream: DataStream[Row]): DataStreamSink[Row] = {
      dataStream.addSink(new MemoryAppendSink)
        .setParallelism(dataStream.getParallelism)
        .name(TableConnectorUtil.generateRuntimeName(this.getClass, getFieldNames))
    }

    private var pk: String = null

    def setPartitionedField(pk: String): Unit = {
      this.pk = pk
    }

    override def getPartitionField(): String = pk

    override def shuffleEmptyKey(): Boolean = false

    /** Emits the BoundedStream. */
    override def emitBoundedStream(
      boundedStream: DataStream[Row],
      tableConfig: TableConfig,
      executionConfig: ExecutionConfig): DataStreamSink[Row] = {
      val ret = boundedStream.addSink(new MemoryAppendSink)
        .name(TableConnectorUtil.generateRuntimeName(this.getClass, getFieldNames))
      ret.setParallelism(1)
      ret.getTransformation.setMaxParallelism(1)
      ret
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
