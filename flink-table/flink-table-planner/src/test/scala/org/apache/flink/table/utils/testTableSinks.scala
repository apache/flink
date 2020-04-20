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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchTableSink, OverwritableTableSink, TableSink}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

/**
  * An [[OverwritableTableSink]] for testing.
  */
final class TestingOverwritableTableSink private (
    path: String,
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]])
  extends AppendStreamTableSink[Row]
  with BatchTableSink[Row]
  with OverwritableTableSink {

  var overwrite = false

  def this (path: String) = {
    this(path, null, null)
  }

  override def setOverwrite(overwrite: Boolean): Unit = {
    this.overwrite = overwrite
  }

  override def consumeDataSet(dataSet: DataSet[Row]): DataSink[_] = {
    val writeMode = if (overwrite) {
      FileSystem.WriteMode.OVERWRITE
    } else {
      FileSystem.WriteMode.NO_OVERWRITE
    }
    dataSet.writeAsText(path, writeMode).setParallelism(1)
  }

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    val writeMode = if (overwrite) {
      FileSystem.WriteMode.OVERWRITE
    } else {
      FileSystem.WriteMode.NO_OVERWRITE
    }
    dataStream.writeAsText(path, writeMode).setParallelism(1)
  }

  override def getConsumedDataType: DataType = getTableSchema.toRowDataType

  def getTableSchema: TableSchema = {
    val dataTypes: Array[DataType] = fieldTypes.map(TypeConversions.fromLegacyInfoToDataType)
    TableSchema.builder().fields(fieldNames, dataTypes).build()
  }

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    if (this.fieldNames != null || this.fieldTypes != null) {
      throw new IllegalStateException(
        "TestingOverwritableTableSink has already been configured field names and field types.")
    }
    new TestingOverwritableTableSink(path, fieldNames, fieldTypes)
  }
}
