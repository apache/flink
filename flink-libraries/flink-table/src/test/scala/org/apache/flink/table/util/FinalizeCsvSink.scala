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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{RowType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sinks.csv.BaseRowCsvOutputFormat
import org.apache.flink.table.sinks.{BatchTableSink, TableSinkBase}

class FinalizeCsvSink(
    path: String,
    fieldDelim: Option[String],
    numFiles: Option[Int],
    writeMode: Option[WriteMode],
    outputFieldNames: Option[Boolean],
    markPath: Option[String])
  extends TableSinkBase[BaseRow]
  with BatchTableSink[BaseRow] {

  private val name = "csv sink: " + path

  def this(
      path: String,
      fieldDelim: String = ",",
      markPath: String = "/tmp/finalized") {
    this(path, Some(fieldDelim), None, None, None, Some(markPath))
  }

  def newOutputFormat(): BaseRowCsvOutputFormat = {
    val outputFormat = new FinalizedTestCsvFormat(
      new Path(path), getFieldTypes.map(_.toInternalType), markPath.getOrElse("/tmp/finalized"))
    outputFormat.setFieldDelimiter(fieldDelim.getOrElse(","))
    outputFormat.setAllowNullValues(true)
    writeMode match {
      case None => Unit
      case Some(vm) => outputFormat.setWriteMode(vm)
    }
    outputFieldNames match {
      case None => Unit
      case Some(b) =>
        outputFormat.setOutputFieldName(b)
        outputFormat.setFieldNames(getFieldNames)
    }

    outputFormat
  }

  /** Emits the BoundedStream. */
  override def emitBoundedStream(
      boundedStream: DataStream[BaseRow],
      tableConfig: TableConfig,
      executionConfig: ExecutionConfig): DataStreamSink[BaseRow] = {
    val outputFormat = newOutputFormat()
    numFiles match {
      case None => boundedStream.writeUsingOutputFormat(outputFormat)
        .name(name)
      case Some(fileNum) =>
        val ret = boundedStream.writeUsingOutputFormat(outputFormat)
        .name(name).setParallelism(fileNum)
        ret.getTransformation.setMaxParallelism(fileNum)
        ret
    }
  }

  override protected def copy: TableSinkBase[BaseRow] = {
    new FinalizeCsvSink(path, fieldDelim, numFiles, writeMode, outputFieldNames, markPath)
  }

  override def getOutputType: RowType = {
    new RowType(getFieldTypes: _*)
  }

}
