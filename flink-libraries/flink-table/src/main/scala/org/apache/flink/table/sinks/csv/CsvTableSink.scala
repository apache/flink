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

package org.apache.flink.table.sinks.csv

import java.util.TimeZone

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.io.AbstractCsvOutputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{DataType, RowType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sinks._

/**
  * A simple [[TableSink]] to emit data as CSV files.
  *
  * @param path The output path to write the Table to.
  * @param fieldDelim The field delimiter
  * @param numFiles The number of files to write to
  * @param writeMode The write mode to specify whether existing files are overwritten or not.
  */
class CsvTableSink(
    path: String,
    fieldDelim: Option[String],
    recordDelim: Option[String],
    quoteCharacter: Option[String],
    numFiles: Option[Int],
    writeMode: Option[WriteMode],
    outputFieldNames: Option[Boolean],
    timezone: Option[TimeZone])
  extends TableSinkBase[BaseRow]
    with BatchTableSink[BaseRow] // Use for CsvBatchTableSinkFactory.
    with BatchCompatibleStreamTableSink[BaseRow] // Used for o.a.f.t.factories.csv.CsvTableFactory.
    with AppendStreamTableSink[BaseRow] {

  private val name = "csv sink: " + path

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter, ',' by default.
    */
  def this(path: String, fieldDelim: String = ",") {
    this(path, Some(fieldDelim), None, None, None, None, None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param recordDelim The record delimiter.
    * @param quoteCharacter The quote character.
    */
  def this(
      path: String, fieldDelim: String, recordDelim: String, quoteCharacter: String) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter), None, None, None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    */
  def this(path: String, fieldDelim: String, numFiles: Int, writeMode: WriteMode) {
    this(path, Some(fieldDelim), None, None, Some(numFiles), Some(writeMode), None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param recordDelim The record delimiter.
    * @param quoteCharacter The quote character.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    */
  def this(
      path: String,
      fieldDelim: String, recordDelim: String, quoteCharacter: String,
      numFiles: Int, writeMode: WriteMode) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter),
      Some(numFiles), Some(writeMode), None, None)
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    * @param outputFieldNames Whether to output field names.
    */
  def this(
      path: String,
      fieldDelim: String,
      numFiles: Int,
      writeMode: WriteMode,
      outputFieldNames: Boolean,
      timezone: TimeZone) {
    this(path, Some(fieldDelim), None, None,
      Some(numFiles), Some(writeMode), Some(outputFieldNames), Option(timezone))
  }

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter.
    * @param recordDelim The record delimiter.
    * @param quoteCharacter The quote character.
    * @param numFiles The number of files to write to.
    * @param writeMode The write mode to specify whether existing files are overwritten or not.
    * @param outputFieldNames Whether to output field names.
    */
  def this(
      path: String,
      fieldDelim: String, recordDelim: String, quoteCharacter: String,
      numFiles: Int, writeMode: WriteMode, outputFieldNames: Boolean, timezone: TimeZone) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter),
      Some(numFiles), Some(writeMode), Some(outputFieldNames), Option(timezone))
  }

  def newOutputFormat(): BaseRowCsvOutputFormat = {
    val outputFormat = new BaseRowCsvOutputFormat(
      new Path(path), getFieldTypes.map(_.toInternalType))
    outputFormat.setFieldDelimiter(fieldDelim.getOrElse(
      AbstractCsvOutputFormat.DEFAULT_FIELD_DELIMITER))
    outputFormat.setRecordDelimiter(recordDelim.getOrElse(
      AbstractCsvOutputFormat.DEFAULT_LINE_DELIMITER))
    outputFormat.setQuoteCharacter(quoteCharacter.orNull)
    outputFormat.setAllowNullValues(true)
    outputFormat.setTimezone(timezone.getOrElse(TimeZone.getTimeZone("UTC")))
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

  override def emitDataStream(dataStream: DataStream[BaseRow]): DataStreamSink[_] = {
    val sink = dataStream.addSink(new OutputFormatSinkFunction(newOutputFormat()))
    sink.name(name)
    if (numFiles.isDefined) {
      sink.setParallelism(numFiles.get)
    } else {
      sink.setParallelism(dataStream.getParallelism)
    }
  }

  override def emitBoundedStream(boundedStream: DataStream[BaseRow],
    tableConfig: TableConfig,
    executionConfig: ExecutionConfig): DataStreamSink[_] = {
    emitBoundedStream(boundedStream)
  }

  /** Emits the DataStream. */
  override def emitBoundedStream(boundedStream: DataStream[BaseRow]): DataStreamSink[BaseRow] = {
    val outputFormat = newOutputFormat()
    numFiles match {
      case None => boundedStream.writeUsingOutputFormat(outputFormat).name(name)
      case Some(fileNum) =>
        val ret = boundedStream.writeUsingOutputFormat(outputFormat)
          .name(name).setParallelism(fileNum)
        ret.getTransformation.setMaxParallelism(fileNum)
        ret
    }
  }

  override protected def copy: TableSinkBase[BaseRow] = {
    new CsvTableSink(
      path, fieldDelim, recordDelim, quoteCharacter, numFiles, writeMode, outputFieldNames,
      timezone)
  }

  override def getOutputType: DataType = {
    new RowType(getFieldTypes: _*)
  }
}
