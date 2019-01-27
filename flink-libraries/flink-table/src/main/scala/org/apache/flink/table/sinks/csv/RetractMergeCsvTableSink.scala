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

import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.api.java.io.{AbstractCsvOutputFormat, RowCsvOutputFormat}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.sinks.{RetractStreamTableSink, TableSinkBase}
import org.apache.flink.types.Row

import java.lang.{Boolean => JBool}
import java.util.TimeZone
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

/**
  * A simple [[org.apache.flink.table.sinks.TableSink]] to emit Retract data after merge
  * as CSV files.
  *
  * @param path The output path to write the Table to.
  * @param fieldDelim The field delimiter
  * @param recordDelim The record delimiter
  * @param quoteCharacter The quote character
  * @param numFiles The number of files to write to
  * @param writeMode The write mode to specify whether existing files are overwritten or not.
  * @param outputFieldNames Whether output field names.
  */
class RetractMergeCsvTableSink(
    path: String,
    fieldDelim: Option[String],
    recordDelim: Option[String],
    quoteCharacter: Option[String],
    numFiles: Option[Int],
    writeMode: Option[WriteMode],
    outputFieldNames: Option[Boolean],
    timezone: Option[TimeZone])
  extends TableSinkBase[JTuple2[JBool, Row]]
  with RetractStreamTableSink[Row] {

  var sinkIndex: Int = RowCollector.createContainer

  def this(path: String, fieldDelim: String = ",") {
    this(path, Some(fieldDelim), None, None, None, None, None, None)
  }

  def this(
    path: String, fieldDelim: String, recordDelim: String, quoteCharacter: String) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter),
      None, None, None, None)
  }

  def this(path: String, fieldDelim: String, numFiles: Int, writeMode: WriteMode) {
    this(path, Some(fieldDelim), None, None, Some(numFiles), Some(writeMode), None, None)
  }

  def this(
    path: String,
    fieldDelim: String, recordDelim: String, quoteCharacter: String,
    numFiles: Int, writeMode: WriteMode) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter),
      Some(numFiles), Some(writeMode), None, None)
  }

  def this(
    path: String, fieldDelim: String,
    numFiles: Int, writeMode: WriteMode,
    outputFieldNames: Boolean, timezone: TimeZone) {
    this(path, Some(fieldDelim), None, None,
      Some(numFiles), Some(writeMode),
      Some(outputFieldNames), Option(timezone))
  }

  def this(
    path: String,
    fieldDelim: String, recordDelim: String, quoteCharacter: String,
    numFiles: Int, writeMode: WriteMode,
    outputFieldNames: Boolean, timezone: TimeZone) {
    this(path, Some(fieldDelim), Some(recordDelim), Option(quoteCharacter),
      Some(numFiles), Some(writeMode),
      Some(outputFieldNames), Option(timezone))
  }

  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    new RetractMergeCsvTableSink(path, fieldDelim, recordDelim, quoteCharacter, numFiles,
      writeMode, outputFieldNames, timezone)
  }

  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes: _*)

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]])= {
    val sink = dataStream.addSink(new RetractMergeRowSinkFunction(
      path,
      fieldDelim.getOrElse(AbstractCsvOutputFormat.DEFAULT_FIELD_DELIMITER),
      recordDelim.getOrElse(AbstractCsvOutputFormat.DEFAULT_LINE_DELIMITER),
      quoteCharacter.orNull,
      writeMode.getOrElse(FileSystem.WriteMode.OVERWRITE),
      sinkIndex,
      numFiles.getOrElse(1),
      outputFieldNames.getOrElse(false),
      getFieldNames,
      timezone.getOrElse(TimeZone.getTimeZone("UTC"))))
    .name("RetractMergeCsvTableSink: " + path)

    if (numFiles.isDefined) {
      sink.setParallelism(numFiles.get)
    }
    sink
  }
}

class RetractMergeRowSinkFunction(
    path: String,
    fieldDelim: String,
    recordDelim: String,
    quoteCharacter: String,
    writeMode: WriteMode,
    sinkIndex: Integer,
    numFiles: Int,
    outputFieldNames: Boolean,
    fieldNames: Array[String],
    timezone: TimeZone)
  extends RichSinkFunction[JTuple2[JBool, Row]] {
  var outputFormat: RowCsvOutputFormat = _

  override def open(parameters: Configuration): Unit = {
    outputFormat = new RowCsvOutputFormat(new Path(path), recordDelim, fieldDelim, quoteCharacter)
    outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE)
    outputFormat.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY)
    outputFormat.setAllowNullValues(true)
    outputFormat.setTimezone(timezone)
    outputFormat.open(getRuntimeContext.getIndexOfThisSubtask, numFiles)
    RowCollector.synchronized {
      if (!RowCollector.sink.contains(sinkIndex)) {
        RowCollector.sink.put(sinkIndex, new mutable.ArrayBuffer[JTuple2[JBool, Row]]())
      }
    }
    super.open(parameters)
  }

  override def invoke(value: JTuple2[JBool, Row]): Unit = {
    RowCollector.sink(sinkIndex).synchronized {
      if (value.f0) {
        RowCollector.sink(sinkIndex) += new JTuple2[JBool, Row](value.f0, Row.copy(value.f1))
      } else {
        val prevValue = new JTuple2(true, value.f1)
        val index = RowCollector.sink(sinkIndex).indexOf(prevValue)
        if (index > -1) {
          RowCollector.sink(sinkIndex).remove(index)
        } else {
          throw new RuntimeException("Tried to retract a value that wasn't added first. " +
            "This is probably an incorrectly implemented test. " +
            "Try to set the parallelism of the sink to 1.")
        }
      }
    }
  }

  override def close(): Unit = {
    val out: List[JTuple2[JBool, Row]] = RowCollector.sink(sinkIndex).toList
    if (outputFieldNames) {
      outputFormat.setOutputFieldName(true)
      outputFormat.setFieldNames(fieldNames)
    }

    // write records
    var cnt = 0
    for (i <- out) {
      outputFormat.writeRecord(i.f1)
    }

    // clean
    RowCollector.sink(sinkIndex).clear()
    RowCollector.nameCntr.set(0)
    outputFormat.close()
  }
}

object RowCollector {
  val nameCntr: AtomicInteger = new AtomicInteger(0)
  val sink = mutable.Map[Integer, mutable.ArrayBuffer[JTuple2[JBool, Row]]]()
  def createContainer: Int = {
    val index = nameCntr.getAndIncrement()
    RowCollector.sink.put(index, new mutable.ArrayBuffer[JTuple2[JBool, Row]]())
    index
  }
}
