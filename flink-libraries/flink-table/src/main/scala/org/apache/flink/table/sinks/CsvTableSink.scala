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

package org.apache.flink.table.sinks

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.types.Row
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.util.TableConnectorUtil

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
    numFiles: Option[Int],
    writeMode: Option[WriteMode])
  extends TableSinkBase[Row] with BatchTableSink[Row] with AppendStreamTableSink[Row] {

  /**
    * A simple [[TableSink]] to emit data as CSV files.
    *
    * @param path The output path to write the Table to.
    * @param fieldDelim The field delimiter, ',' by default.
    */
  def this(path: String, fieldDelim: String = ",") {
    this(path, Some(fieldDelim), None, None)
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
    this(path, Some(fieldDelim), Some(numFiles), Some(writeMode))
  }

  override def emitDataSet(dataSet: DataSet[Row]): Unit = {
    val csvRows = dataSet.map(new CsvFormatter(fieldDelim.getOrElse(",")))

    if (numFiles.isDefined) {
      csvRows.setParallelism(numFiles.get)
    }

    val sink = writeMode match {
      case None => csvRows.writeAsText(path)
      case Some(wm) => csvRows.writeAsText(path, wm)
    }

    if (numFiles.isDefined) {
      sink.setParallelism(numFiles.get)
    }

    sink.name(TableConnectorUtil.generateRuntimeName(this.getClass, getFieldNames))
  }

  override def emitDataStream(dataStream: DataStream[Row]): Unit = {
    val csvRows = dataStream.map(new CsvFormatter(fieldDelim.getOrElse(",")))

    if (numFiles.isDefined) {
      csvRows.setParallelism(numFiles.get)
    }

    val sink = writeMode match {
      case None => csvRows.writeAsText(path)
      case Some(wm) => csvRows.writeAsText(path, wm)
    }

    if (numFiles.isDefined) {
      sink.setParallelism(numFiles.get)
    }

    sink.name(TableConnectorUtil.generateRuntimeName(this.getClass, getFieldNames))
  }

  override protected def copy: TableSinkBase[Row] = {
    new CsvTableSink(path, fieldDelim, numFiles, writeMode)
  }

  override def getOutputType: TypeInformation[Row] = {
    new RowTypeInfo(getFieldTypes: _*)
  }
}

/**
  * Formats a [[Row]] into a [[String]] with fields separated by the field delimiter.
  *
  * @param fieldDelim The field delimiter.
  */
class CsvFormatter(fieldDelim: String) extends MapFunction[Row, String] {
  override def map(row: Row): String = {

    val builder = new StringBuilder

    // write first value
    val v = row.getField(0)
    if (v != null) {
      builder.append(v.toString)
    }

    // write following values
    for (i <- 1 until row.getArity) {
      builder.append(fieldDelim)
      val v = row.getField(i)
      if (v != null) {
        builder.append(v.toString)
      }
    }
    builder.mkString
  }
}
