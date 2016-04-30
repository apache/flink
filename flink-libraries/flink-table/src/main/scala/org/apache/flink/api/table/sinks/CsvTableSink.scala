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

package org.apache.flink.api.table.sinks

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream

/**
  * A simple [[TableSink]] to emit data as CSV files.
  *
  * @param path The output path to write the Table to.
  * @param fieldDelim The field delimiter, ',' by default.
  */
class CsvTableSink(
    path: String,
    fieldDelim: String = ",")
  extends BatchTableSink[Row] with StreamTableSink[Row] {

  override def emitDataSet(dataSet: DataSet[Row]): Unit = {
    dataSet
      .map(new CsvFormatter(fieldDelim))
      .writeAsText(path)
  }

  override def emitDataStream(dataStream: DataStream[Row]): Unit = {
    dataStream
      .map(new CsvFormatter(fieldDelim))
      .writeAsText(path)
  }

  override protected def copy: TableSink[Row] = {
    new CsvTableSink(path, fieldDelim)
  }

  override def getOutputType: TypeInformation[Row] = {
    new RowTypeInfo(getFieldTypes)
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
    val v = row.productElement(0)
    if (v != null) {
      builder.append(v.toString)
    }

    // write following values
    for (i <- 1 until row.productArity) {
      builder.append(fieldDelim)
      val v = row.productElement(i)
      if (v != null) {
        builder.append(v.toString)
      }
    }
    builder.mkString
  }
}
