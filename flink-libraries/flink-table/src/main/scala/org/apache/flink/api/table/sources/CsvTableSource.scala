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

package org.apache.flink.api.table.sources

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.{Row, TableException}
import org.apache.flink.api.table.runtime.io.RowCsvInputFormat
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * A [[BatchTableSource]] and [[StreamTableSource]] for simple CSV files with a
  * (logically) unlimited number of fields.
  *
  * @param path The path to the CSV file.
  * @param fieldNames The names of the table fields.
  * @param fieldTypes The types of the table fields.
  * @param fieldDelim The field delimiter, "," by default.
  * @param rowDelim The row delimiter, "\n" by default.
  * @param quoteCharacter An optional quote character for String values, null by default.
  * @param ignoreFirstLine Flag to ignore the first line, false by default.
  * @param ignoreComments An optional prefix to indicate comments, null by default.
  * @param lenient Flag to skip records with parse error instead to fail, false by default.
  */
class CsvTableSource(
    path: String,
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    fieldDelim: String = CsvInputFormat.DEFAULT_FIELD_DELIMITER,
    rowDelim: String = CsvInputFormat.DEFAULT_LINE_DELIMITER,
    quoteCharacter: Character = null,
    ignoreFirstLine: Boolean = false,
    ignoreComments: String = null,
    lenient: Boolean = false)
  extends BatchTableSource[Row]
  with StreamTableSource[Row] {

  /**
  * A [[BatchTableSource]] and [[StreamTableSource]] for simple CSV files with a
  * (logically) unlimited number of fields.
  *
  * @param path The path to the CSV file.
  * @param fieldNames The names of the table fields.
  * @param fieldTypes The types of the table fields.
  */
  def this(path: String, fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]) =
    this(path, fieldNames, fieldTypes, CsvInputFormat.DEFAULT_FIELD_DELIMITER,
      CsvInputFormat.DEFAULT_LINE_DELIMITER, null, false, null, false)

  if (fieldNames.length != fieldTypes.length) {
    throw TableException("Number of field names and field types must be equal.")
  }

  private val returnType = new RowTypeInfo(fieldTypes)

  /** Returns the data of the table as a [[DataSet]] of [[Row]]. */
  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.createInput(createCsvInput(), returnType)
  }

  /** Returns the types of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  /** Returns the names of the table fields. */
  override def getFieldsNames: Array[String] = fieldNames

  /** Returns the number of fields of the table. */
  override def getNumberOfFields: Int = fieldNames.length

  /** Returns the [[RowTypeInfo]] for the return type of the [[CsvTableSource]]. */
  override def getReturnType: RowTypeInfo = returnType

  /** Returns the data of the table as a [[DataStream]] of [[Row]]. */
  override def getDataStream(streamExecEnv: StreamExecutionEnvironment): DataStream[Row] = {
    streamExecEnv.createInput(createCsvInput(), returnType)
  }

  private def createCsvInput(): RowCsvInputFormat = {
    val inputFormat = new RowCsvInputFormat(new Path(path), returnType, rowDelim, fieldDelim)

    inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine)
    inputFormat.setLenient(lenient)
    if (quoteCharacter != null) {
      inputFormat.enableQuotedStringParsing(quoteCharacter)
    }
    if (ignoreComments != null) {
      inputFormat.setCommentPrefix(ignoreComments)
    }

    inputFormat
  }
}
