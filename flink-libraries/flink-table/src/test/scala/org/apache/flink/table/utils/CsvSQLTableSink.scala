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
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.types.Row

class CsvSQLTableSink(
    path: String,
    fieldTypes: Array[TypeInformation[_]],
    fieldNames: Option[Array[String]],
    fieldDelim: Option[String],
    numFiles: Option[Int],
    writeMode: Option[WriteMode])
  extends CsvTableSink(path, fieldDelim, numFiles, writeMode){

  def this(path: String, fieldTypes: Array[TypeInformation[_]], fieldNames: Array[String],
    fieldDelim: String) {
    this(path, fieldTypes, Some(fieldNames), Some(fieldDelim), None, None)
  }

  def this(path: String, fieldTypes: Array[TypeInformation[_]], fieldDelim: String) {
    this(path, fieldTypes, None, Some(fieldDelim), None, None)
  }

  /** Return the field types of the [[org.apache.calcite.schema.Table]] to emit. */
  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  /**
    * Return the field names of the [[org.apache.calcite.schema.Table]] to emit. */
  override def getFieldNames: Array[String] = fieldNames.getOrElse(super.getFieldNames)

  override def getOutputType: TypeInformation[Row] = {
    new RowTypeInfo(getFieldTypes, getFieldNames)
  }
}
