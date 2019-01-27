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

package org.apache.flink.table.sources.orc

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, InternalType, TypeConverters}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.ColumnarRow
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch
import org.apache.flink.table.typeutils.BaseRowTypeInfo

/**
  * Creates a TableSource to scan an Orc table based
  * [[VectorizedColumnBatch]],
  * and return [[ColumnarRow]].
  */
class OrcVectorizedColumnRowTableSource(
    filePath: Path,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    fieldNullables: Array[Boolean],
    enumerateNestedFiles: Boolean,
    copyToFlink: Boolean = false)
  extends OrcTableSource[ColumnarRow](
    filePath,
    fieldTypes,
    fieldNames,
    fieldNullables,
    enumerateNestedFiles) {

  def this(filePath: Path,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    enumerateNestedFiles: Boolean) = {
    this(
      filePath,
      fieldTypes,
      fieldNames,
      fieldTypes.map(x => !FlinkTypeFactory.isTimeIndicatorType(x)),
      enumerateNestedFiles,
      false
    )
  }

  def this(filePath: Path,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    enumerateNestedFiles: Boolean,
    copyToFlink: Boolean) = {
    this(
      filePath,
      fieldTypes,
      fieldNames,
      fieldTypes.map(x => !FlinkTypeFactory.isTimeIndicatorType(x)),
      enumerateNestedFiles,
      copyToFlink)
  }

  override protected def createTableSource(
      fieldTypes: Array[InternalType],
      fieldNames: Array[String],
      fieldNullables: Array[Boolean]): OrcTableSource[ColumnarRow] = {
    val tableSource = new OrcVectorizedColumnRowTableSource(
      filePath, fieldTypes, fieldNames, fieldNullables, enumerateNestedFiles, copyToFlink)
    tableSource.setFilterPredicate(filterPredicate)
    tableSource.setFilterPushedDown(filterPushedDown)
    tableSource.setSchemaFields(schemaFieldNames)
    tableSource
  }

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[ColumnarRow] = {
    val inputFormat =
      new VectorizedColumnRowInputOrcFormat(filePath, fieldTypes, fieldNames, copyToFlink)
    try
      inputFormat.setFilterPredicate(filterPredicate)
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
    inputFormat.setNestedFileEnumeration(enumerateNestedFiles)
    inputFormat.setSchemaFields(schemaFieldNames)
    streamEnv.createInputV2(inputFormat, getPhysicalType,
      s"OrcVectorizedColumnRowTableSource: ${filePath.getName}")
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[ColumnarRow] = {
    val inputFormat =
      new VectorizedColumnRowInputOrcFormat(filePath, fieldTypes, fieldNames, copyToFlink)
    try
      inputFormat.setFilterPredicate(filterPredicate)
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
    inputFormat.setNestedFileEnumeration(enumerateNestedFiles)
    execEnv.createInputV2(inputFormat, getPhysicalType)
  }

  override def getReturnType: DataType = getPhysicalType

  def getPhysicalType = {
    val typeInfos = this.fieldTypes.map(x => TypeConverters.createExternalTypeInfoFromDataType(x))
    new BaseRowTypeInfo(typeInfos, this.fieldNames).asInstanceOf[TypeInformation[ColumnarRow]]
  }

  override def explainSource(): String = {
    val predicate = if (filterPredicate == null) "" else filterPredicate.toString
    s"OrcVectorizedColumnRowTableSource ->" +
      s"selectedFields=[${fieldNames.mkString(", ")}];" +
        s"filterPredicates=[${predicate}];" +
          s"copyToFlink=[${copyToFlink}];" +
            s"path=[${filePath}]"
  }
}
