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

package org.apache.flink.table.sources.parquet

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.types.{DataType, InternalType, RowType, TypeConverters}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.ColumnarRow
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch

import _root_.java.util.{Set => JSet}

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.JavaConversions._

/**
  * Creates a TableSource to scan an Parquet table based
  * [[VectorizedColumnBatch]],
  * and return [[ColumnarRow]].
  */
class ParquetVectorizedColumnRowTableSource(
    filePath: Path,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    fieldNullables: Array[Boolean],
    enumerateNestedFiles: Boolean,
    numTimes: Int = 1,
    sourceName: String = "",
    uniqueKeySet: JSet[JSet[String]] = null)
  extends ParquetTableSource[ColumnarRow](
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
      fieldTypes.map(!FlinkTypeFactory.isTimeIndicatorType(_)),
      enumerateNestedFiles)
  }

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[ColumnarRow] = {
    val inputFormat = new VectorizedColumnRowInputParquetFormat(
      filePath, fieldTypes, fieldNames, limit)
    try
      inputFormat.setFilterPredicate(filterPredicate)
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
    inputFormat.setNestedFileEnumeration(enumerateNestedFiles)
    streamEnv.createInputV2(inputFormat, getPhysicalType,
      s"ParquetVectorizedColumnRowTableSource: ${filePath.getName}")
  }

  override def getReturnType: RowType = new RowType(
    this.fieldTypes.toArray[DataType], this.fieldNames)

  def getPhysicalType: TypeInformation[ColumnarRow] =
    TypeConverters.toBaseRowTypeInfo(getReturnType)
        .asInstanceOf[TypeInformation[ColumnarRow]]

  override protected def createTableSource(
      fieldTypes: Array[InternalType],
      fieldNames: Array[String],
      fieldNullables: Array[Boolean]): ParquetTableSource[ColumnarRow] = {
    val newUniqueKeys = if (uniqueKeySet != null) {
      uniqueKeySet.filter(_.forall(fieldNames.contains)).asJava
    } else {
      null
    }
    val tableSource = new ParquetVectorizedColumnRowTableSource(
      filePath, fieldTypes, fieldNames, fieldNullables, enumerateNestedFiles, numTimes,
      sourceName, newUniqueKeys)
    tableSource.setFilterPredicate(filterPredicate)
    tableSource.setFilterPushedDown(filterPushedDown)
    tableSource.setLimit(limit)
    tableSource.setLimitPushedDown(limitPushedDown)
    tableSource
  }

  override def explainSource(): String = {
    val limitStr = if (isLimitPushedDown && limit < Long.MaxValue) s"; limit=$limit" else ""
    val predicate = if (filterPredicate == null) "" else filterPredicate.toString
    s"ParquetVectorizedColumnRowTableSource -> " +
      s"selectedFields=[${fieldNames.mkString(", ")}];" +
      s"filterPredicates=[$predicate]$limitStr"
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[ColumnarRow] = {
    val inputFormat = new VectorizedColumnRowInputParquetFormat(
      filePath, fieldTypes, fieldNames, limit)
    try
      inputFormat.setFilterPredicate(filterPredicate)
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
    inputFormat.setNestedFileEnumeration(enumerateNestedFiles)
    execEnv.createInputV2(inputFormat, getPhysicalType, sourceName).setParallelism(numTimes)
  }

  override def getTableSchema: TableSchema = {
    val builder = TableSchema.builder()
    fieldNames.zip(fieldTypes).zip(fieldNullables).foreach {
      case ((name:String, tpe:InternalType), nullable:Boolean) =>
        builder.field(name, tpe.asInstanceOf[InternalType], nullable)
    }
    if (uniqueKeySet != null) {
      uniqueKeySet.foreach {
        case uniqueKey: JSet[String] =>
          builder.uniqueIndex(uniqueKey.toArray(new Array[String](0)):_*)
      }
    }
    builder.build()
  }

}
