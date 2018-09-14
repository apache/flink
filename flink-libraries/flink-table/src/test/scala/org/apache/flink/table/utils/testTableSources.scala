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

import java.util
import java.util.{Collections, List => JList}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, PreserveWatermarks}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class TestTableSourceWithTime[T](
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[T],
    rowtime: String = null,
    proctime: String = null,
    mapping: Map[String, String] = null)
  extends StreamTableSource[T]
    with BatchTableSource[T]
    with DefinedRowtimeAttributes
    with DefinedProctimeAttribute
    with DefinedFieldMapping {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {
    execEnv.fromCollection(values.asJava, returnType)
  }

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[T] = {
    execEnv.fromCollection(values.asJava, returnType)
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    // return a RowtimeAttributeDescriptor if rowtime attribute is defined
    if (rowtime != null) {
      Collections.singletonList(new RowtimeAttributeDescriptor(
        rowtime,
        new ExistingField(rowtime),
        new AscendingTimestamps))
    } else {
      Collections.EMPTY_LIST.asInstanceOf[util.List[RowtimeAttributeDescriptor]]
    }
  }

  override def getProctimeAttribute: String = proctime

  override def getReturnType: TypeInformation[T] = returnType

  override def getTableSchema: TableSchema = tableSchema

  override def getFieldMapping: util.Map[String, String] = {
    if (mapping != null) {
      mapping.asJava
    } else {
      null
    }
  }
}

class TestProjectableTableSource(
    tableSchema: TableSchema,
    returnType: TypeInformation[Row],
    values: Seq[Row],
    rowtime: String = null,
    proctime: String = null,
    fieldMapping: Map[String, String] = null)
  extends TestTableSourceWithTime[Row](
    tableSchema,
    returnType,
    values,
    rowtime,
    proctime,
    fieldMapping)
  with ProjectableTableSource[Row] {

  override def projectFields(fields: Array[Int]): TableSource[Row] = {

    val rowType = returnType.asInstanceOf[RowTypeInfo]

    val (projectedNames: Array[String], projectedMapping) = if (fieldMapping == null) {
      val projectedNames = fields.map(rowType.getFieldNames.apply(_))
      (projectedNames, null)
    } else {
      val invertedMapping = fieldMapping.map(_.swap)
      val projectedNames = fields.map(rowType.getFieldNames.apply(_))

      val projectedMapping: Map[String, String] = projectedNames.map{ f =>
        val logField = invertedMapping(f)
        logField -> s"remapped-$f"
      }.toMap
      val renamedNames = projectedNames.map(f => s"remapped-$f")
      (renamedNames, projectedMapping)
    }

    val projectedTypes = fields.map(rowType.getFieldTypes.apply(_))
    val projectedReturnType = new RowTypeInfo(
      projectedTypes.asInstanceOf[Array[TypeInformation[_]]],
      projectedNames)

    val projectedValues = values.map { fromRow =>
      val pRow = new Row(fields.length)
      fields.zipWithIndex.foreach{ case (from, to) => pRow.setField(to, fromRow.getField(from)) }
      pRow
    }

    new TestProjectableTableSource(
      tableSchema,
      projectedReturnType,
      projectedValues,
      rowtime,
      proctime,
      projectedMapping)
  }

  override def explainSource(): String = {
    s"TestSource(" +
      s"physical fields: ${getReturnType.asInstanceOf[RowTypeInfo].getFieldNames.mkString(", ")})"
  }
}

class TestNestedProjectableTableSource(
    tableSchema: TableSchema,
    returnType: TypeInformation[Row],
    values: Seq[Row],
    rowtime: String = null,
    proctime: String = null)
  extends TestTableSourceWithTime[Row](
    tableSchema,
    returnType,
    values,
    rowtime,
    proctime,
    null)
  with NestedFieldsProjectableTableSource[Row] {

  var readNestedFields: Seq[String] = tableSchema.getColumnNames.map(f => s"$f.*")

  override def projectNestedFields(
      fields: Array[Int],
      nestedFields: Array[Array[String]]): TableSource[Row] = {

    val rowType = returnType.asInstanceOf[RowTypeInfo]

    val projectedNames = fields.map(rowType.getFieldNames.apply(_))
    val projectedTypes = fields.map(rowType.getFieldTypes.apply(_))

    val projectedReturnType = new RowTypeInfo(
      projectedTypes.asInstanceOf[Array[TypeInformation[_]]],
      projectedNames)

    // update read nested fields
    val newReadNestedFields = projectedNames.zip(nestedFields)
      .flatMap(f => f._2.map(n => s"${f._1}.$n"))

    val projectedValues = values.map { fromRow =>
      val pRow = new Row(fields.length)
      fields.zipWithIndex.foreach{ case (from, to) => pRow.setField(to, fromRow.getField(from)) }
      pRow
    }

    val copy = new TestNestedProjectableTableSource(
      tableSchema,
      projectedReturnType,
      projectedValues,
      rowtime,
      proctime)
    copy.readNestedFields = newReadNestedFields

    copy
  }

  override def explainSource(): String = {
    s"TestSource(" +
      s"read nested fields: ${readNestedFields.mkString(", ")})"
  }
}

class TestPreserveWMTableSource[T](
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[Either[(Long, T), Long]],
    rowtime: String)
  extends StreamTableSource[T]
    with DefinedRowtimeAttributes {

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    Collections.singletonList(new RowtimeAttributeDescriptor(
      rowtime,
      new ExistingField(rowtime),
      PreserveWatermarks.INSTANCE))
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {
    execEnv.addSource(new EventTimeSourceFunction[T](values)).setParallelism(1).returns(returnType)
  }

  override def getReturnType: TypeInformation[T] = returnType

  override def getTableSchema: TableSchema = tableSchema

}
