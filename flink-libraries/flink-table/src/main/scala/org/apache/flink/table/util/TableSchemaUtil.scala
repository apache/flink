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

import org.apache.flink.table.api.types.{RowType, DataType, DataTypes, TimestampType}
import org.apache.flink.table.api.{Column, TableSchema}
import org.apache.flink.table.calcite.FlinkTypeFactory

/**
  * Utils for TableSchema
  */
object TableSchemaUtil {

  /**
    * Converts a table schema into a (nested) type information describing a Row.
    *
    * @param tableSchema
    * @return type information where columns are fields of a Row.
    */
  def toRowType(tableSchema: TableSchema): DataType = {
    DataTypes.createRowType(tableSchema.getFieldTypes.toArray[DataType], tableSchema.getFieldNames)
  }

  /**
    * Converts a table schema into a schema that represents the result that would be written
    * into a table sink or operator outside of the Table & SQL API. Time attributes are replaced
    * by proper TIMESTAMP data types.
    *
    * @return a table schema with no time attributes
    */
  def withoutTimeAttributes(tableSchema: TableSchema): TableSchema = {
    val columns = tableSchema.getColumns
    val converted = columns.map { t =>
      if (FlinkTypeFactory.isTimeIndicatorType(t.internalType)) {
        new Column(t.name(), TimestampType.TIMESTAMP, false)
      } else {
        t
      }
    }
    new TableSchema(converted)
  }

  /**
    * Create table schema from a data type.
    */
  def fromDataType(
    dataType: DataType,
    fieldNullables: Option[Array[Boolean]] = None): TableSchema = {
    dataType.toInternalType match {
      case bt: RowType =>
        val fieldNames = bt.getFieldNames
        val fieldTypes = bt.getFieldTypes.map(_.toInternalType)
        if (fieldNullables.isDefined) {
          new TableSchema(fieldNames, fieldTypes, fieldNullables.get)
        } else {
          new TableSchema(fieldNames, fieldTypes)
        }
      case t =>
        val fieldNames = Array("f0")
        val fieldTypes = Array(t)
        if (fieldNullables.isDefined) {
          new TableSchema(fieldNames, fieldTypes, fieldNullables.get)
        } else {
          new TableSchema(fieldNames, fieldTypes)
        }
    }
  }

  /**
    * Create table schema builder from a data type.
    */
  def builderFromDataType(dataType: DataType): TableSchema.Builder = {
    val tableSchema = fromDataType(dataType, None)
    val builder = new TableSchema.Builder()
    tableSchema.getColumns.foreach(c => builder.field(c.name, c.internalType, c.isNullable))
    builder
  }
}
