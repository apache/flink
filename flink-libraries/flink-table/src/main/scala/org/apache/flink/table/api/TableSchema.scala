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
package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType

import _root_.scala.collection.mutable.ArrayBuffer
import _root_.java.util.Objects

import org.apache.flink.table.calcite.FlinkTypeFactory

/**
  * A TableSchema represents a Table's structure.
  */
class TableSchema(
  private val columnNames: Array[String],
  private val columnTypes: Array[TypeInformation[_]]) {

  private val columnNameToIndex: Map[String, Int] = columnNames.zipWithIndex.toMap

  if (columnNames.length != columnTypes.length) {
    throw new TableException(
      s"Number of field names and field types must be equal.\n" +
        s"Number of names is ${columnNames.length}, number of types is ${columnTypes.length}.\n" +
        s"List of field names: ${columnNames.mkString("[", ", ", "]")}.\n" +
        s"List of field types: ${columnTypes.mkString("[", ", ", "]")}.")
  }

  // check uniqueness of field names
  if (columnNames.toSet.size != columnTypes.length) {
    val duplicateFields = columnNames
      // count occurrences of field names
      .groupBy(identity).mapValues(_.length)
      // filter for occurrences > 1 and map to field name
      .filter(g => g._2 > 1).keys

    throw new TableException(
      s"Field names must be unique.\n" +
        s"List of duplicate fields: ${duplicateFields.mkString("[", ", ", "]")}.\n" +
        s"List of all fields: ${columnNames.mkString("[", ", ", "]")}.")
  }

  /**
    * Returns a deep copy of the TableSchema.
    */
  def copy: TableSchema = {
    new TableSchema(columnNames.clone(), columnTypes.clone())
  }

  /**
    * Returns all type information as an array.
    */
  def getTypes: Array[TypeInformation[_]] = columnTypes

  /**
    * Returns the specified type information for the given column index.
    *
    * @param columnIndex the index of the field
    */
  def getType(columnIndex: Int): Option[TypeInformation[_]] = {
    if (columnIndex < 0 || columnIndex >= columnNames.length) {
      None
    } else {
      Some(columnTypes(columnIndex))
    }
  }

  /**
    * Returns the specified type information for the given column name.
    *
    * @param columnName the name of the field
    */
  def getType(columnName: String): Option[TypeInformation[_]] = {
    if (columnNameToIndex.contains(columnName)) {
      Some(columnTypes(columnNameToIndex(columnName)))
    } else {
      None
    }
  }

  /**
    * Returns the number of columns.
    */
  def getColumnCount: Int = columnNames.length

  /**
    * Returns all column names as an array.
    */
  def getColumnNames: Array[String] = columnNames

  /**
    * Returns the specified column name for the given column index.
    *
    * @param columnIndex the index of the field
    */
  def getColumnName(columnIndex: Int): Option[String] = {
    if (columnIndex < 0 || columnIndex >= columnNames.length) {
      None
    } else {
      Some(columnNames(columnIndex))
    }
  }

  /**
    * Converts a table schema into a schema that represents the result that would be written
    * into a table sink or operator outside of the Table & SQL API. Time attributes are replaced
    * by proper TIMESTAMP data types.
    *
    * @return a table schema with no time attributes
    */
  def withoutTimeAttributes: TableSchema = {
    val converted = columnTypes.map { t =>
      if (FlinkTypeFactory.isTimeIndicatorType(t)) {
        Types.SQL_TIMESTAMP
      } else {
        t
      }
    }
    new TableSchema(columnNames, converted)
  }

  override def toString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    columnNames.zip(columnTypes).foreach{ case (name, typeInfo) =>
      builder.append(s" |-- $name: $typeInfo\n")
    }
    builder.toString()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: TableSchema if that.canEqual(this) =>
        this.columnNames.deep == that.columnNames.deep &&
        this.columnTypes.deep == that.columnTypes.deep
      case _ => false
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[TableSchema]

  override def hashCode(): Int = {
    Objects.hash(columnNames, columnTypes)
  }
}

object TableSchema {

  /**
    * Creates a [[TableSchema]] from a [[TypeInformation]].
    * If the [[TypeInformation]] is a [[CompositeType]], the fieldnames and types for the composite
    * type are used to construct the [[TableSchema]].
    * Otherwise, a [[TableSchema]] with a single field is created. The field name is "f0" and the
    * field type the provided type.
    *
    * @param typeInfo The [[TypeInformation]] from which the [[TableSchema]] is generated.
    * @return The [[TableSchema]] that was generated from the given [[TypeInformation]].
    */
  def fromTypeInfo(typeInfo: TypeInformation[_]): TableSchema = {
    typeInfo match {
      case c: CompositeType[_] =>
        // get field names and types from composite type
        val fieldNames = c.getFieldNames
        val fieldTypes = fieldNames.map(c.getTypeAt).asInstanceOf[Array[TypeInformation[_]]]
        new TableSchema(fieldNames, fieldTypes)
      case t: TypeInformation[_] =>
        // create table schema with a single field named "f0" of the given type.
        new TableSchema(Array("f0"), Array(t))
    }
  }

  def builder(): TableSchemaBuilder = {
    new TableSchemaBuilder
  }

}

class TableSchemaBuilder {

  private val fieldNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  private val fieldTypes: ArrayBuffer[TypeInformation[_]] = new ArrayBuffer[TypeInformation[_]]()

  def field(name: String, tpe: TypeInformation[_]): TableSchemaBuilder = {
    fieldNames.append(name)
    fieldTypes.append(tpe)
    this
  }

  def build(): TableSchema = {
    new TableSchema(fieldNames.toArray, fieldTypes.toArray)
  }
}
