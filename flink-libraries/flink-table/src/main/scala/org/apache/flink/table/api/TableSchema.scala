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

/**
  * A TableSchema represents a Table's structure.
  */
class TableSchema(
  private val columnNames: Array[String],
  private val columnTypes: Array[TypeInformation[_]]) {

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
      // count occurences of field names
      .groupBy(identity).mapValues(_.length)
      // filter for occurences > 1 and map to field name
      .filter(g => g._2 > 1).keys

    throw new TableException(
      s"Field names must be unique.\n" +
        s"List of duplicate fields: ${duplicateFields.mkString("[", ", ", "]")}.\n" +
        s"List of all fields: ${columnNames.mkString("[", ", ", "]")}.")
  }

  val columnNameToIndex: Map[String, Int] = columnNames.zipWithIndex.toMap

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

  override def toString = {
    val builder = new StringBuilder
    builder.append("root\n")
    columnNames.zip(columnTypes).foreach{ case (name, typeInfo) =>
      builder.append(s" |-- $name: $typeInfo\n")
    }
    builder.toString()
  }

}
