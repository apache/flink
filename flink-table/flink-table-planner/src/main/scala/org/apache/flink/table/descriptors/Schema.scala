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

package org.apache.flink.table.descriptors

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableSchema, ValidationException}
import org.apache.flink.table.descriptors.SchemaValidator._
import org.apache.flink.table.utils.TypeStringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Describes a schema of a table.
  *
  * Note: Field names are matched by the exact name by default (case sensitive).
  */
class Schema extends Descriptor {

  // maps a field name to a list of properties that describe type, origin, and the time attribute
  private val tableSchema = mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, String]]()

  private var lastField: Option[String] = None

  /**
    * Sets the schema with field names and the types. Required.
    *
    * This method overwrites existing fields added with [[field()]].
    *
    * @param schema the table schema
    */
  def schema(schema: TableSchema): Schema = {
    tableSchema.clear()
    lastField = None
    schema.getFieldNames.zip(schema.getFieldTypes).foreach { case (n, t) =>
      field(n, t)
    }
    this
  }

  /**
    * Adds a field with the field name and the type information. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in a row.
    *
    * @param fieldName the field name
    * @param fieldType the type information of the field
    */
  def field(fieldName: String, fieldType: TypeInformation[_]): Schema = {
    field(fieldName, TypeStringUtils.writeTypeInfo(fieldType))
    this
  }

  /**
    * Adds a field with the field name and the type string. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in a row.
    *
    * @param fieldName the field name
    * @param fieldType the type string of the field
    */
  def field(fieldName: String, fieldType: String): Schema = {
    if (tableSchema.contains(fieldName)) {
      throw new ValidationException(s"Duplicate field name $fieldName.")
    }

    val fieldProperties = mutable.LinkedHashMap[String, String]()
    fieldProperties += (SCHEMA_TYPE -> fieldType)

    tableSchema += (fieldName -> fieldProperties)

    lastField = Some(fieldName)
    this
  }

  /**
    * Specifies the origin of the previously defined field. The origin field is defined by a
    * connector or format.
    *
    * E.g. field("myString", Types.STRING).from("CSV_MY_STRING")
    *
    * Note: Field names are matched by the exact name by default (case sensitive).
    */
  def from(originFieldName: String): Schema = {
    lastField match {
      case None => throw new ValidationException("No field previously defined. Use field() before.")
      case Some(f) =>
        tableSchema(f) += (SCHEMA_FROM -> originFieldName)
        lastField = None
    }
    this
  }

  /**
    * Specifies the previously defined field as a processing-time attribute.
    *
    * E.g. field("proctime", Types.SQL_TIMESTAMP).proctime()
    */
  def proctime(): Schema = {
    lastField match {
      case None => throw new ValidationException("No field defined previously. Use field() before.")
      case Some(f) =>
        tableSchema(f) += (SCHEMA_PROCTIME -> "true")
        lastField = None
    }
    this
  }

  /**
    * Specifies the previously defined field as an event-time attribute.
    *
    * E.g. field("rowtime", Types.SQL_TIMESTAMP).rowtime(...)
    */
  def rowtime(rowtime: Rowtime): Schema = {
    lastField match {
      case None => throw new ValidationException("No field defined previously. Use field() before.")
      case Some(f) =>
        tableSchema(f) ++= rowtime.toProperties.asScala
        lastField = None
    }
    this
  }

  /**
    * Converts this descriptor into a set of properties.
    */
  final override def toProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()
    properties.putIndexedVariableProperties(
      SCHEMA,
      tableSchema.toSeq.map { case (name, props) =>
        (Map(SCHEMA_NAME -> name) ++ props).asJava
      }.asJava
    )
    properties.asMap()
  }
}

/**
  * Describes a schema of a table.
  */
object Schema {

  /**
    * Describes a schema of a table.
    *
    * @deprecated Use `new Schema()`.
    */
  @deprecated
  def apply(): Schema = new Schema()
}
