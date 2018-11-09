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
import org.apache.flink.table.descriptors.CsvValidator._
import org.apache.flink.table.utils.TypeStringUtils

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Format descriptor for comma-separated values (CSV).
  */
class Csv extends FormatDescriptor(FORMAT_TYPE_VALUE, 1) {

  private var fieldDelim: Option[String] = None
  private var lineDelim: Option[String] = None
  private val schema: mutable.LinkedHashMap[String, String] =
    mutable.LinkedHashMap[String, String]()
  private var quoteCharacter: Option[Character] = None
  private var commentPrefix: Option[String] = None
  private var isIgnoreFirstLine: Option[Boolean] = None
  private var lenient: Option[Boolean] = None

  /**
    * Sets the field delimiter, "," by default.
    *
    * @param delim the field delimiter
    */
  def fieldDelimiter(delim: String): Csv = {
    this.fieldDelim = Some(delim)
    this
  }

  /**
    * Sets the line delimiter, "\n" by default.
    *
    * @param delim the line delimiter
    */
  def lineDelimiter(delim: String): Csv = {
    this.lineDelim = Some(delim)
    this
  }

  /**
    * Sets the format schema with field names and the types. Required.
    * The table schema must not contain nested fields.
    *
    * This method overwrites existing fields added with [[field()]].
    *
    * @param schema the table schema
    */
  def schema(schema: TableSchema): Csv = {
    this.schema.clear()
    schema.getFieldNames.zip(schema.getFieldTypes).foreach { case (n, t) =>
      field(n, t)
    }
    this
  }

  /**
    * Adds a format field with the field name and the type information. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in the format.
    *
    * @param fieldName the field name
    * @param fieldType the type information of the field
    */
  def field(fieldName: String, fieldType: TypeInformation[_]): Csv = {
    field(fieldName, TypeStringUtils.writeTypeInfo(fieldType))
    this
  }

  /**
    * Adds a format field with the field name and the type string. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in the format.
    *
    * @param fieldName the field name
    * @param fieldType the type string of the field
    */
  def field(fieldName: String, fieldType: String): Csv = {
    if (schema.contains(fieldName)) {
      throw new ValidationException(s"Duplicate field name $fieldName.")
    }
    schema += (fieldName -> fieldType)
    this
  }

  /**
    * Sets a quote character for String values, null by default.
    *
    * @param quote the quote character
    */
  def quoteCharacter(quote: Character): Csv = {
    this.quoteCharacter = Option(quote)
    this
  }

  /**
    * Sets a prefix to indicate comments, null by default.
    *
    * @param prefix the prefix to indicate comments
    */
  def commentPrefix(prefix: String): Csv = {
    this.commentPrefix = Option(prefix)
    this
  }

  /**
    * Ignore the first line. Not skip the first line by default.
    */
  def ignoreFirstLine(): Csv = {
    this.isIgnoreFirstLine = Some(true)
    this
  }

  /**
    * Skip records with parse error instead to fail. Throw an exception by default.
    */
  def ignoreParseErrors(): Csv = {
    this.lenient = Some(true)
    this
  }

  override protected def toFormatProperties: util.Map[String, String] = {
    val properties = new DescriptorProperties()

    fieldDelim.foreach(properties.putString(FORMAT_FIELD_DELIMITER, _))
    lineDelim.foreach(properties.putString(FORMAT_LINE_DELIMITER, _))

    val subKeys = util.Arrays.asList(
      DescriptorProperties.TABLE_SCHEMA_NAME,
      DescriptorProperties.TABLE_SCHEMA_TYPE)

    val subValues = schema.map(e => util.Arrays.asList(e._1, e._2)).toList.asJava

    properties.putIndexedFixedProperties(
      FORMAT_FIELDS,
      subKeys,
      subValues)
    quoteCharacter.foreach(properties.putCharacter(FORMAT_QUOTE_CHARACTER, _))
    commentPrefix.foreach(properties.putString(FORMAT_COMMENT_PREFIX, _))
    isIgnoreFirstLine.foreach(properties.putBoolean(FORMAT_IGNORE_FIRST_LINE, _))
    lenient.foreach(properties.putBoolean(FORMAT_IGNORE_PARSE_ERRORS, _))

    properties.asMap()
  }
}

/**
  * Format descriptor for comma-separated values (CSV).
  */
object Csv {

  /**
    * Format descriptor for comma-separated values (CSV).
    *
    * @deprecated Use `new Csv()`.
    */
  @deprecated
  def apply(): Csv = new Csv()

}
