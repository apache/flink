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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.CsvValidator._
import org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE

/**
  * Validator for [[Csv]].
  */
class CsvValidator extends FormatDescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    super.validate(properties)
    properties.validateValue(FORMAT_TYPE, FORMAT_TYPE_VALUE, false)
    properties.validateString(FORMAT_FIELD_DELIMITER, true, 1)
    properties.validateString(FORMAT_LINE_DELIMITER, true, 1)
    properties.validateString(FORMAT_QUOTE_CHARACTER, true, 1, 1)
    properties.validateString(FORMAT_COMMENT_PREFIX, true, 1)
    properties.validateString(FORMAT_ARRAY_ELEMENT_DELIMITER, true, 1)
    properties.validateString(FORMAT_ESCAPE_CHARACTER, true, 1, 1)
    properties.validateString(FORMAT_BYTES_CHARSET, true, 1)
    properties.validateString(FORMAT_NULL_VALUE, true, 1)
    properties.validateBoolean(FORMAT_IGNORE_FIRST_LINE, true)
    properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true)
    properties.validateBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA, true)

    val tableSchema = properties.getOptionalTableSchema(FORMAT_FIELDS)
    val derived = properties.getOptionalBoolean(
      FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA).orElse(false)
    if (derived && tableSchema.isPresent) {
      throw new ValidationException(
        "Format cannot define a schema and derive from the table's schema at the same time.")
    } else if (tableSchema.isPresent) {
      properties.validateTableSchema(FORMAT_FIELDS, false)
    } else if (!tableSchema.isPresent && derived) {
      throw new ValidationException(
        "A definition of a schema or derive from the table's schema is required.")
    }
  }
}

object CsvValidator {

  val FORMAT_TYPE_VALUE = "csv"
  val FORMAT_FIELD_DELIMITER = "format.field-delimiter"
  val FORMAT_LINE_DELIMITER = "format.line-delimiter"
  val FORMAT_QUOTE_CHARACTER = "format.quote-character"
  val FORMAT_COMMENT_PREFIX = "format.comment-prefix"
  val FORMAT_IGNORE_FIRST_LINE = "format.ignore-first-line"
  val FORMAT_IGNORE_PARSE_ERRORS = "format.ignore-parse-errors"
  val FORMAT_FIELDS = "format.fields"
  val FORMAT_ARRAY_ELEMENT_DELIMITER = "format.array-element-delimiter"
  val FORMAT_ESCAPE_CHARACTER = "format.escape-character"
  val FORMAT_BYTES_CHARSET = "format.bytes-charset"
  val FORMAT_NULL_VALUE = "format.null-value"
}
