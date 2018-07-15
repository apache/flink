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

import org.apache.flink.table.descriptors.CsvValidator._
import org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE

/**
  * Validator for [[Csv]].
  */
class CsvValidator extends FormatDescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    super.validate(properties)
    properties.validateValue(FORMAT_TYPE, FORMAT_TYPE_VALUE, isOptional = false)
    properties.validateString(FORMAT_FIELD_DELIMITER, isOptional = true, minLen = 1)
    properties.validateString(FORMAT_LINE_DELIMITER, isOptional = true, minLen = 1)
    properties.validateString(FORMAT_QUOTE_CHARACTER, isOptional = true, minLen = 1, maxLen = 1)
    properties.validateString(FORMAT_COMMENT_PREFIX, isOptional = true, minLen = 1)
    properties.validateBoolean(FORMAT_IGNORE_FIRST_LINE, isOptional = true)
    properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, isOptional = true)
    properties.validateTableSchema(FORMAT_FIELDS, isOptional = false)
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
}
