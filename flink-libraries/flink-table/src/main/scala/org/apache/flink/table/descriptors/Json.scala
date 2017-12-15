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

import org.apache.flink.table.descriptors.JsonValidator.{FORMAT_FAIL_ON_MISSING_FIELD, FORMAT_SCHEMA_STRING, FORMAT_TYPE_VALUE}

/**
  * Encoding descriptor for JSON.
  */
class Json extends FormatDescriptor(FORMAT_TYPE_VALUE, version = 1) {

  private var failOnMissingField: Option[Boolean] = None

  private var schema: Option[String] = None

  /**
    * Sets flag whether to fail if a field is missing or not.
    *
    * @param failOnMissingField If set to true, the operation fails if there is a missing field.
    *                           If set to false, a missing field is set to null.
    * @return The builder.
    */
  def failOnMissingField(failOnMissingField: Boolean): Json = {
    this.failOnMissingField = Some(failOnMissingField)
    this
  }

  /**
    * Sets the JSON schema string with field names and the types according to the JSON schema
    * specification [[http://json-schema.org/specification.html]]. Required.
    *
    * The schema might be nested.
    *
    * @param schema JSON schema
    */
  def schema(schema: String): Json = {
    this.schema = Some(schema)
    this
  }

  /**
    * Internal method for format properties conversion.
    */
  override protected def addFormatProperties(properties: DescriptorProperties): Unit = {
    // we distinguish between "schema string" and "schema" to allow parsing of a
    // schema object in the future (such that the entire JSON schema can be defined in a YAML
    // file instead of one large string)
    schema.foreach(properties.putString(FORMAT_SCHEMA_STRING, _))
    failOnMissingField.foreach(properties.putBoolean(FORMAT_FAIL_ON_MISSING_FIELD, _))
  }
}

/**
  * Encoding descriptor for JSON.
  */
object Json {

  /**
    * Encoding descriptor for JSON.
    */
  def apply(): Json = new Json()
}
