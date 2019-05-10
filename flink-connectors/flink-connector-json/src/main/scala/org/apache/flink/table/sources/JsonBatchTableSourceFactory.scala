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

package org.apache.flink.table.sources

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.formats.json.JsonRowSchemaConverter
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors._
import org.apache.flink.table.descriptors.FileSystemValidator.{CONNECTOR_PATH, CONNECTOR_TYPE_VALUE}
import org.apache.flink.table.descriptors.FormatDescriptorValidator.{FORMAT_PROPERTY_VERSION, FORMAT_TYPE}
import org.apache.flink.table.factories.BatchTableSourceFactory
import org.apache.flink.types.Row

/**
  * Factory for creating configured instances of [[JsonBatchTableSource]] in a batch environment.
  */
class JsonBatchTableSourceFactory extends BatchTableSourceFactory[Row] {

  val SCHEMA = "schema"
  val FORMAT_TYPE_VALUE = "json"
  val FORMAT_FIELDS = "format.fields"

  /**
    * Creates and configures a [[org.apache.flink.table.sources.BatchTableSource]]
    * using the given properties.
    *
    * @param properties normalized properties describing a batch table source.
    * @return the configured batch table source.
    */
  override def createBatchTableSource(properties: util.Map[String, String])
    : BatchTableSource[Row] = {
      val params = new DescriptorProperties()
      params.putProperties(properties)

      // validate
      new FileSystemValidator().validate(params)
      new JsonValidator().validate(params)

      var fieldNames : Array[String] = null
      var fieldTypes : Array[TypeInformation[_]] = null
      if (params.containsKey(JsonValidator.FORMAT_SCHEMA)) {
        val rowTypeInfo : RowTypeInfo = params.getType(JsonValidator.FORMAT_SCHEMA)
          .asInstanceOf[RowTypeInfo]
        fieldNames = rowTypeInfo.getFieldNames
        fieldTypes = rowTypeInfo.getFieldTypes
      } else if (params.containsKey(JsonValidator.FORMAT_JSON_SCHEMA)) {
        val rowTypeInfo : RowTypeInfo = JsonRowSchemaConverter.convert(
          params.getString(JsonValidator.FORMAT_JSON_SCHEMA)).asInstanceOf[RowTypeInfo]
        fieldNames = rowTypeInfo.getFieldNames
        fieldTypes = rowTypeInfo.getFieldTypes
      } else {
        val tableSchema = params.getTableSchema(SCHEMA)
        fieldNames = tableSchema.getFieldNames
        fieldTypes = tableSchema.getFieldTypes
      }

      val path = params.getString(CONNECTOR_PATH)
      if (path == null) throw new IllegalArgumentException("Path must be defined.")

      new JsonBatchTableSource(path, fieldNames, fieldTypes)
  }

  /**
    * Specifies the context that this factory has been implemented for. The framework guarantees to
    * only match for this factory if the specified set of properties and values are met.
    *
    * <p>Typical properties might be:
    *   - connector.type
    *   - format.type
    *
    * <p>Specified property versions allow the framework to provide backwards compatible properties
    * in case of string format changes:
    *   - connector.property-version
    *   - format.property-version
    *
    * <p>An empty context means that the factory matches for all requests.
    */
  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE)
    context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE)
    context.put(CONNECTOR_PROPERTY_VERSION, "1")
    context.put(FORMAT_PROPERTY_VERSION, "1")
    context
  }

  /**
    * List of property keys that this factory can handle. This method will be used for validation.
    * If a property is passed that this factory cannot handle, an exception will be thrown. The
    * list must not contain the keys that are specified by the context.
    *
    * <p>Example properties might be:
    *   - schema.#.type
    *   - schema.#.name
    *   - connector.path
    *   - format.schema
    *   - format.json-schema
    *   - format.fail-on-missing-field
    *   - format.fields.#.type
    *   - format.fields.#.name
    *
    * <p>Note: Use "#" to denote an array of values where "#" represents one or more digits.
    * Property versions like "format.property-version"
    * must not be part of the supported properties.
    *
    * <p>In some cases it might be useful to declare wildcards "*". Wildcards
    * can only be declared at the end of a property key.
    *
    * <p>For example, if an arbitrary format should be supported:
    *   - format.*
    *
    * <p>Note: Wildcards should be used with caution as they might swallow unsupported properties
    * and thus might lead to undesired behavior.
    */
  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    // connector
    properties.add(CONNECTOR_PATH)
    // format
    properties.add(s"$FORMAT_FIELDS.#.${DescriptorProperties.TABLE_SCHEMA_TYPE}")
    properties.add(s"$FORMAT_FIELDS.#.${DescriptorProperties.TABLE_SCHEMA_NAME}")
    properties.add(JsonValidator.FORMAT_SCHEMA)
    properties.add(JsonValidator.FORMAT_JSON_SCHEMA)
    properties.add(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD)
    properties.add(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA)
    properties.add(CONNECTOR_PATH)
    // schema
    properties.add(s"$SCHEMA.#.${DescriptorProperties.TABLE_SCHEMA_TYPE}")
    properties.add(s"$SCHEMA.#.${DescriptorProperties.TABLE_SCHEMA_NAME}")
    properties
  }
}
