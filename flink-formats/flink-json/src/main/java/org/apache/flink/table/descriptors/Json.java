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

package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_JSON_SCHEMA;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_SCHEMA;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_TYPE_VALUE;

/**
  * Format descriptor for JSON.
  */
public class Json extends FormatDescriptor {

	private Boolean failOnMissingField;
	private Boolean deriveSchema;
	private String jsonSchema;
	private String schema;

	/**
	  * Format descriptor for JSON.
	  */
	public Json() {
		super(FORMAT_TYPE_VALUE, 1);
	}

	/**
	 * Sets flag whether to fail if a field is missing or not.
	 *
	 * @param failOnMissingField If set to true, the operation fails if there is a missing field.
	 *                           If set to false, a missing field is set to null.
	 */
	public Json failOnMissingField(boolean failOnMissingField) {
		this.failOnMissingField = failOnMissingField;
		return this;
	}

	/**
	 * Sets the JSON schema string with field names and the types according to the JSON schema
	 * specification [[http://json-schema.org/specification.html]].
	 *
	 * <p>The schema might be nested.
	 *
	 * @param jsonSchema JSON schema
	 * @deprecated {@link Json} supports derive schema from table schema by default,
	 *             it is no longer necessary to explicitly declare the format schema.
	 *             This method will be removed in the future.
	 */
	@Deprecated
	public Json jsonSchema(String jsonSchema) {
		Preconditions.checkNotNull(jsonSchema);
		this.jsonSchema = jsonSchema;
		this.schema = null;
		this.deriveSchema = null;
		return this;
	}

	/**
	 * Sets the schema using type information.
	 *
	 * <p>JSON objects are represented as ROW types.
	 *
	 * <p>The schema might be nested.
	 *
	 * @param schemaType type information that describes the schema
	 * @deprecated {@link Json} supports derive schema from table schema by default,
	 *             it is no longer necessary to explicitly declare the format schema.
	 *             This method will be removed in the future.
	 */
	@Deprecated
	public Json schema(TypeInformation<Row> schemaType) {
		Preconditions.checkNotNull(schemaType);
		this.schema = TypeStringUtils.writeTypeInfo(schemaType);
		this.jsonSchema = null;
		this.deriveSchema = null;
		return this;
	}

	/**
	 * Derives the format schema from the table's schema described.
	 *
	 * <p>This allows for defining schema information only once.
	 *
	 * <p>The names, types, and fields' order of the format are determined by the table's
	 * schema. Time attributes are ignored if their origin is not a field. A "from" definition
	 * is interpreted as a field renaming in the format.
	 *
	 * @deprecated Derivation format schema from table's schema is the default behavior now.
	 * 	So there is no need to explicitly declare to derive schema.
	 */
	@Deprecated
	public Json deriveSchema() {
		this.deriveSchema = true;
		this.schema = null;
		this.jsonSchema = null;
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {
		final DescriptorProperties properties = new DescriptorProperties();

		if (deriveSchema != null) {
			properties.putBoolean(FORMAT_DERIVE_SCHEMA, deriveSchema);
		}

		if (jsonSchema != null) {
			properties.putString(FORMAT_JSON_SCHEMA, jsonSchema);
		}

		if (schema != null) {
			properties.putString(FORMAT_SCHEMA, schema);
		}

		if (failOnMissingField != null) {
			properties.putBoolean(FORMAT_FAIL_ON_MISSING_FIELD, failOnMissingField);
		}

		return properties.asMap();
	}
}
