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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TypeStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a schema of a table.
 *
 * <p>Note: Field names are matched by the exact name by default (case sensitive).
 */
@PublicEvolving
public class Schema implements Descriptor {

	public static final String SCHEMA = "schema";
	public static final String SCHEMA_NAME = "name";
	/**
	 * @deprecated {@link Schema} uses the legacy type key (e.g. schema.0.type = LONG) to store type information in
	 * prior v1.9. Since v1.10, {@link Schema} uses data type key (e.g. schema.0.data-type = BIGINT) to store types.
	 */
	@Deprecated
	public static final String SCHEMA_TYPE = "type";
	public static final String SCHEMA_DATA_TYPE = "data-type";
	public static final String SCHEMA_PROCTIME = "proctime";
	public static final String SCHEMA_FROM = "from";

	// maps a field name to a list of properties that describe type, origin, and the time attribute
	private final Map<String, LinkedHashMap<String, String>> tableSchema = new LinkedHashMap<>();

	private String lastField;

	/**
	 * Sets the schema with field names and the types. Required.
	 *
	 * <p>This method overwrites existing fields added with {@link #field(String, DataType)}.
	 *
	 * @param schema the table schema
	 */
	public Schema schema(TableSchema schema) {
		tableSchema.clear();
		lastField = null;
		for (int i = 0; i < schema.getFieldCount(); i++) {
			field(schema.getFieldName(i).get(), schema.getFieldDataType(i).get());
		}
		return this;
	}

	/**
	 * Adds a field with the field name and the data type. Required.
	 * This method can be called multiple times. The call order of this method defines
	 * also the order of the fields in a row.
	 *
	 * @param fieldName the field name
	 * @param fieldType the type information of the field
	 */
	public Schema field(String fieldName, DataType fieldType) {
		addField(fieldName, fieldType.getLogicalType().asSerializableString());
		return this;
	}


	/**
	 * Adds a field with the field name and the type information. Required.
	 * This method can be called multiple times. The call order of this method defines
	 * also the order of the fields in a row.
	 *
	 * @param fieldName the field name
	 * @param fieldType the type information of the field
	 * @deprecated This method will be removed in future versions as it uses the old type system.
	 * 				Please use {@link #field(String, DataType)} instead.
	 */
	@Deprecated
	public Schema field(String fieldName, TypeInformation<?> fieldType) {
		field(fieldName, TypeConversions.fromLegacyInfoToDataType(fieldType));
		return this;
	}

	/**
	 * Adds a field with the field name and the type string. Required.
	 * This method can be called multiple times. The call order of this method defines
	 * also the order of the fields in a row.
	 *
	 * <p>NOTE: the fieldType string should follow the type string defined in {@link LogicalTypeParser}.
	 * This method also keeps compatible with old type string defined in {@link TypeStringUtils}
	 * but will be dropped in future versions as it uses the old type system.
	 *
	 * @param fieldName the field name
	 * @param fieldType the type string of the field
	 */
	public Schema field(String fieldName, String fieldType) {
		if (isLegacyTypeString(fieldType)) {
			// fallback to legacy parser
			TypeInformation<?> typeInfo = TypeStringUtils.readTypeInfo(fieldType);
			return field(fieldName, TypeConversions.fromLegacyInfoToDataType(typeInfo));
		} else {
			return addField(fieldName, fieldType);
		}
	}

	private Schema addField(String fieldName, String fieldType) {
		if (tableSchema.containsKey(fieldName)) {
			throw new ValidationException("Duplicate field name " + fieldName + ".");
		}

		LinkedHashMap<String, String> fieldProperties = new LinkedHashMap<>();
		fieldProperties.put(SCHEMA_DATA_TYPE, fieldType);

		tableSchema.put(fieldName, fieldProperties);

		lastField = fieldName;
		return this;
	}

	private static boolean isLegacyTypeString(String fieldType) {
		try {
			LogicalType type = LogicalTypeParser.parse(fieldType);
			return type instanceof UnresolvedUserDefinedType;
		} catch (Exception e) {
			// if the parsing failed, fallback to the legacy parser
			return true;
		}
	}

	/**
	 * Specifies the origin of the previously defined field. The origin field is defined by a
	 * connector or format.
	 *
	 * <p>E.g. field("myString", Types.STRING).from("CSV_MY_STRING")
	 *
	 * <p>Note: Field names are matched by the exact name by default (case sensitive).
	 */
	public Schema from(String originFieldName) {
		if (lastField == null) {
			throw new ValidationException("No field previously defined. Use field() before.");
		}
		tableSchema.get(lastField).put(SCHEMA_FROM, originFieldName);
		lastField = null;
		return this;
	}

	/**
	 * Specifies the previously defined field as a processing-time attribute.
	 *
	 * <p>E.g. field("proctime", Types.SQL_TIMESTAMP).proctime()
	 */
	public Schema proctime() {
		if (lastField == null) {
			throw new ValidationException("No field defined previously. Use field() before.");
		}
		tableSchema.get(lastField).put(SCHEMA_PROCTIME, "true");
		lastField = null;
		return this;
	}

	/**
	 * Specifies the previously defined field as an event-time attribute.
	 *
	 * <p>E.g. field("rowtime", Types.SQL_TIMESTAMP).rowtime(...)
	 */
	public Schema rowtime(Rowtime rowtime) {
		if (lastField == null) {
			throw new ValidationException("No field defined previously. Use field() before.");
		}
		tableSchema.get(lastField).putAll(rowtime.toProperties());
		lastField = null;
		return this;
	}

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		List<Map<String, String>> subKeyValues = new ArrayList<>();
		for (Map.Entry<String, LinkedHashMap<String, String>> entry : tableSchema.entrySet()) {
			String name = entry.getKey();
			LinkedHashMap<String, String> props = entry.getValue();
			Map<String, String> map = new HashMap<>();
			map.put(SCHEMA_NAME, name);
			map.putAll(props);
			subKeyValues.add(map);
		}
		properties.putIndexedVariableProperties(
			SCHEMA,
			subKeyValues);
		return properties.asMap();
	}
}
