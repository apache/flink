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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of JSON-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class JsonRowFormatFactory extends TableFormatFactoryBase<Row>
		implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

	public JsonRowFormatFactory() {
		super(JsonValidator.FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(JsonValidator.FORMAT_JSON_SCHEMA);
		properties.add(JsonValidator.FORMAT_SCHEMA);
		properties.add(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD);
		return properties;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		// create and configure
		final JsonRowDeserializationSchema.Builder schema =
			new JsonRowDeserializationSchema.Builder(createTypeInformation(descriptorProperties));

		descriptorProperties.getOptionalBoolean(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD)
			.ifPresent(flag -> {
				if (flag) {
					schema.failOnMissingField();
				}
			});

		return schema.build();
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		// create and configure
		return new JsonRowSerializationSchema.Builder(createTypeInformation(descriptorProperties)).build();
	}

	private TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
		if (descriptorProperties.containsKey(JsonValidator.FORMAT_SCHEMA)) {
			return (RowTypeInfo) descriptorProperties.getType(JsonValidator.FORMAT_SCHEMA);
		} else if (descriptorProperties.containsKey(JsonValidator.FORMAT_JSON_SCHEMA)) {
			return JsonRowSchemaConverter.convert(descriptorProperties.getString(JsonValidator.FORMAT_JSON_SCHEMA));
		} else {
			return deriveSchema(descriptorProperties.asMap()).toRowType();
		}
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new JsonValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
