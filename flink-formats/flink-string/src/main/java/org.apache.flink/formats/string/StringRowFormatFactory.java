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

package org.apache.flink.formats.string;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.StringValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of String-to-Row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class StringRowFormatFactory implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = validateAndGetProperties(properties);
		String schema = descriptorProperties.getString(StringValidator.FORMAT_SCHEMA);
		boolean failOnNull = descriptorProperties.getOptionalBoolean(StringValidator.FORMAT_FAIL_ON_NULL).orElse(false);
		boolean failOnEmpty = descriptorProperties.getOptionalBoolean(StringValidator.FORMAT_FAIL_ON_EMPTY).orElse(false);
		String encoding = descriptorProperties.getOptionalString(StringValidator.FORMAT_ENCODING).orElse("UTF-8");
		return new StringRowDeserializationSchema(schema, encoding)
			.setFailOnNull(failOnNull)
			.setFailOnEmpty(failOnEmpty);
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = validateAndGetProperties(properties);
		String encoding = descriptorProperties.getOptionalString(StringValidator.FORMAT_ENCODING).orElse("UTF-8");
		return new StringRowSerializationSchema(encoding);
	}

	@Override
	public boolean supportsSchemaDerivation() {
		return true;
	}

	@Override
	public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(FormatDescriptorValidator.FORMAT_TYPE(), StringValidator.FORMAT_TYPE_VALUE);
		context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION(), "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(StringValidator.FORMAT_FAIL_ON_EMPTY);
		properties.add(StringValidator.FORMAT_FAIL_ON_NULL);
		properties.add(StringValidator.FORMAT_ENCODING);
		properties.add(StringValidator.FORMAT_SCHEMA);
		return properties;
	}

	private static DescriptorProperties validateAndGetProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new StringValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
