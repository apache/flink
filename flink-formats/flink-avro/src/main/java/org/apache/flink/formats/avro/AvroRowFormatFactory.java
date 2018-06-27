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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.descriptors.AvroValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.formats.DeserializationSchemaFactory;
import org.apache.flink.table.formats.SerializationSchemaFactory;
import org.apache.flink.types.Row;

import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of Avro-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class AvroRowFormatFactory implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(FormatDescriptorValidator.FORMAT_TYPE(), AvroValidator.FORMAT_TYPE_VALUE);
		context.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION(), "1");
		return context;
	}

	@Override
	public boolean supportsSchemaDerivation() {
		return false;
	}

	@Override
	public List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(AvroValidator.FORMAT_RECORD_CLASS);
		properties.add(AvroValidator.FORMAT_AVRO_SCHEMA);
		return properties;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties props = new DescriptorProperties(true);
		props.putProperties(properties);

		// validate
		new AvroValidator().validate(props);

		// create and configure
		if (props.containsKey(AvroValidator.FORMAT_RECORD_CLASS)) {
			return new AvroRowDeserializationSchema(
				props.getClass(AvroValidator.FORMAT_RECORD_CLASS, SpecificRecord.class));
		} else {
			return new AvroRowDeserializationSchema(props.getString(AvroValidator.FORMAT_AVRO_SCHEMA));
		}
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties props = new DescriptorProperties(true);
		props.putProperties(properties);

		// validate
		new AvroValidator().validate(props);

		// create and configure
		if (props.containsKey(AvroValidator.FORMAT_RECORD_CLASS)) {
			return new AvroRowSerializationSchema(
				props.getClass(AvroValidator.FORMAT_RECORD_CLASS, SpecificRecord.class));
		} else {
			return new AvroRowSerializationSchema(props.getString(AvroValidator.FORMAT_AVRO_SCHEMA));
		}
	}
}
