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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.CsvValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Table format for providing configured instances of CSV-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public class CsvRowFormatFactory implements SerializationSchemaFactory<Row>,
	DeserializationSchemaFactory<Row>  {

	@Override
	public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(FormatDescriptorValidator.FORMAT_TYPE(), CsvValidator.FORMAT_TYPE_VALUE());
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
		properties.add(CsvValidator.FORMAT_FIELDS() + ".#." + DescriptorProperties.TYPE());
		properties.add(CsvValidator.FORMAT_FIELDS() + ".#." + DescriptorProperties.NAME());
		properties.add(CsvValidator.FORMAT_FIELD_DELIMITER());
		properties.add(CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER());
		properties.add(CsvValidator.FORMAT_QUOTE_CHARACTER());
		properties.add(CsvValidator.FORMAT_ESCAPE_CHARACTER());
		properties.add(CsvValidator.FORMAT_BYTES_CHARSET());
		return properties;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = validateAndGetProperties(properties);

		final CsvRowDeserializationSchema schema = new CsvRowDeserializationSchema(
			createTypeInformation(descriptorProperties));

		// update csv schema with properties
		descriptorProperties.getOptionalString(CsvValidator.FORMAT_FIELD_DELIMITER())
			.ifPresent(schema::setFieldDelimiter);
		descriptorProperties.getOptionalString(CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER())
			.ifPresent(schema::setArrayElementDelimiter);
		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_QUOTE_CHARACTER())
			.ifPresent(schema::setQuoteCharacter);
		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_ESCAPE_CHARACTER())
			.ifPresent(schema::setEscapeCharacter);
		descriptorProperties.getOptionalString(CsvValidator.FORMAT_BYTES_CHARSET())
			.ifPresent(schema::setCharset);

		return new CsvRowDeserializationSchema(createTypeInformation(descriptorProperties));
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = validateAndGetProperties(properties);

		final CsvRowSerializationSchema schema = new CsvRowSerializationSchema(
			createTypeInformation(descriptorProperties));

		// update csv schema with properties
		descriptorProperties.getOptionalString(CsvValidator.FORMAT_FIELD_DELIMITER())
			.ifPresent(schema::setFieldDelimiter);
		descriptorProperties.getOptionalString(CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER())
			.ifPresent(schema::setArrayElementDelimiter);
		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_QUOTE_CHARACTER())
			.ifPresent(schema::setQuoteCharacter);
		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_ESCAPE_CHARACTER())
			.ifPresent(schema::setEscapeCharacter);
		descriptorProperties.getOptionalString(CsvValidator.FORMAT_BYTES_CHARSET())
			.ifPresent(schema::setCharset);

		return new CsvRowSerializationSchema(createTypeInformation(descriptorProperties));
	}

	private static DescriptorProperties validateAndGetProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new CsvValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	/**
	 * Create a {@link TypeInformation} based on the "format-fields" in {@link CsvValidator}.
	 * @param descriptorProperties
	 * @return {@link TypeInformation}
	 */
	private static TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
		return descriptorProperties.getTableSchema(CsvValidator.FORMAT_FIELDS()).toRowType();
	}
}
