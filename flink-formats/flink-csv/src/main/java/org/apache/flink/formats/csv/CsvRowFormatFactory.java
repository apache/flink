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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.CsvValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of CSV-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public final class CsvRowFormatFactory extends TableFormatFactoryBase<Row>
	implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row>  {

	public CsvRowFormatFactory() {
		super(CsvValidator.FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	public List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(CsvValidator.FORMAT_FIELD_DELIMITER);
		properties.add(CsvValidator.FORMAT_LINE_DELIMITER);
		properties.add(CsvValidator.FORMAT_DISABLE_QUOTE_CHARACTER);
		properties.add(CsvValidator.FORMAT_QUOTE_CHARACTER);
		properties.add(CsvValidator.FORMAT_ALLOW_COMMENTS);
		properties.add(CsvValidator.FORMAT_IGNORE_PARSE_ERRORS);
		properties.add(CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER);
		properties.add(CsvValidator.FORMAT_ESCAPE_CHARACTER);
		properties.add(CsvValidator.FORMAT_NULL_LITERAL);
		properties.add(CsvValidator.FORMAT_SCHEMA);
		return properties;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final CsvRowDeserializationSchema.Builder schemaBuilder = new CsvRowDeserializationSchema.Builder(
			createTypeInformation(descriptorProperties));

		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_FIELD_DELIMITER)
			.ifPresent(schemaBuilder::setFieldDelimiter);

		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_QUOTE_CHARACTER)
			.ifPresent(schemaBuilder::setQuoteCharacter);

		descriptorProperties.getOptionalBoolean(CsvValidator.FORMAT_ALLOW_COMMENTS)
			.ifPresent(schemaBuilder::setAllowComments);

		descriptorProperties.getOptionalBoolean(CsvValidator.FORMAT_IGNORE_PARSE_ERRORS)
			.ifPresent(schemaBuilder::setIgnoreParseErrors);

		descriptorProperties.getOptionalString(CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER)
			.ifPresent(schemaBuilder::setArrayElementDelimiter);

		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_ESCAPE_CHARACTER)
			.ifPresent(schemaBuilder::setEscapeCharacter);

		descriptorProperties.getOptionalString(CsvValidator.FORMAT_NULL_LITERAL)
			.ifPresent(schemaBuilder::setNullLiteral);

		return schemaBuilder.build();
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final CsvRowSerializationSchema.Builder schemaBuilder = new CsvRowSerializationSchema.Builder(
			createTypeInformation(descriptorProperties));

		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_FIELD_DELIMITER)
			.ifPresent(schemaBuilder::setFieldDelimiter);

		descriptorProperties.getOptionalString(CsvValidator.FORMAT_LINE_DELIMITER)
			.ifPresent(schemaBuilder::setLineDelimiter);

		if (descriptorProperties.getOptionalBoolean(CsvValidator.FORMAT_DISABLE_QUOTE_CHARACTER).orElse(false)) {
			schemaBuilder.disableQuoteCharacter();
		} else {
			descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_QUOTE_CHARACTER).ifPresent(schemaBuilder::setQuoteCharacter);
		}

		descriptorProperties.getOptionalString(CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER)
			.ifPresent(schemaBuilder::setArrayElementDelimiter);

		descriptorProperties.getOptionalCharacter(CsvValidator.FORMAT_ESCAPE_CHARACTER)
			.ifPresent(schemaBuilder::setEscapeCharacter);

		descriptorProperties.getOptionalString(CsvValidator.FORMAT_NULL_LITERAL)
			.ifPresent(schemaBuilder::setNullLiteral);

		return schemaBuilder.build();
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(propertiesMap);

		new CsvValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private static TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
		if (descriptorProperties.containsKey(CsvValidator.FORMAT_SCHEMA)) {
			return (RowTypeInfo) descriptorProperties.getType(CsvValidator.FORMAT_SCHEMA);
		} else {
			return deriveSchema(descriptorProperties.asMap()).toRowType();
		}
	}
}
