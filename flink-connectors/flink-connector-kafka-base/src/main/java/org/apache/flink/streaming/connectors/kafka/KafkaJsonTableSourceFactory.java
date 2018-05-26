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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.descriptors.SchemaValidator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating configured instances of {@link KafkaJsonTableSource}.
 */
public abstract class KafkaJsonTableSourceFactory extends KafkaTableSourceFactory {

	@Override
	protected String formatType() {
		return JsonValidator.FORMAT_TYPE_VALUE;
	}

	@Override
	protected int formatPropertyVersion() {
		return 1;
	}

	@Override
	protected List<String> formatProperties() {
		return Arrays.asList(
			JsonValidator.FORMAT_JSON_SCHEMA,
			JsonValidator.FORMAT_SCHEMA,
			JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD,
			FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA());
	}

	@Override
	protected FormatDescriptorValidator formatValidator() {
		return new JsonValidator();
	}

	@Override
	protected KafkaTableSource.Builder createBuilderWithFormat(DescriptorProperties params) {
		final KafkaJsonTableSource.Builder builder = createKafkaJsonBuilder();

		// missing field
		params.getOptionalBoolean(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD)
				.ifPresent(builder::failOnMissingField);

		// json schema
		final TableSchema formatSchema;
		if (params.containsKey(JsonValidator.FORMAT_SCHEMA)) {
			final TypeInformation<?> info = params.getType(JsonValidator.FORMAT_SCHEMA);
			formatSchema = TableSchema.fromTypeInfo(info);
		} else if (params.containsKey(JsonValidator.FORMAT_JSON_SCHEMA)) {
			final TypeInformation<?> info = JsonSchemaConverter.convert(params.getString(JsonValidator.FORMAT_JSON_SCHEMA));
			formatSchema = TableSchema.fromTypeInfo(info);
		} else {
			formatSchema = SchemaValidator.deriveFormatFields(params);
		}
		builder.forJsonSchema(formatSchema);

		// field mapping
		final Map<String, String> mapping = SchemaValidator.deriveFieldMapping(params, Optional.of(formatSchema));
		builder.withTableToJsonMapping(mapping);

		return builder;
	}

	/**
	 * Creates a version specific {@link KafkaJsonTableSource.Builder}.
	 */
	protected abstract KafkaJsonTableSource.Builder createKafkaJsonBuilder();

}
