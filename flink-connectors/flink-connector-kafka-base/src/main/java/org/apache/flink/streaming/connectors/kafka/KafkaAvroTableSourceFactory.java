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

import org.apache.flink.formats.avro.typeutils.AvroRecordClassConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.AvroValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.SchemaValidator;

import org.apache.avro.specific.SpecificRecordBase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating configured instances of {@link KafkaAvroTableSource}.
 */
public abstract class KafkaAvroTableSourceFactory extends KafkaTableSourceFactory {

	@Override
	protected String formatType() {
		return AvroValidator.FORMAT_TYPE_VALUE;
	}

	@Override
	protected int formatPropertyVersion() {
		return 1;
	}

	@Override
	protected List<String> formatProperties() {
		return Collections.singletonList(AvroValidator.FORMAT_RECORD_CLASS);
	}

	@Override
	protected FormatDescriptorValidator formatValidator() {
		return new AvroValidator();
	}

	@Override
	protected KafkaTableSource.Builder createBuilderWithFormat(DescriptorProperties params) {
		KafkaAvroTableSource.Builder builder = createKafkaAvroBuilder();

		// Avro format schema
		final Class<? extends SpecificRecordBase> avroRecordClass =
				params.getClass(AvroValidator.FORMAT_RECORD_CLASS, SpecificRecordBase.class);
		builder.forAvroRecordClass(avroRecordClass);
		final TableSchema formatSchema = TableSchema.fromTypeInfo(AvroRecordClassConverter.convert(avroRecordClass));

		// field mapping
		final Map<String, String> mapping = SchemaValidator.deriveFieldMapping(params, Optional.of(formatSchema));
		builder.withTableToAvroMapping(mapping);

		return builder;
	}

	/**
	 * Creates a version specific {@link KafkaAvroTableSource.Builder}.
	 */
	protected abstract KafkaAvroTableSource.Builder createKafkaAvroBuilder();
}
