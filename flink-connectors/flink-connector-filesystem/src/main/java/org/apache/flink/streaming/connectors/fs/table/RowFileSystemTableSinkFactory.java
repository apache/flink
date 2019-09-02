/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.streaming.connectors.fs.table;

import org.apache.flink.api.common.serialization.SerializationEncoder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.descriptors.BucketValidator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.BucketValidator.CONNECTOR_BASEPATH;
import static org.apache.flink.table.descriptors.BucketValidator.CONNECTOR_DATE_FORMAT;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link RowFileSystemTableSink}.
 */
public class RowFileSystemTableSinkFactory extends FileSystemTableSinkFactoryBase {

	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {

		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);
		final String path = descriptorProperties.getString(CONNECTOR_BASEPATH);

		SerializationSchema<Row> serializationSchema = getSerializationSchema(properties);

		StreamingFileSink.RowFormatBuilder builder = StreamingFileSink.forRowFormat(
			new Path(path),
			new SerializationEncoder<Row>(
				serializationSchema));

		if (descriptorProperties.containsKey(CONNECTOR_DATE_FORMAT)) {
			String dateFormat = descriptorProperties.getString(CONNECTOR_DATE_FORMAT);
			builder = builder.withBucketAssigner(new DateTimeBucketAssigner(dateFormat));
		}

		StreamingFileSink<Row> sink = builder.build();
		return new RowFileSystemTableSink(schema, sink);
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(BucketValidator.CONNECTOR_DATA_TYPE);

		properties.add(CONNECTOR_BASEPATH);
		properties.add(CONNECTOR_TYPE);
		properties.add(CONNECTOR_DATE_FORMAT);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + SCHEMA_FROM);

		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	protected DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = super.getValidatedProperties(properties);
		new SchemaValidator(true, false, false).validate(descriptorProperties);
		return descriptorProperties;
	}

	private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
		@SuppressWarnings("unchecked") final SerializationSchemaFactory<Row> formatFactory = TableFactoryService
			.find(
				SerializationSchemaFactory.class,
				properties,
				this.getClass().getClassLoader());
		return formatFactory.createSerializationSchema(properties);
	}

	@Override
	protected String formatType() {
		return BucketValidator.CONNECTOR_DATA_TYPE_ROW_VALUE;
	}
}
