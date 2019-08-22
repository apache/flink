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

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.AvroValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.StreamTableSink;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

import static org.apache.flink.streaming.connectors.fs.table.descriptors.BucketValidator.CONNECTOR_BASEPATH;
import static org.apache.flink.streaming.connectors.fs.table.descriptors.BucketValidator.CONNECTOR_DATE_FORMAT;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;

/**
 * Factory for creating configured instances of AvroFileSystemTableSink .
 */
public class AvroFileSystemTableSinkFactory extends BultFileSystemTableSinkFactory {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = super.requiredContext();
		context.put(FORMAT_TYPE, "avro");
		return context;
	}

	@Override
	public StreamTableSink createStreamTableSink(Map<String, String> properties) {

		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		new AvroValidator().validate(descriptorProperties);
		final String path = descriptorProperties.getString(CONNECTOR_BASEPATH);

		TableSchema tableSchema = null;
		AvroWriterFactory writerFactory = null;
		String schemaString = null;
		if (descriptorProperties.containsKey(AvroValidator.FORMAT_RECORD_CLASS)) {
			Class specificClass = descriptorProperties.getClass(
				AvroValidator.FORMAT_RECORD_CLASS,
				SpecificRecord.class);
			tableSchema = TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(
				specificClass));
			Schema avroschema = SpecificData.get().getSchema(specificClass);
			schemaString = avroschema.toString();
			writerFactory = new AvroWriterFactory(schemaString);
		} else if (descriptorProperties.containsKey(AvroValidator.FORMAT_AVRO_SCHEMA)) {
			schemaString = descriptorProperties.getString(AvroValidator.FORMAT_AVRO_SCHEMA);
			tableSchema = TableSchema.fromTypeInfo(AvroSchemaConverter.convertToTypeInfo(
				schemaString));
			writerFactory = new AvroWriterFactory(schemaString);
		}

		StreamingFileSink.BulkFormatBuilder builder = StreamingFileSink.forBulkFormat(
			new Path(path),
			writerFactory);
		if (descriptorProperties.containsKey(CONNECTOR_DATE_FORMAT)) {
			String dateFormat = descriptorProperties.getString(CONNECTOR_DATE_FORMAT);
			builder = builder.withBucketAssigner(new DateTimeBucketAssigner(dateFormat));
		}
		StreamingFileSink sink = builder.build();
		return new AvroFileSystemTableSink(tableSchema, schemaString, sink);
	}

}
