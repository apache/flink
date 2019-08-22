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

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.streaming.connectors.fs.table.AvroFileSystemTableSink;
import org.apache.flink.streaming.connectors.fs.table.ParquetFileSystemTableSink;
import org.apache.flink.streaming.connectors.fs.table.RowFileSystemTableSink;
import org.apache.flink.streaming.connectors.fs.table.descriptors.Bucket;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Parquet;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TestTableDescriptor;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class FileSystemTableSinkFactoryTCase extends TestLogger {

	private static final String FIELD_NAME = "name";
	private static final String FIELD_AGE = "age";
	private static final String FIELD_TS = "ts";

	@Test
	public void testRawFileSystemTableSink() {

		final TableSchema schema = createTestSchema();
		final String path = "file:///tmp/json";
		final TestTableDescriptor testDesc = new TestTableDescriptor(
			new Bucket().basePath(path).rowFormat())
			.withFormat(
				new Json()
					.deriveSchema())
			.withSchema(
				new Schema()
					.field(FIELD_NAME, Types.STRING())
					.field(FIELD_AGE, Types.INT())
					.field(FIELD_TS, Types.SQL_TIMESTAMP()))
			.inAppendMode();
		final Map<String, String> propertiesMap = testDesc.toProperties();
		final TableSink<?> sink = TableFactoryService
			.find(StreamTableSinkFactory.class, propertiesMap)
			.createStreamTableSink(propertiesMap);

		assertTrue(sink instanceof RowFileSystemTableSink);
	}

	@Test
	public void testAvroFileSystemTableSink() {

		final TableSchema schema = createTestSchema();
		final String path = "file:///tmp/avro";
		final TestTableDescriptor testDesc = new TestTableDescriptor(
			new Bucket().basePath(path).bultFormat())
			.withFormat(new Avro().avroSchema(
				"{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"org.apache.flink.formats.avro.generated\",\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"string\"}]}"))
			.inAppendMode();

		final Map<String, String> propertiesMap = testDesc.toProperties();
		final TableSink<?> sink = TableFactoryService
			.find(StreamTableSinkFactory.class, propertiesMap)
			.createStreamTableSink(propertiesMap);

		assertTrue(sink instanceof AvroFileSystemTableSink);
	}

	@Test
	public void testParquetFileSystemTableSink() {

		String schemaStr = "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"org.apache.flink.formats.avro.generated\",\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"state\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"string\"}]}";
		org.apache.avro.Schema avroschema = new org.apache.avro.Schema.Parser().parse(schemaStr);

		final String path = "file:///tmp/parquet";
		final TestTableDescriptor testDesc = new TestTableDescriptor(
			new Bucket().basePath(path).bultFormat())
			.withFormat(new Parquet().schema(avroschema))
			.inAppendMode();

		final Map<String, String> propertiesMap = testDesc.toProperties();
		final TableSink<?> sink = TableFactoryService
			.find(StreamTableSinkFactory.class, propertiesMap)
			.createStreamTableSink(propertiesMap);

		assertTrue(sink instanceof ParquetFileSystemTableSink);
	}

	protected TableSchema createTestSchema() {
		return TableSchema.builder()
			.field(FIELD_NAME, Types.STRING())
			.field(FIELD_AGE, Types.INT())
			.field(FIELD_TS, Types.SQL_TIMESTAMP())
			.build();
	}

}
