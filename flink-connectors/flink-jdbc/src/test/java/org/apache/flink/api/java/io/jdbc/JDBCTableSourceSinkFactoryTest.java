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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Test for {@link JDBCTableSource} and {@link JDBCUpsertTableSink} created
 * by {@link JDBCTableSourceSinkFactory}.
 */
public class JDBCTableSourceSinkFactoryTest {

	@Test
	public void testJDBCCommonProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.driver", "org.apache.derby.jdbc.EmbeddedDriver");
		properties.put("connector.username", "user");
		properties.put("connector.password", "pass");

		final StreamTableSource<?> actual = TableFactoryService.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties);

		final JDBCOptions options = JDBCOptions.builder()
			.setDBUrl("jdbc:derby:memory:mydb")
			.setTableName("mytable")
			.setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
			.setUsername("user")
			.setPassword("pass")
			.build();
		final TableSchema schema = TableSchema.builder()
			.field("aaa", DataTypes.INT())
			.field("bbb", DataTypes.STRING())
			.field("ccc", DataTypes.DOUBLE())
			.build();
		final JDBCTableSource expected = JDBCTableSource.builder()
			.setOptions(options)
			.setSchema(schema)
			.build();

		assertEquals(expected, actual);
	}

	@Test
	public void testJDBCReadProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.read.partition.column", "aaa");
		properties.put("connector.read.partition.lower-bound", "-10");
		properties.put("connector.read.partition.upper-bound", "100");
		properties.put("connector.read.partition.num", "10");
		properties.put("connector.read.fetch-size", "20");

		final StreamTableSource<?> actual = TableFactoryService.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties);

		final JDBCOptions options = JDBCOptions.builder()
			.setDBUrl("jdbc:derby:memory:mydb")
			.setTableName("mytable")
			.build();
		final JDBCReadOptions readOptions = JDBCReadOptions.builder()
			.setPartitionColumnName("aaa")
			.setPartitionLowerBound(-10)
			.setPartitionUpperBound(100)
			.setNumPartitions(10)
			.setFetchSize(20)
			.build();
		final TableSchema schema = TableSchema.builder()
			.field("aaa", DataTypes.INT())
			.field("bbb", DataTypes.STRING())
			.field("ccc", DataTypes.DOUBLE())
			.build();
		final JDBCTableSource expected = JDBCTableSource.builder()
			.setOptions(options)
			.setReadOptions(readOptions)
			.setSchema(schema)
			.build();

		assertEquals(expected, actual);
	}

	@Test
	public void testJDBCLookupProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.lookup.cache.max-rows", "1000");
		properties.put("connector.lookup.cache.ttl", "10s");
		properties.put("connector.lookup.max-retries", "10");

		final StreamTableSource<?> actual = TableFactoryService.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties);

		final JDBCOptions options = JDBCOptions.builder()
			.setDBUrl("jdbc:derby:memory:mydb")
			.setTableName("mytable")
			.build();
		final JDBCLookupOptions lookupOptions = JDBCLookupOptions.builder()
			.setCacheMaxSize(1000)
			.setCacheExpireMs(10_000)
			.setMaxRetryTimes(10)
			.build();
		final TableSchema schema = TableSchema.builder()
			.field("aaa", DataTypes.INT())
			.field("bbb", DataTypes.STRING())
			.field("ccc", DataTypes.DOUBLE())
			.build();
		final JDBCTableSource expected = JDBCTableSource.builder()
			.setOptions(options)
			.setLookupOptions(lookupOptions)
			.setSchema(schema)
			.build();

		assertEquals(expected, actual);
	}

	@Test
	public void testJDBCSinkProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.write.flush.max-rows", "1000");
		properties.put("connector.write.flush.interval", "2min");
		properties.put("connector.write.max-retries", "5");

		final StreamTableSink<?> actual = TableFactoryService.find(StreamTableSinkFactory.class, properties)
			.createStreamTableSink(properties);

		final JDBCOptions options = JDBCOptions.builder()
			.setDBUrl("jdbc:derby:memory:mydb")
			.setTableName("mytable")
			.build();
		final TableSchema schema = TableSchema.builder()
			.field("aaa", DataTypes.INT())
			.field("bbb", DataTypes.STRING())
			.field("ccc", DataTypes.DOUBLE())
			.build();
		final JDBCUpsertTableSink expected = JDBCUpsertTableSink.builder()
			.setOptions(options)
			.setTableSchema(schema)
			.setFlushMaxSize(1000)
			.setFlushIntervalMills(120_000)
			.setMaxRetryTimes(5)
			.build();

		assertEquals(expected, actual);
	}

	@Test
	public void testJDBCWithFilter() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.driver", "org.apache.derby.jdbc.EmbeddedDriver");
		properties.put("connector.username", "user");
		properties.put("connector.password", "pass");

		final TableSource<?> actual = ((JDBCTableSource) TableFactoryService
			.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties))
			.projectFields(new int[] {0, 2});

		Map<String, DataType> projectedFields = ((FieldsDataType) actual.getProducedDataType()).getFieldDataTypes();
		assertEquals(projectedFields.get("aaa"), DataTypes.INT());
		assertNull(projectedFields.get("bbb"));
		assertEquals(projectedFields.get("ccc"), DataTypes.DOUBLE());
	}

	@Test
	public void testJDBCValidation() {
		// only password, no username
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.password", "pass");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// read partition properties not complete
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.read.partition.column", "aaa");
			properties.put("connector.read.partition.lower-bound", "-10");
			properties.put("connector.read.partition.upper-bound", "100");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// read partition lower-bound > upper-bound
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.read.partition.column", "aaa");
			properties.put("connector.read.partition.lower-bound", "100");
			properties.put("connector.read.partition.upper-bound", "-10");
			properties.put("connector.read.partition.num", "10");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// lookup cache properties not complete
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.lookup.cache.max-rows", "10");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}

		// lookup cache properties not complete
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.lookup.cache.ttl", "1s");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (IllegalArgumentException ignored) {
		}
	}

	private Map<String, String> getBasicProperties() {
		Map<String, String> properties = new HashMap<>();

		properties.put("connector.type", "jdbc");
		properties.put("connector.property-version", "1");

		properties.put("connector.url", "jdbc:derby:memory:mydb");
		properties.put("connector.table", "mytable");

		properties.put("schema.0.name", "aaa");
		properties.put("schema.0.type", "INT");
		properties.put("schema.1.name", "bbb");
		properties.put("schema.1.type", "VARCHAR");
		properties.put("schema.2.name", "ccc");
		properties.put("schema.2.type", "DOUBLE");

		return properties;
	}
}
