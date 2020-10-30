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

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link FileSystemTableFactory}.
 */
public class FileSystemTableFactoryTest {

	private static final TableSchema TEST_SCHEMA = TableSchema.builder()
			.field("f0", DataTypes.STRING())
			.field("f1", DataTypes.BIGINT())
			.field("f2", DataTypes.BIGINT())
			.build();

	@Test
	public void testSourceSink() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
		descriptor.putString("path", "/tmp");
		descriptor.putString("format", "testcsv");

		// test ignore format options
		descriptor.putString("testcsv.my_option", "my_value");

		DynamicTableSource source = createSource(descriptor);
		Assert.assertTrue(source instanceof FileSystemTableSource);

		DynamicTableSink sink = createSink(descriptor);
		Assert.assertTrue(sink instanceof FileSystemTableSink);
	}

	@Test
	public void testLackOptionSource() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
		descriptor.putString("path", "/tmp");

		try {
			createSource(descriptor);
		} catch (ValidationException e) {
			Throwable cause = e.getCause();
			Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
			Assert.assertTrue(cause.getMessage(), cause.getMessage().contains(
					"Missing required options are:\n\nformat"));
			return;
		}

		Assert.fail("Should fail by ValidationException.");
	}

	@Test
	public void testLackOptionSink() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
		descriptor.putString("path", "/tmp");

		try {
			createSink(descriptor);
		} catch (ValidationException e) {
			Throwable cause = e.getCause();
			Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
			Assert.assertTrue(cause.getMessage(), cause.getMessage().contains(
					"Missing required options are:\n\nformat"));
			return;
		}

		Assert.fail("Should fail by ValidationException.");
	}

	@Test
	public void testUnsupportedOptionSource() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
		descriptor.putString("path", "/tmp");
		descriptor.putString("format", "csv");
		descriptor.putString("my_option", "my");

		try {
			createSource(descriptor);
		} catch (ValidationException e) {
			Throwable cause = e.getCause();
			Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
			Assert.assertTrue(cause.getMessage(), cause.getMessage().contains(
					"Unsupported options:\n\nmy_option"));
			return;
		}

		Assert.fail("Should fail by ValidationException.");
	}

	@Test
	public void testUnsupportedOptionSink() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
		descriptor.putString("path", "/tmp");
		descriptor.putString("format", "csv");
		descriptor.putString("my_option", "my");

		try {
			createSink(descriptor);
		} catch (ValidationException e) {
			Throwable cause = e.getCause();
			Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
			Assert.assertTrue(cause.getMessage(), cause.getMessage().contains(
					"Unsupported options:\n\nmy_option"));
			return;
		}

		Assert.fail("Should fail by ValidationException.");
	}

	private static DynamicTableSource createSource(DescriptorProperties properties) {
		return FactoryUtil.createTableSource(
				null,
				ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
				new CatalogTableImpl(TEST_SCHEMA, properties.asMap(), ""),
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	private static DynamicTableSink createSink(DescriptorProperties properties) {
		return FactoryUtil.createTableSink(
				null,
				ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
				new CatalogTableImpl(TEST_SCHEMA, properties.asMap(), ""),
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}
}
