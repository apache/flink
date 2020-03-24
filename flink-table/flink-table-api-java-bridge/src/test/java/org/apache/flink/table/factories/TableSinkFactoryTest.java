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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.BlackHoleTableSinkFactory;
import org.apache.flink.table.sinks.PrintTableSinkFactory;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.datagen.DataGenTableSourceFactory;

import org.junit.Test;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataGenTableSourceFactory}.
 */
public class TableSinkFactoryTest {

	private static final TableSchema TEST_SCHEMA = TableSchema.builder()
		.field("f0", DataTypes.STRING())
		.field("f1", DataTypes.BIGINT())
		.field("f2", DataTypes.BIGINT())
		.build();

	@Test
	public void testPrint() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(CONNECTOR_TYPE, "print");

		TableSinkFactory factory = TableFactoryService.find(
				TableSinkFactory.class, descriptor.asMap());

		assertTrue(factory instanceof PrintTableSinkFactory);

		TableSink sink = factory.createTableSink(new TableSinkFactoryContextImpl(
				ObjectIdentifier.of("", "", ""),
				new CatalogTableImpl(TEST_SCHEMA, descriptor.asMap(), ""),
				new Configuration()));

		assertTrue(sink instanceof RetractStreamTableSink);

		RetractStreamTableSink retractSink = (RetractStreamTableSink) sink;

		assertEquals(TEST_SCHEMA.toRowType(), retractSink.getRecordType());
	}

	@Test
	public void testBlackHole() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(CONNECTOR_TYPE, "blackhole");

		TableSinkFactory factory = TableFactoryService.find(
				TableSinkFactory.class, descriptor.asMap());

		assertTrue(factory instanceof BlackHoleTableSinkFactory);

		TableSink sink = factory.createTableSink(new TableSinkFactoryContextImpl(
				ObjectIdentifier.of("", "", ""),
				new CatalogTableImpl(TEST_SCHEMA, descriptor.asMap(), ""),
				new Configuration()));

		assertTrue(sink instanceof RetractStreamTableSink);

		RetractStreamTableSink retractSink = (RetractStreamTableSink) sink;

		assertEquals(TEST_SCHEMA.toRowType(), retractSink.getRecordType());
	}
}
