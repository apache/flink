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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.datagen.DataGenTableSource;
import org.apache.flink.table.sources.datagen.DataGenTableSourceFactory;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.CONNECTOR_ROWS_PER_SECOND;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.END;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.GENERATOR;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.LENGTH;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.MAX;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.MIN;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.RANDOM;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.SEQUENCE;
import static org.apache.flink.table.sources.datagen.DataGenTableSourceFactory.START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataGenTableSourceFactory}.
 */
public class DataGenTableSourceFactoryTest {

	private static final TableSchema TEST_SCHEMA = TableSchema.builder()
		.field("f0", DataTypes.STRING())
		.field("f1", DataTypes.BIGINT())
		.field("f2", DataTypes.BIGINT())
		.build();

	@Test
	public void testSource() throws Exception {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		descriptor.putLong(CONNECTOR_ROWS_PER_SECOND, 100);

		descriptor.putString(SCHEMA + ".f0." + GENERATOR, RANDOM);
		descriptor.putLong(SCHEMA + ".f0." + GENERATOR + "." + LENGTH, 20);

		descriptor.putString(SCHEMA + ".f1." + GENERATOR, RANDOM);
		descriptor.putLong(SCHEMA + ".f1." + GENERATOR + "." + MIN, 10);
		descriptor.putLong(SCHEMA + ".f1." + GENERATOR + "." + MAX, 100);

		descriptor.putString(SCHEMA + ".f2." + GENERATOR, SEQUENCE);
		descriptor.putLong(SCHEMA + ".f2." + GENERATOR + "." + START, 50);
		descriptor.putLong(SCHEMA + ".f2." + GENERATOR + "." + END, 60);

		TableSource source = TableFactoryService.find(
				TableSourceFactory.class, descriptor.asMap())
				.createTableSource(new TableSourceFactoryContextImpl(
						ObjectIdentifier.of("", "", ""),
						new CatalogTableImpl(TEST_SCHEMA, descriptor.asMap(), ""),
						new Configuration()));

		assertTrue(source instanceof DataGenTableSource);
		assertEquals(TEST_SCHEMA.toRowDataType(), source.getProducedDataType());

		DataGenTableSource dataGenTableSource = (DataGenTableSource) source;
		DataGeneratorSource<Row> gen = dataGenTableSource.createSource();

		StreamSource<Row, DataGeneratorSource<Row>> src = new StreamSource<>(gen);
		AbstractStreamOperatorTestHarness<Row> testHarness =
				new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
		testHarness.open();

		List<Row> results = new ArrayList<>();

		gen.run(new SourceFunction.SourceContext<Row>() {

			private Object lock = new Object();

			@Override
			public void collect(Row element) {
				results.add(element);
			}

			@Override
			public void collectWithTimestamp(Row element, long timestamp) {
			}

			@Override
			public void emitWatermark(Watermark mark) {
			}

			@Override
			public void markAsTemporarilyIdle() {
			}

			@Override
			public Object getCheckpointLock() {
				return lock;
			}

			@Override
			public void close() {
			}
		});

		Assert.assertEquals(11, results.size());
		for (int i = 0; i < results.size(); i++) {
			Row row = results.get(i);
			Assert.assertEquals(20, row.getField(0).toString().length());
			long f1 = (long) row.getField(1);
			Assert.assertTrue(f1 >= 10 && f1 <= 100);
			Assert.assertEquals(i + 50, (long) row.getField(2));
		}
	}
}
