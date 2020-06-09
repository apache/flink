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
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSourceTest;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DataGenTableSourceFactory.DataGenTableSource;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.factories.DataGenTableSourceFactory.END;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.FIELDS;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.KIND;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.LENGTH;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.MAX;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.MIN;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.RANDOM;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.ROWS_PER_SECOND;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.SEQUENCE;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.START;
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
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
		descriptor.putLong(ROWS_PER_SECOND.key(), 100);

		descriptor.putString(FIELDS + ".f0." + KIND, RANDOM);
		descriptor.putLong(FIELDS + ".f0." + LENGTH, 20);

		descriptor.putString(FIELDS + ".f1." + KIND, RANDOM);
		descriptor.putLong(FIELDS + ".f1." + MIN, 10);
		descriptor.putLong(FIELDS + ".f1." + MAX, 100);

		descriptor.putString(FIELDS + ".f2." + KIND, SEQUENCE);
		descriptor.putLong(FIELDS + ".f2." + START, 50);
		descriptor.putLong(FIELDS + ".f2." + END, 60);

		DynamicTableSource source = createSource(
				TEST_SCHEMA, descriptor.asMap());

		assertTrue(source instanceof DataGenTableSource);

		DataGenTableSource dataGenTableSource = (DataGenTableSource) source;
		DataGeneratorSource<RowData> gen = dataGenTableSource.createSource();

		// test java serialization.
		gen = InstantiationUtil.clone(gen);

		StreamSource<RowData, DataGeneratorSource<RowData>> src = new StreamSource<>(gen);
		AbstractStreamOperatorTestHarness<RowData> testHarness =
				new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
		testHarness.open();

		List<RowData> results = new ArrayList<>();

		gen.run(new SourceFunction.SourceContext<RowData>() {

			private Object lock = new Object();

			@Override
			public void collect(RowData element) {
				results.add(element);
			}

			@Override
			public void collectWithTimestamp(RowData element, long timestamp) {
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
			RowData row = results.get(i);
			Assert.assertEquals(20, row.getString(0).toString().length());
			long f1 = row.getLong(1);
			Assert.assertTrue(f1 >= 10 && f1 <= 100);
			Assert.assertEquals(i + 50, row.getLong(2));
		}
	}

	@Test
	public void testSequenceCheckpointRestore() throws Exception {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
		descriptor.putString(FIELDS + ".f0." + KIND, SEQUENCE);
		descriptor.putLong(FIELDS + ".f0." + START, 0);
		descriptor.putLong(FIELDS + ".f0." + END, 100);

		DynamicTableSource dynamicTableSource = createSource(
				TableSchema.builder().field("f0", DataTypes.BIGINT()).build(),
				descriptor.asMap());

		DataGenTableSource dataGenTableSource = (DataGenTableSource) dynamicTableSource;
		DataGeneratorSource<RowData> source = dataGenTableSource.createSource();

		final int initElement = 0;
		final int maxElement = 100;
		final Set<RowData> expectedOutput = new HashSet<>();
		for (long i = initElement; i <= maxElement; i++) {
			expectedOutput.add(GenericRowData.of(i));
		}
		DataGeneratorSourceTest.innerTestDataGenCheckpointRestore(
				() -> {
					try {
						return InstantiationUtil.clone(source);
					} catch (IOException | ClassNotFoundException e) {
						throw new RuntimeException(e);
					}
				}, expectedOutput);
	}

	@Test
	public void testLackStartForSequence() {
		try {
			DescriptorProperties descriptor = new DescriptorProperties();
			descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
			descriptor.putString(FIELDS + ".f0." + KIND, SEQUENCE);
			descriptor.putLong(FIELDS + ".f0." + END, 100);

			createSource(
					TableSchema.builder().field("f0", DataTypes.BIGINT()).build(),
					descriptor.asMap());
		} catch (ValidationException e) {
			Throwable cause = e.getCause();
			Assert.assertTrue(cause instanceof ValidationException);
			Assert.assertTrue(cause.getMessage().contains(
					"Could not find required property 'fields.f0.start' for sequence generator."));
			return;
		}
		Assert.fail("Should fail by ValidationException.");
	}

	@Test
	public void testLackEndForSequence() {
		try {
			DescriptorProperties descriptor = new DescriptorProperties();
			descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
			descriptor.putString(FIELDS + ".f0." + KIND, SEQUENCE);
			descriptor.putLong(FIELDS + ".f0." + START, 0);

			createSource(
					TableSchema.builder().field("f0", DataTypes.BIGINT()).build(),
					descriptor.asMap());
		} catch (ValidationException e) {
			Throwable cause = e.getCause();
			Assert.assertTrue(cause instanceof ValidationException);
			Assert.assertTrue(cause.getMessage().contains(
					"Could not find required property 'fields.f0.end' for sequence generator."));
			return;
		}
		Assert.fail("Should fail by ValidationException.");
	}

	private static DynamicTableSource createSource(TableSchema schema, Map<String, String> options) {
		return FactoryUtil.createTableSource(
				null,
				ObjectIdentifier.of("", "", ""),
				new CatalogTableImpl(schema, options, ""),
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
	}
}
