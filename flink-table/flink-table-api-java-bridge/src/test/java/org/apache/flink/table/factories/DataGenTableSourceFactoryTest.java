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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSourceTest;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.datagen.DataGenTableSource;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.factories.DataGenTableSourceFactory.END;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.FIELDS;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.KIND;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.LENGTH;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.MAX;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.MIN;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.NUMBER_OF_ROWS;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.RANDOM;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.ROWS_PER_SECOND;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.SEQUENCE;
import static org.apache.flink.table.factories.DataGenTableSourceFactory.START;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertTrue;

/** Tests for {@link DataGenTableSourceFactory}. */
public class DataGenTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("f0", DataTypes.STRING()),
                    Column.physical("f1", DataTypes.BIGINT()),
                    Column.physical("f2", DataTypes.BIGINT()));

    @Test
    public void testDataTypeCoverage() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("f0", DataTypes.CHAR(1)),
                        Column.physical("f1", DataTypes.VARCHAR(10)),
                        Column.physical("f2", DataTypes.STRING()),
                        Column.physical("f3", DataTypes.BOOLEAN()),
                        Column.physical("f4", DataTypes.DECIMAL(32, 2)),
                        Column.physical("f5", DataTypes.TINYINT()),
                        Column.physical("f6", DataTypes.SMALLINT()),
                        Column.physical("f7", DataTypes.INT()),
                        Column.physical("f8", DataTypes.BIGINT()),
                        Column.physical("f9", DataTypes.FLOAT()),
                        Column.physical("f10", DataTypes.DOUBLE()),
                        Column.physical("f11", DataTypes.DATE()),
                        Column.physical("f12", DataTypes.TIME()),
                        Column.physical("f13", DataTypes.TIMESTAMP()),
                        Column.physical("f14", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f15", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                        Column.physical("f16", DataTypes.INTERVAL(DataTypes.DAY())),
                        Column.physical("f17", DataTypes.ARRAY(DataTypes.INT())),
                        Column.physical("f18", DataTypes.MAP(DataTypes.STRING(), DataTypes.DATE())),
                        Column.physical("f19", DataTypes.MULTISET(DataTypes.DECIMAL(32, 2))),
                        Column.physical(
                                "f20",
                                DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.BIGINT()),
                                        DataTypes.FIELD("b", DataTypes.TIME()),
                                        DataTypes.FIELD(
                                                "c",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "d", DataTypes.TIMESTAMP()))))));

        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(NUMBER_OF_ROWS.key(), "10");

        // add min max option for numeric types
        descriptor.putString("fields.f4.min", "1.0");
        descriptor.putString("fields.f4.max", "1000.0");
        descriptor.putString("fields.f5.min", "0");
        descriptor.putString("fields.f5.max", "127");
        descriptor.putString("fields.f6.min", "0");
        descriptor.putString("fields.f6.max", "32767");
        descriptor.putString("fields.f7.min", "0");
        descriptor.putString("fields.f7.max", "65535");
        descriptor.putString("fields.f8.min", "0");
        descriptor.putString("fields.f8.max", String.valueOf(Long.MAX_VALUE));
        descriptor.putString("fields.f9.min", "0");
        descriptor.putString("fields.f9.max", String.valueOf(Float.MAX_VALUE));
        descriptor.putString("fields.f10.min", "0");
        descriptor.putString("fields.f10.max", String.valueOf(Double.MAX_VALUE));

        List<RowData> results = runGenerator(schema, descriptor);
        Assert.assertEquals("Failed to generate all rows", 10, results.size());

        for (RowData row : results) {
            for (int i = 0; i < row.getArity(); i++) {
                Assert.assertFalse(
                        "Column " + schema.getColumnNames().get(i) + " should not be null",
                        row.isNullAt(i));
            }
        }
    }

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

        List<RowData> results = runGenerator(SCHEMA, descriptor);

        Assert.assertEquals(11, results.size());
        for (int i = 0; i < results.size(); i++) {
            RowData row = results.get(i);
            Assert.assertEquals(20, row.getString(0).toString().length());
            long f1 = row.getLong(1);
            Assert.assertTrue(f1 >= 10 && f1 <= 100);
            Assert.assertEquals(i + 50, row.getLong(2));
        }
    }

    private List<RowData> runGenerator(ResolvedSchema schema, DescriptorProperties descriptor)
            throws Exception {
        DynamicTableSource source = createTableSource(schema, descriptor.asMap());

        assertTrue(source instanceof DataGenTableSource);

        DataGenTableSource dataGenTableSource = (DataGenTableSource) source;
        DataGeneratorSource<RowData> gen = dataGenTableSource.createSource();

        // test java serialization.
        gen = InstantiationUtil.clone(gen);

        StreamSource<RowData, DataGeneratorSource<RowData>> src = new StreamSource<>(gen);
        AbstractStreamOperatorTestHarness<RowData> testHarness =
                new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
        testHarness.open();

        TestContext ctx = new TestContext();

        gen.run(ctx);

        return ctx.results;
    }

    @Test
    public void testSequenceCheckpointRestore() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(FIELDS + ".f0." + KIND, SEQUENCE);
        descriptor.putLong(FIELDS + ".f0." + START, 0);
        descriptor.putLong(FIELDS + ".f0." + END, 100);

        DynamicTableSource dynamicTableSource =
                createTableSource(
                        ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
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
                },
                expectedOutput);
    }

    @Test
    public void testLackStartForSequence() {
        try {
            DescriptorProperties descriptor = new DescriptorProperties();
            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
            descriptor.putString(FIELDS + ".f0." + KIND, SEQUENCE);
            descriptor.putLong(FIELDS + ".f0." + END, 100);

            createTableSource(
                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                    descriptor.asMap());
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
            Assert.assertTrue(
                    cause.getMessage(),
                    cause.getMessage()
                            .contains(
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

            createTableSource(
                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                    descriptor.asMap());
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
            Assert.assertTrue(
                    cause.getMessage(),
                    cause.getMessage()
                            .contains(
                                    "Could not find required property 'fields.f0.end' for sequence generator."));
            return;
        }
        Assert.fail("Should fail by ValidationException.");
    }

    @Test
    public void testWrongKey() {
        try {
            DescriptorProperties descriptor = new DescriptorProperties();
            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
            descriptor.putLong("wrong-rows-per-second", 1);

            createTableSource(
                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                    descriptor.asMap());
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
            Assert.assertTrue(
                    cause.getMessage(),
                    cause.getMessage().contains("Unsupported options:\n\nwrong-rows-per-second"));
            return;
        }
        Assert.fail("Should fail by ValidationException.");
    }

    @Test
    public void testWrongStartInRandom() {
        try {
            DescriptorProperties descriptor = new DescriptorProperties();
            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
            descriptor.putString(FIELDS + ".f0." + KIND, RANDOM);
            descriptor.putLong(FIELDS + ".f0." + START, 0);

            createTableSource(
                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                    descriptor.asMap());
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
            Assert.assertTrue(
                    cause.getMessage(),
                    cause.getMessage().contains("Unsupported options:\n\nfields.f0.start"));
            return;
        }
        Assert.fail("Should fail by ValidationException.");
    }

    @Test
    public void testWrongLenInRandomLong() {
        try {
            DescriptorProperties descriptor = new DescriptorProperties();
            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
            descriptor.putString(FIELDS + ".f0." + KIND, RANDOM);
            descriptor.putInt(FIELDS + ".f0." + LENGTH, 100);

            createTableSource(
                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                    descriptor.asMap());
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause.toString(), cause instanceof ValidationException);
            Assert.assertTrue(
                    cause.getMessage(),
                    cause.getMessage().contains("Unsupported options:\n\nfields.f0.length"));
            return;
        }
        Assert.fail("Should fail by ValidationException.");
    }

    @Test
    public void testWrongTypes() {
        try {
            DescriptorProperties descriptor = new DescriptorProperties();
            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
            descriptor.putString(FIELDS + ".f0." + KIND, SEQUENCE);
            descriptor.putString(FIELDS + ".f0." + START, "Wrong");
            descriptor.putString(FIELDS + ".f0." + END, "Wrong");

            createTableSource(
                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                    descriptor.asMap());
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause.toString(), cause instanceof IllegalArgumentException);
            Assert.assertTrue(
                    cause.getMessage(),
                    cause.getMessage()
                            .contains("Could not parse value 'Wrong' for key 'fields.f0.start'"));
            return;
        }
        Assert.fail("Should fail by ValidationException.");
    }

    private static class TestContext implements SourceFunction.SourceContext<RowData> {

        private final Object lock = new Object();

        private final List<RowData> results = new ArrayList<>();

        @Override
        public void collect(RowData element) {
            results.add(element);
        }

        @Override
        public void collectWithTimestamp(RowData element, long timestamp) {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void markAsTemporarilyIdle() {}

        @Override
        public Object getCheckpointLock() {
            return lock;
        }

        @Override
        public void close() {}
    }
}
