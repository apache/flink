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

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptionsUtil;
import org.apache.flink.connector.datagen.table.DataGenTableSource;
import org.apache.flink.connector.datagen.table.DataGenTableSourceFactory;
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
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataGenTableSourceFactory}. */
class DataGenTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("f0", DataTypes.STRING()),
                    Column.physical("f1", DataTypes.BIGINT()),
                    Column.physical("f2", DataTypes.BIGINT()),
                    Column.physical("f3", DataTypes.TIMESTAMP()),
                    Column.physical("f4", DataTypes.BINARY(2)),
                    Column.physical("f5", DataTypes.VARBINARY(4)),
                    Column.physical("f6", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                    Column.physical("f7", DataTypes.STRING()));

    @Test
    void testDataTypeCoverage() throws Exception {
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
                                                                "d", DataTypes.TIMESTAMP()))))),
                        Column.physical("f21", DataTypes.BINARY(2)),
                        Column.physical("f22", DataTypes.BYTES()),
                        Column.physical("f23", DataTypes.VARBINARY(4)));

        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), "10");

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
        assertThat(results).as("Failed to generate all rows").hasSize(10);

        for (RowData row : results) {
            for (int i = 0; i < row.getArity(); i++) {
                assertThat(row.isNullAt(i))
                        .as("Column " + schema.getColumnNames().get(i) + " should not be null")
                        .isFalse();
            }
        }
    }

    @Test
    void testSource() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.ROWS_PER_SECOND.key(), 100);

        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.RANDOM);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.LENGTH,
                20);

        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f1." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.RANDOM);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f1." + DataGenConnectorOptionsUtil.MIN, 10);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f1." + DataGenConnectorOptionsUtil.MAX, 100);

        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f2." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f2." + DataGenConnectorOptionsUtil.START,
                50);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f2." + DataGenConnectorOptionsUtil.END, 60);

        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f3." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.RANDOM);
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f3." + DataGenConnectorOptionsUtil.MAX_PAST,
                "5s");

        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f4." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.RANDOM);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f4." + DataGenConnectorOptionsUtil.LENGTH,
                2);
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f5." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f5." + DataGenConnectorOptionsUtil.START, 1);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f5." + DataGenConnectorOptionsUtil.END, 11);
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS
                        + ".f6.key."
                        + DataGenConnectorOptionsUtil.NULL_RATE,
                "1");

        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f7." + DataGenConnectorOptionsUtil.NULL_RATE,
                "1");

        final long begin = System.currentTimeMillis();
        List<RowData> results = runGenerator(SCHEMA, descriptor);
        final long end = System.currentTimeMillis();

        assertThat(results).hasSize(11);
        for (int i = 0; i < results.size(); i++) {
            RowData row = results.get(i);
            assertThat(row.getString(0).toString()).hasSize(20);
            assertThat(row.getLong(1)).isBetween(10L, 100L);
            assertThat(row.getLong(2)).isEqualTo(i + 50);
            assertThat(row.getTimestamp(3, 3).getMillisecond()).isBetween(begin - 5000, end);
            assertThat(row.getBinary(4)).hasSize(2);
            // f5 is sequence bytes produced in sequence long [1, 11]
            assertThat(row.getBinary(5)).hasSize(8);
            assertThat(row.getBinary(5)[row.getBinary(5).length - 1]).isEqualTo((byte) (i + 1));
            assertThat(row.getMap(6).keyArray().isNullAt(0)).isTrue();
            assertThat(row.getString(7)).isNull();
        }
    }

    private List<RowData> runGenerator(ResolvedSchema schema, DescriptorProperties descriptor)
            throws Exception {
        DynamicTableSource source = createTableSource(schema, descriptor.asMap());

        assertThat(source).isInstanceOf(DataGenTableSource.class);

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
    void testSequenceCheckpointRestore() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.START, 0);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.END, 100);

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
    void testLackStartForSequence() {
        assertThatThrownBy(
                        () -> {
                            DescriptorProperties descriptor = new DescriptorProperties();
                            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
                            descriptor.putString(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.KIND,
                                    DataGenConnectorOptionsUtil.SEQUENCE);
                            descriptor.putLong(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.END,
                                    100);

                            createTableSource(
                                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                    descriptor.asMap());
                        })
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Could not find required property 'fields.f0.start' for sequence generator."));
    }

    @Test
    void testLackEndForSequence() {
        assertThatThrownBy(
                        () -> {
                            DescriptorProperties descriptor = new DescriptorProperties();
                            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
                            descriptor.putString(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.KIND,
                                    DataGenConnectorOptionsUtil.SEQUENCE);
                            descriptor.putLong(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.START,
                                    0);

                            createTableSource(
                                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                    descriptor.asMap());
                        })
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Could not find required property 'fields.f0.end' for sequence generator."));
    }

    @Test
    void testWrongKey() {
        assertThatThrownBy(
                        () -> {
                            DescriptorProperties descriptor = new DescriptorProperties();
                            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
                            descriptor.putLong("wrong-rows-per-second", 1);

                            createTableSource(
                                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                    descriptor.asMap());
                        })
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Unsupported options:\n\nwrong-rows-per-second"));
    }

    @Test
    void testWrongStartInRandom() {
        assertThatThrownBy(
                        () -> {
                            DescriptorProperties descriptor = new DescriptorProperties();
                            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
                            descriptor.putString(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.KIND,
                                    DataGenConnectorOptionsUtil.RANDOM);
                            descriptor.putLong(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.START,
                                    0);

                            createTableSource(
                                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                    descriptor.asMap());
                        })
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Unsupported options:\n\nfields.f0.start"));
    }

    @Test
    void testWrongLenInRandomLong() {
        assertThatThrownBy(
                        () -> {
                            DescriptorProperties descriptor = new DescriptorProperties();
                            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
                            descriptor.putString(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.KIND,
                                    DataGenConnectorOptionsUtil.RANDOM);
                            descriptor.putInt(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.LENGTH,
                                    100);

                            createTableSource(
                                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                    descriptor.asMap());
                        })
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Unsupported options:\n\nfields.f0.length"));
    }

    @Test
    void testWrongTypes() {
        assertThatThrownBy(
                        () -> {
                            DescriptorProperties descriptor = new DescriptorProperties();
                            descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
                            descriptor.putString(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.KIND,
                                    DataGenConnectorOptionsUtil.SEQUENCE);
                            descriptor.putString(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.START,
                                    "Wrong");
                            descriptor.putString(
                                    DataGenConnectorOptionsUtil.FIELDS
                                            + ".f0."
                                            + DataGenConnectorOptionsUtil.END,
                                    "Wrong");

                            createTableSource(
                                    ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                    descriptor.asMap());
                        })
                .satisfies(
                        anyCauseMatches("Could not parse value 'Wrong' for key 'fields.f0.start'"));
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
