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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptionsUtil;
import org.apache.flink.connector.datagen.table.DataGenTableSource;
import org.apache.flink.connector.datagen.table.DataGenTableSourceFactory;
import org.apache.flink.connector.datagen.table.RandomGeneratorVisitor;
import org.apache.flink.connector.datagen.table.types.SequenceGeneratorFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.datagen.table.RandomGeneratorVisitor.RANDOM_COLLECTION_LENGTH_DEFAULT;
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
    private static final ResolvedSchema LENGTH_CONSTRAINED_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("f0", DataTypes.CHAR(50)),
                    Column.physical("f1", DataTypes.BINARY(40)),
                    Column.physical("f2", DataTypes.VARCHAR(30)),
                    Column.physical("f3", DataTypes.VARBINARY(20)),
                    Column.physical("f4", DataTypes.STRING()));
    private static final ResolvedSchema COLLECTION_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("f0", DataTypes.ARRAY(DataTypes.STRING())),
                    Column.physical("f1", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                    Column.physical("f2", DataTypes.MULTISET(DataTypes.INT())));

    /**
     * Upper bound on the number of rows generated for unbounded (random-only) sources, where {@link
     * DataGenTableSource#computeEffectiveCount()} returns {@link Long#MAX_VALUE}. Tests that assert a
     * specific row count always configure {@code number-of-rows} explicitly and never hit this cap.
     */
    private static final long DEFAULT_UNBOUNDED_TEST_LIMIT = 1_000L;

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

        // The two sequence fields use [50,60] (count 11) and [1,11] (count 11); both equal so the
        // bound is 11. Without numberOfRows configured, the source halts at 11.
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

    @Test
    void testVariableLengthDataGeneration() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        final int rowsNumber = 999;
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), rowsNumber);

        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("f0", DataTypes.STRING()),
                        Column.physical("f1", DataTypes.VARCHAR(20)),
                        Column.physical("f2", DataTypes.BYTES()),
                        Column.physical("f3", DataTypes.VARBINARY(4)),
                        Column.physical("f4", DataTypes.BINARY(2)));

        List<RowData> results = runGenerator(schema, descriptor);
        assertThat(results).hasSize(rowsNumber);

        for (RowData row : results) {
            assertThat(row.getString(0).toString())
                    .hasSize(RandomGeneratorVisitor.RANDOM_STRING_LENGTH_DEFAULT);
            assertThat(row.getString(1).toString()).hasSize(20);
            assertThat(row.getBinary(2))
                    .hasSize(RandomGeneratorVisitor.RANDOM_BYTES_LENGTH_DEFAULT);
            assertThat(row.getBinary(3)).hasSize(4);
        }

        descriptor.putBoolean(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.VAR_LEN,
                true);
        descriptor.putBoolean(
                DataGenConnectorOptionsUtil.FIELDS + ".f1." + DataGenConnectorOptionsUtil.VAR_LEN,
                true);
        descriptor.putBoolean(
                DataGenConnectorOptionsUtil.FIELDS + ".f2." + DataGenConnectorOptionsUtil.VAR_LEN,
                true);
        descriptor.putBoolean(
                DataGenConnectorOptionsUtil.FIELDS + ".f3." + DataGenConnectorOptionsUtil.VAR_LEN,
                true);

        results = runGenerator(schema, descriptor);
        Set<Integer> sizeString = new HashSet<>();
        Set<Integer> sizeVarChar = new HashSet<>();
        Set<Integer> sizeBytes = new HashSet<>();
        Set<Integer> sizeVarBinary = new HashSet<>();

        for (RowData row : results) {
            assertThat(row.getString(0).toString())
                    .hasSizeBetween(1, RandomGeneratorVisitor.RANDOM_STRING_LENGTH_DEFAULT);
            assertThat(row.getString(1).toString())
                    .hasSizeBetween(1, RandomGeneratorVisitor.RANDOM_STRING_LENGTH_DEFAULT);
            assertThat(row.getBinary(2))
                    .hasSizeBetween(1, RandomGeneratorVisitor.RANDOM_BYTES_LENGTH_DEFAULT);
            assertThat(row.getBinary(3))
                    .hasSizeBetween(1, RandomGeneratorVisitor.RANDOM_BYTES_LENGTH_DEFAULT);
            sizeString.add(row.getString(0).toString().length());
            sizeVarChar.add(row.getString(1).toString().length());
            sizeBytes.add(row.getBinary(2).length);
            sizeVarBinary.add(row.getBinary(3).length);
        }
        assertThat(sizeString.size()).isGreaterThan(1);
        assertThat(sizeBytes.size()).isGreaterThan(1);
        assertThat(sizeVarBinary.size()).isGreaterThan(1);
        assertThat(sizeVarChar.size()).isGreaterThan(1);

        assertException(
                schema,
                descriptor,
                "f4",
                null,
                true,
                String.format(
                        "Only supports specifying '%s' option for variable-length types (VARCHAR/STRING/VARBINARY/BYTES). The type of field '%s' is not within this range.",
                        DataGenConnectorOptions.FIELD_VAR_LEN.key(), "f4"));
    }

    @Test
    void testVariableLengthDataType() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        final int rowsNumber = 200;
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), rowsNumber);

        List<RowData> results = runGenerator(LENGTH_CONSTRAINED_SCHEMA, descriptor);
        assertThat(results).hasSize(rowsNumber);

        for (RowData row : results) {
            assertThat(row.getString(2).toString()).hasSize(30);
            assertThat(row.getBinary(3)).hasSize(20);
            assertThat(row.getString(4).toString())
                    .hasSize(RandomGeneratorVisitor.RANDOM_STRING_LENGTH_DEFAULT);
        }

        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f2." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.RANDOM);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f2." + DataGenConnectorOptionsUtil.LENGTH,
                25);
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f4." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.RANDOM);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f4." + DataGenConnectorOptionsUtil.LENGTH,
                9999);

        results = runGenerator(LENGTH_CONSTRAINED_SCHEMA, descriptor);

        for (RowData row : results) {
            assertThat(row.getString(2).toString()).hasSize(25);
            assertThat(row.getString(4).toString()).hasSize(9999);
        }

        assertException(
                LENGTH_CONSTRAINED_SCHEMA,
                descriptor,
                "f3",
                21,
                null,
                "Custom length '21' for variable-length type (VARCHAR/STRING/VARBINARY/BYTES) field 'f3' should be shorter than '20' defined in the schema.");
    }

    @Test
    void testLengthForCollectionType() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        final int rowsNumber = 200;
        final int collectionSize = 10;
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), rowsNumber);
        // test for default length.
        List<RowData> results = runGenerator(COLLECTION_SCHEMA, descriptor);
        assertThat(results).hasSize(rowsNumber);
        for (RowData row : results) {
            assertThat(row.getArray(0).size()).isEqualTo(RANDOM_COLLECTION_LENGTH_DEFAULT);
            assertThat(row.getMap(1).size())
                    .isEqualTo(RandomGeneratorVisitor.RANDOM_COLLECTION_LENGTH_DEFAULT);
            assertThat(row.getMap(2).size())
                    .isEqualTo(RandomGeneratorVisitor.RANDOM_COLLECTION_LENGTH_DEFAULT);
        }

        // test for provided length.
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.LENGTH,
                collectionSize);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f1." + DataGenConnectorOptionsUtil.LENGTH,
                collectionSize);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f2." + DataGenConnectorOptionsUtil.LENGTH,
                collectionSize);
        results = runGenerator(COLLECTION_SCHEMA, descriptor);
        assertThat(results).hasSize(rowsNumber);
        for (RowData row : results) {
            assertThat(row.getArray(0).size()).isEqualTo(collectionSize);
            assertThat(row.getMap(1).size()).isEqualTo(collectionSize);
            assertThat(row.getMap(2).size()).isEqualTo(collectionSize);
        }
    }

    @Test
    void testFixedLengthDataType() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        final int rowsNumber = 200;
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), rowsNumber);

        List<RowData> results = runGenerator(LENGTH_CONSTRAINED_SCHEMA, descriptor);
        assertThat(results).hasSize(rowsNumber);

        for (RowData row : results) {
            assertThat(row.getString(0).toString()).hasSize(50);
            assertThat(row.getBinary(1)).hasSize(40);
        }

        assertException(
                LENGTH_CONSTRAINED_SCHEMA,
                descriptor,
                "f0",
                20,
                null,
                "Custom length for fixed-length type (CHAR/BINARY) field 'f0' is not supported.");
    }

    /**
     * Drives the {@link DataGenTableSource}'s {@link GeneratorFunction} directly, exercising the
     * factory wiring and per-field generator behavior end-to-end without needing a running cluster.
     * Counts are bounded by {@link DataGenTableSource#computeEffectiveCount()} which preserves the
     * legacy behavior of halting at the smallest configured sequence range when {@code
     * number-of-rows} is not set.
     */
    private List<RowData> runGenerator(ResolvedSchema schema, DescriptorProperties descriptor)
            throws Exception {
        DynamicTableSource source = createTableSource(schema, descriptor.asMap());

        assertThat(source).isInstanceOf(DataGenTableSource.class);

        DataGenTableSource dataGenTableSource = (DataGenTableSource) source;
        GeneratorFunction<Long, RowData> rowGenerator = dataGenTableSource.buildRowGenerator();
        rowGenerator.open(stubReaderContext());

        long count = dataGenTableSource.computeEffectiveCount();
        // Cap the number of generated rows to a sane upper bound when no explicit limit was
        // configured (e.g. unbounded random-only sources). Tests that depend on a specific count
        // always set NUMBER_OF_ROWS explicitly.
        long limit = count == Long.MAX_VALUE ? DEFAULT_UNBOUNDED_TEST_LIMIT : count;

        List<RowData> results = new ArrayList<>();
        for (long i = 0; i < limit; i++) {
            results.add(rowGenerator.map(i));
        }
        return results;
    }

    @Test
    void testSequenceProducesEachValueExactlyOnce() throws Exception {
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

        DataGenTableSource source = (DataGenTableSource) dynamicTableSource;

        // The factory wires a SequenceGeneratorFunction[0..100] for f0; with no number-of-rows
        // configured the effective bound matches the sequence range, so every value in [0,100]
        // is produced exactly once when the source is driven across indices [0, count).
        assertThat(source.computeEffectiveCount()).isEqualTo(101L);
        assertThat(source.getFieldGenerators()[0]).isInstanceOf(SequenceGeneratorFunction.class);
        assertThat(((SequenceGeneratorFunction<?>) source.getFieldGenerators()[0]).getTotalCount())
                .isEqualTo(101L);

        List<RowData> results =
                runGenerator(
                        ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())), descriptor);

        Set<Long> emitted = new HashSet<>();
        for (RowData row : results) {
            emitted.add(row.getLong(0));
        }
        Set<Long> expected = new HashSet<>();
        for (long i = 0; i <= 100; i++) {
            expected.add(i);
        }
        assertThat(emitted).isEqualTo(expected);
        assertThat(results).hasSize(101);
    }

    @Test
    void testRowGeneratorIsSerializable() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.START, 0);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.END, 10);

        ResolvedSchema schema = ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT()));
        DataGenTableSource source =
                (DataGenTableSource) createTableSource(schema, descriptor.asMap());

        // The generator functions are shipped to the TaskManagers, so they must survive Java
        // serialization. Clone the freshly built generator and verify the round-tripped copy still
        // produces the full [0, 10] sequence.
        GeneratorFunction<Long, RowData> cloned =
                InstantiationUtil.clone(source.buildRowGenerator());
        cloned.open(stubReaderContext());

        List<Long> emitted = new ArrayList<>();
        for (long i = 0; i < source.computeEffectiveCount(); i++) {
            emitted.add(cloned.map(i).getLong(0));
        }
        assertThat(emitted).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    @Test
    void testEffectiveCountIsCappedByRowCountWhenSmaller() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), 5);
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.START, 0);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.END, 100);

        DataGenTableSource source =
                (DataGenTableSource)
                        createTableSource(
                                ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT())),
                                descriptor.asMap());

        // number-of-rows is the tighter bound, so it wins over the sequence range [0,100].
        assertThat(source.computeEffectiveCount()).isEqualTo(5L);
    }

    @Test
    void testEffectiveCountIsCappedBySequenceWhenRowCountLarger() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), 1000);
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.START, 1);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.END, 10);

        ResolvedSchema schema = ResolvedSchema.of(Column.physical("f0", DataTypes.TINYINT()));
        DataGenTableSource source =
                (DataGenTableSource) createTableSource(schema, descriptor.asMap());

        // The sequence range [1,10] is the tighter bound; without the cap the source would request
        // indices up to 999 and SequenceGeneratorFunction.map(idx) would silently wrap past TINYINT
        // bounds via (byte) (start + idx).
        assertThat(source.computeEffectiveCount()).isEqualTo(10L);

        List<RowData> results = runGenerator(schema, descriptor);
        assertThat(results).hasSize(10);
        Set<Byte> emitted = new HashSet<>();
        for (RowData row : results) {
            emitted.add(row.getByte(0));
        }
        Set<Byte> expected = new HashSet<>();
        for (byte i = 1; i <= 10; i++) {
            expected.add(i);
        }
        assertThat(emitted).isEqualTo(expected);
    }

    @Test
    void testNumberOfRowsZero() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putLong(DataGenConnectorOptions.NUMBER_OF_ROWS.key(), 0);

        ResolvedSchema schema = ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT()));
        DataGenTableSource source =
                (DataGenTableSource) createTableSource(schema, descriptor.asMap());

        assertThat(source.computeEffectiveCount()).isZero();
        assertThat(runGenerator(schema, descriptor)).isEmpty();
    }

    @Test
    void testSequenceStartEqualsEnd() throws Exception {
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "datagen");
        descriptor.putString(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.KIND,
                DataGenConnectorOptionsUtil.SEQUENCE);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.START,
                42);
        descriptor.putLong(
                DataGenConnectorOptionsUtil.FIELDS + ".f0." + DataGenConnectorOptionsUtil.END, 42);

        ResolvedSchema schema = ResolvedSchema.of(Column.physical("f0", DataTypes.BIGINT()));
        DataGenTableSource source =
                (DataGenTableSource) createTableSource(schema, descriptor.asMap());

        assertThat(source.computeEffectiveCount()).isEqualTo(1L);
        assertThat(((SequenceGeneratorFunction<?>) source.getFieldGenerators()[0]).getTotalCount())
                .isEqualTo(1L);

        List<RowData> results = runGenerator(schema, descriptor);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getLong(0)).isEqualTo(42L);
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

    @Test
    void testWithParallelism() {
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("f0", DataTypes.CHAR(1)));

        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "datagen");
        options.put(DataGenConnectorOptions.SOURCE_PARALLELISM.key(), "10");

        DynamicTableSource source = createTableSource(schema, options);
        assertThat(source).isInstanceOf(DataGenTableSource.class);

        DataGenTableSource dataGenTableSource = (DataGenTableSource) source;
        assertThat(dataGenTableSource.getParallelism()).isEqualTo(10);
    }

    private void assertException(
            ResolvedSchema schema,
            DescriptorProperties descriptor,
            String fieldName,
            @Nullable Integer len,
            @Nullable Boolean varLen,
            String expectedMessage) {
        assertThatThrownBy(
                        () -> {
                            descriptor.putString(
                                    String.join(
                                            ".",
                                            DataGenConnectorOptionsUtil.FIELDS,
                                            fieldName,
                                            DataGenConnectorOptionsUtil.KIND),
                                    DataGenConnectorOptionsUtil.RANDOM);
                            if (len != null) {
                                descriptor.putLong(
                                        String.join(
                                                ".",
                                                DataGenConnectorOptionsUtil.FIELDS,
                                                fieldName,
                                                DataGenConnectorOptionsUtil.LENGTH),
                                        len);
                            }
                            if (varLen != null) {
                                descriptor.putBoolean(
                                        String.join(
                                                ".",
                                                DataGenConnectorOptionsUtil.FIELDS,
                                                fieldName,
                                                DataGenConnectorOptionsUtil.VAR_LEN),
                                        varLen);
                            }

                            runGenerator(schema, descriptor);
                        })
                .satisfies(anyCauseMatches(ValidationException.class, expectedMessage));
    }

    /**
     * Returns a {@link SourceReaderContext} suitable for opening generator functions in unit tests.
     * The generator implementations only call {@code open} for one-time initialization (e.g. {@code
     * RandomGeneratorFunction} instantiating its {@code RandomDataGenerator}); they do not invoke
     * any methods on the context, so a {@code null}-returning stub is sufficient.
     */
    private static SourceReaderContext stubReaderContext() {
        return null;
    }
}
