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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.operators.values.ValuesSource;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ValuesSource} that runs the code of {@link ValuesCodeGenerator}. */
class ValuesCodeGeneratorTest {

    private static final FlinkTypeFactory TYPE_FACTORY =
            new FlinkTypeFactory(
                    Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

    private static final RowType ROW_TYPE = RowType.of(new IntType(false), new CharType(false, 1));

    @Test
    void testEmitsEachRow() throws Exception {
        ValuesSource source = source();
        assertThat(read(source, assignedSplits(source), Integer.MAX_VALUE).rows)
                .containsExactly("1,a", "2,b", "3,c");
    }

    @Test
    void testRestoreEmitsOnlyTheRemainingRows() throws Exception {
        ValuesSource source = source();
        Read first = read(source, assignedSplits(source), 1);
        assertThat(first.rows).containsExactly("1,a");

        Read second =
                read(source, serializeAndDeserialize(source, first.snapshot), Integer.MAX_VALUE);
        assertThat(second.rows).containsExactly("2,b", "3,c");
        assertThat(second.snapshot).isEmpty();
    }

    /** A source for {@code VALUES (1, 'a'), (2, 'b'), (3, 'c')}. */
    private static ValuesSource source() {
        RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
        List<List<RexLiteral>> tuples = new ArrayList<>();
        for (String[] row : new String[][] {{"1", "a"}, {"2", "b"}, {"3", "c"}}) {
            tuples.add(
                    Arrays.asList(
                            rexBuilder.makeExactLiteral(
                                    new BigDecimal(row[0]),
                                    TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)),
                            rexBuilder.makeLiteral(row[1])));
        }
        return new ValuesSource(
                ValuesCodeGenerator.generatorFunction(
                        TableConfig.getDefault(),
                        Thread.currentThread().getContextClassLoader(),
                        ROW_TYPE,
                        tuples,
                        "TestValues"),
                InternalTypeInfo.of(ROW_TYPE),
                tuples.size());
    }

    /** The splits that the enumerator assigns to the single reader. */
    private static List<NumberSequenceSplit> assignedSplits(ValuesSource source) throws Exception {
        try (MockSplitEnumeratorContext<NumberSequenceSplit> context =
                        new MockSplitEnumeratorContext<>(1);
                SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> enumerator =
                        source.createEnumerator(context)) {
            enumerator.start();
            context.registerReader(new ReaderInfo(0, "localhost"));
            enumerator.addReader(0);
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(0, "localhost");
            assertThat(context.hasNoMoreSplits(0)).isTrue();
            return context.getSplitsAssignmentSequence().stream()
                    .flatMap(assignment -> assignment.assignment().get(0).stream())
                    .collect(Collectors.toList());
        }
    }

    /** The reader state as it comes back from a checkpoint. */
    private static List<NumberSequenceSplit> serializeAndDeserialize(
            ValuesSource source, List<NumberSequenceSplit> splits) throws Exception {
        SimpleVersionedSerializer<NumberSequenceSplit> serializer = source.getSplitSerializer();
        List<NumberSequenceSplit> restored = new ArrayList<>();
        for (NumberSequenceSplit split : splits) {
            restored.add(
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(split)));
        }
        return restored;
    }

    /** Reads at most {@code maxRows} rows from the splits, then takes a snapshot. */
    private static Read read(ValuesSource source, List<NumberSequenceSplit> splits, int maxRows)
            throws Exception {
        TestingReaderOutput<RowData> output = new TestingReaderOutput<>();
        List<NumberSequenceSplit> snapshot;
        try (SourceReader<RowData, NumberSequenceSplit> reader =
                source.createReader(new TestingReaderContext())) {
            reader.start();
            reader.addSplits(splits);
            reader.notifyNoMoreSplits();
            InputStatus status = InputStatus.MORE_AVAILABLE;
            while (output.getEmittedRecords().size() < maxRows
                    && status != InputStatus.END_OF_INPUT) {
                status = reader.pollNext(output);
                if (status == InputStatus.NOTHING_AVAILABLE) {
                    reader.isAvailable().get();
                }
            }
            snapshot = reader.snapshotState(1L);
        }
        // The emitted rows are read after all of them were emitted, so a row object that the
        // generated code reused for the next row would show up with the values of the last row.
        List<String> rows =
                output.getEmittedRecords().stream()
                        .map(row -> row.getInt(0) + "," + row.getString(1))
                        .collect(Collectors.toList());
        return new Read(rows, snapshot);
    }

    private static final class Read {
        final List<String> rows;
        final List<NumberSequenceSplit> snapshot;

        Read(List<String> rows, List<NumberSequenceSplit> snapshot) {
            this.rows = rows;
            this.snapshot = snapshot;
        }
    }
}
