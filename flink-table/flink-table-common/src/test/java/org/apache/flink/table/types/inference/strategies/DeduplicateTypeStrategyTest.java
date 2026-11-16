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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction.ChangelogContext;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_INPUT;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_KEEP;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_RESET_TTL_ON_DUPLICATE;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_SEQUENCE_BY;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.ARG_STATE_TTL;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.CHANGELOG_MODE_STRATEGY;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.INPUT_TYPE_STRATEGY;
import static org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy.OUTPUT_TYPE_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DeduplicateTypeStrategy}. */
class DeduplicateTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final DataType ROWTIME =
            new AtomicDataType(new LocalZonedTimestampType(true, TimestampKind.ROWTIME, 3));

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("user_id", DataTypes.STRING()),
                    DataTypes.FIELD("event", DataTypes.STRING()),
                    DataTypes.FIELD("ts", ROWTIME),
                    DataTypes.FIELD("lsn", DataTypes.BIGINT()),
                    DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())));

    private static final DataType[] ARGUMENT_TYPES = {
        INPUT_TYPE,
        DataTypes.STRING(),
        DataTypes.DESCRIPTOR(),
        DataTypes.INTERVAL(DataTypes.SECOND()),
        DataTypes.BOOLEAN()
    };

    private static final TableSemanticsMock PARTITIONED_BY_USER =
            new TableSemanticsMock(
                    INPUT_TYPE, new int[] {0}, new int[0], -1, ChangelogMode.insertOnly());

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                withDefaults("All optional arguments omitted", PARTITIONED_BY_USER)
                        .expectArgumentTypes(ARGUMENT_TYPES),
                withDefaults("All arguments set", PARTITIONED_BY_USER)
                        .calledWithLiteralAt(ARG_KEEP, "last")
                        .calledWithLiteralAt(ARG_SEQUENCE_BY, ColumnList.of("lsn", "ts"))
                        .calledWithLiteralAt(ARG_STATE_TTL, Duration.ofHours(1))
                        .calledWithLiteralAt(ARG_RESET_TTL_ON_DUPLICATE, false)
                        .expectArgumentTypes(ARGUMENT_TYPES),
                withDefaults("Invalid keep", PARTITIONED_BY_USER)
                        .calledWithLiteralAt(ARG_KEEP, "FIRST")
                        .expectErrorMessage(
                                "DEDUPLICATE requires keep => 'first' or keep => 'last', but "
                                        + "received keep => 'FIRST'. Use keep => 'first' to keep "
                                        + "the first record per key, or keep => 'last' to keep "
                                        + "the last one."),
                nonLiteral("Non-literal keep", ARG_KEEP)
                        .expectErrorMessage(
                                "DEDUPLICATE requires keep => 'first' or keep => 'last', but "
                                        + "received a non-literal expression."),
                withDefaults(
                                "ORDER BY",
                                new TableSemanticsMock(
                                        INPUT_TYPE,
                                        new int[] {0},
                                        new int[] {3},
                                        -1,
                                        ChangelogMode.insertOnly()))
                        .expectErrorMessage(
                                "DEDUPLICATE does not support ORDER BY on its input, but received "
                                        + "ORDER BY lsn. Use sequence_by => DESCRIPTOR(lsn) to "
                                        + "order the records of a key."),
                withDefaults(
                                "Time attribute in PARTITION BY",
                                new TableSemanticsMock(
                                        INPUT_TYPE,
                                        new int[] {2},
                                        new int[0],
                                        -1,
                                        ChangelogMode.insertOnly()))
                        .expectErrorMessage(
                                "DEDUPLICATE does not support a time attribute as a PARTITION BY "
                                        + "column, but 'ts' is one. Partition by a regular column, "
                                        + "or cast the column in a subquery first, for example "
                                        + "CAST(ts AS TIMESTAMP_LTZ(3)) AS ts."),
                TestSpec.forStrategy("on_time", INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(
                                INPUT_TYPE,
                                DataTypes.STRING(),
                                DataTypes.DESCRIPTOR(),
                                DataTypes.INTERVAL(DataTypes.SECOND()),
                                DataTypes.BOOLEAN(),
                                DataTypes.DESCRIPTOR(),
                                DataTypes.STRING())
                        .calledWithTableSemanticsAt(ARG_INPUT, PARTITIONED_BY_USER)
                        .calledWithLiteralAt(ARG_KEEP, null)
                        .calledWithLiteralAt(ARG_SEQUENCE_BY, null)
                        .calledWithLiteralAt(ARG_STATE_TTL, null)
                        .calledWithLiteralAt(ARG_RESET_TTL_ON_DUPLICATE, null)
                        .calledWithLiteralAt(5, ColumnList.of("ts"))
                        .calledWithLiteralAt(6, null)
                        .expectErrorMessage(
                                "DEDUPLICATE does not support on_time, but received on_time => "
                                        + "DESCRIPTOR(ts). Use sequence_by => DESCRIPTOR(ts) to "
                                        + "order by a timestamp column."),
                sequenceBy("Unknown sequence_by column", "nope")
                        .expectErrorMessage(
                                "The sequence_by column 'nope' does not exist in the input of "
                                        + "DEDUPLICATE. Available columns are: user_id, event, "
                                        + "ts, lsn, tags."),
                sequenceBy("Repeated sequence_by column", "lsn", "lsn")
                        .expectErrorMessage(
                                "The sequence_by column 'lsn' is listed more than once. List "
                                        + "each ordering column once."),
                sequenceBy("PARTITION BY column in sequence_by", "user_id")
                        .expectErrorMessage(
                                "The sequence_by column 'user_id' is a PARTITION BY column, "
                                        + "which has the same value for every record of a key."),
                sequenceBy("Non-orderable sequence_by column", "tags")
                        .expectErrorMessage(
                                "The sequence_by column 'tags' has type 'ARRAY<STRING>', which "
                                        + "cannot order records. Use a numeric, character "
                                        + "string, binary string, boolean, or date-time column."),
                withDefaults("Negative state_ttl", PARTITIONED_BY_USER)
                        .calledWithLiteralAt(ARG_STATE_TTL, Duration.ofSeconds(-1))
                        .expectErrorMessage(
                                "The 'state_ttl' argument must not be negative, but was '-1 s'."),
                nonLiteral("Non-literal state_ttl", ARG_STATE_TTL)
                        .expectErrorMessage(
                                "The 'state_ttl' argument must be a constant INTERVAL literal, "
                                        + "but received a non-literal expression."),
                nonLiteral("Non-literal reset_ttl_on_duplicate", ARG_RESET_TTL_ON_DUPLICATE)
                        .expectErrorMessage(
                                "The 'reset_ttl_on_duplicate' argument must be a constant "
                                        + "BOOLEAN literal, but received a non-literal "
                                        + "expression."));
    }

    @Test
    void testOutputExcludesPartitionKeysAndTimeAttributes() {
        assertThat(OUTPUT_TYPE_STRATEGY.inferType(callContext(ColumnList.of())))
                .contains(
                        DataTypes.ROW(
                                        DataTypes.FIELD("event", DataTypes.STRING()),
                                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)),
                                        DataTypes.FIELD("lsn", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "tags", DataTypes.ARRAY(DataTypes.STRING())))
                                .notNull());
    }

    @Test
    void testSequenceByIndicesPointIntoPayload() {
        assertThat(
                        DeduplicateTypeStrategy.sequenceByOutputIndices(
                                callContext(ColumnList.of("lsn", "event"))))
                .containsExactly(2, 0);
    }

    @Test
    void testChangelogModeEmitsUpdateBeforeOnlyWhenRequired() {
        assertThat(inferChangelogMode(ChangelogMode.upsert()))
                .isEqualTo(mode(RowKind.INSERT, RowKind.UPDATE_AFTER));
        assertThat(inferChangelogMode(ChangelogMode.all()))
                .isEqualTo(mode(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER));
    }

    private static TestSpec withDefaults(
            final String description, final TableSemanticsMock tableSemantics) {
        return TestSpec.forStrategy(description, INPUT_TYPE_STRATEGY)
                .calledWithArgumentTypes(ARGUMENT_TYPES)
                .calledWithTableSemanticsAt(ARG_INPUT, tableSemantics)
                .calledWithLiteralAt(ARG_KEEP, null)
                .calledWithLiteralAt(ARG_SEQUENCE_BY, null)
                .calledWithLiteralAt(ARG_STATE_TTL, null)
                .calledWithLiteralAt(ARG_RESET_TTL_ON_DUPLICATE, null);
    }

    private static TestSpec nonLiteral(final String description, final int argument) {
        final TestSpec spec =
                TestSpec.forStrategy(description, INPUT_TYPE_STRATEGY)
                        .calledWithArgumentTypes(ARGUMENT_TYPES)
                        .calledWithTableSemanticsAt(ARG_INPUT, PARTITIONED_BY_USER);
        for (int pos = ARG_KEEP; pos <= ARG_RESET_TTL_ON_DUPLICATE; pos++) {
            if (pos != argument) {
                spec.calledWithLiteralAt(pos, null);
            }
        }
        return spec;
    }

    private static TestSpec sequenceBy(final String description, final String... columns) {
        return withDefaults(description, PARTITIONED_BY_USER)
                .calledWithLiteralAt(ARG_SEQUENCE_BY, ColumnList.of(Arrays.asList(columns)));
    }

    private static CallContextMock callContext(final ColumnList sequenceBy) {
        final CallContextMock callContext = new CallContextMock();
        callContext.argumentValues =
                Arrays.asList(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(sequenceBy),
                        Optional.empty(),
                        Optional.empty());
        callContext.tableSemantics = Map.of(ARG_INPUT, PARTITIONED_BY_USER);
        return callContext;
    }

    private static ChangelogMode inferChangelogMode(final ChangelogMode requiredMode) {
        return CHANGELOG_MODE_STRATEGY.inferChangelogMode(
                new ChangelogContext() {
                    @Override
                    public ChangelogMode getTableChangelogMode(final int pos) {
                        return ChangelogMode.insertOnly();
                    }

                    @Override
                    public ChangelogMode getRequiredChangelogMode() {
                        return requiredMode;
                    }

                    @Override
                    public Optional<TableSemantics> getTableSemantics(final int pos) {
                        return Optional.empty();
                    }
                });
    }

    private static ChangelogMode mode(final RowKind... kinds) {
        final ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (final RowKind kind : kinds) {
            builder.addContainedKind(kind);
        }
        return builder.build();
    }
}
