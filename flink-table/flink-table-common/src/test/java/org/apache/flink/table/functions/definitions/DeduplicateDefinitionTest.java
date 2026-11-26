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

package org.apache.flink.table.functions.definitions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.types.ColumnList;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_INPUT;
import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_KEEP;
import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_SEQUENCE_BY;
import static org.apache.flink.table.functions.definitions.DeduplicateDefinition.ARG_STATE_TTL;

/** Tests for {@link DeduplicateDefinition}. */
class DeduplicateDefinitionTest extends InputTypeStrategiesTestBase {

    private static final DataType ROWTIME =
            new AtomicDataType(new LocalZonedTimestampType(true, TimestampKind.ROWTIME, 3));

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("user_id", DataTypes.STRING()),
                    DataTypes.FIELD("event", DataTypes.STRING()),
                    DataTypes.FIELD("ts", ROWTIME),
                    DataTypes.FIELD("lsn", DataTypes.BIGINT()),
                    DataTypes.FIELD("tags", DataTypes.VARIANT()));

    private static final DataType[] ARGUMENT_TYPES = {
        INPUT_TYPE,
        DataTypes.STRING(),
        DataTypes.DESCRIPTOR(),
        DataTypes.INTERVAL(DataTypes.SECOND())
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
                        .expectArgumentTypes(ARGUMENT_TYPES),
                withDefaults("Invalid keep", PARTITIONED_BY_USER)
                        .calledWithLiteralAt(ARG_KEEP, "FIRST")
                        .expectErrorMessage(
                                "DEDUPLICATE requires keep => 'first' or keep => 'last', but "
                                        + "received keep => 'FIRST'."),
                sequenceBy("Empty sequence_by")
                        .expectErrorMessage("sequence_by requires at least one column."),
                sequenceBy("Unknown sequence_by column", "nope")
                        .expectErrorMessage(
                                "The sequence_by column 'nope' does not exist in the input of "
                                        + "DEDUPLICATE. Available columns are: user_id, event, "
                                        + "ts, lsn, tags."),
                sequenceBy("PARTITION BY column in sequence_by", "user_id")
                        .expectErrorMessage(
                                "The sequence_by column 'user_id' is a PARTITION BY column, "
                                        + "which has the same value for every record of a key."),
                sequenceBy("Non-orderable sequence_by column", "tags")
                        .expectErrorMessage(
                                "The sequence_by column 'tags' has type 'VARIANT', which "
                                        + "cannot be ordered. Use a column of a type that "
                                        + "supports ORDER BY."),
                withDefaults("Negative state_ttl", PARTITIONED_BY_USER)
                        .calledWithLiteralAt(ARG_STATE_TTL, Duration.ofSeconds(-1))
                        .expectErrorMessage(
                                "The 'state_ttl' argument must not be negative, but was '-1 s'. "
                                        + "Use INTERVAL '0' SECOND to retain state indefinitely."));
    }

    private static TestSpec withDefaults(
            final String description, final TableSemanticsMock tableSemantics) {
        return TestSpec.forStrategy(description, DeduplicateDefinition.inputTypeStrategy())
                .calledWithArgumentTypes(ARGUMENT_TYPES)
                .calledWithTableSemanticsAt(ARG_INPUT, tableSemantics)
                .calledWithLiteralAt(ARG_KEEP, null)
                .calledWithLiteralAt(ARG_SEQUENCE_BY, null)
                .calledWithLiteralAt(ARG_STATE_TTL, null);
    }

    private static TestSpec sequenceBy(final String description, final String... columns) {
        return withDefaults(description, PARTITIONED_BY_USER)
                .calledWithLiteralAt(ARG_SEQUENCE_BY, ColumnList.of(List.of(columns)));
    }
}
