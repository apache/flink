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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.UUID;

/** {@link TableTestProgram}s for the {@link DataTypes#UUID()} type. */
public class UuidTestPrograms {

    private static final String LITERAL_A = "550e8400-e29b-41d4-a716-446655440000";
    private static final String LITERAL_B = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    private static final UUID UUID_A = UUID.fromString(LITERAL_A);
    private static final UUID UUID_B = UUID.fromString(LITERAL_B);

    // Boundary values around the signed/unsigned byte split at 0x7f/0x80. Under the unsigned
    // big-endian ordering of the type: ZERO < HIGH_BIT < MAX. A signed byte comparison would
    // instead rank HIGH_BIT (0x80 = -128) as the smallest, so these values make the two orderings
    // disagree and let the tests pin down the unsigned semantics.
    private static final String LITERAL_ZERO = "00000000-0000-0000-0000-000000000000";
    private static final String LITERAL_HIGH_BIT = "80000000-0000-0000-0000-000000000000";
    private static final String LITERAL_MAX = "ffffffff-ffff-ffff-ffff-ffffffffffff";
    private static final UUID UUID_ZERO = UUID.fromString(LITERAL_ZERO);
    private static final UUID UUID_HIGH_BIT = UUID.fromString(LITERAL_HIGH_BIT);
    private static final UUID UUID_MAX = UUID.fromString(LITERAL_MAX);

    private static SourceTestStep singleRowDriver() {
        return SourceTestStep.newBuilder("t").addSchema("d INT").producedValues(Row.of(1)).build();
    }

    public static final TableTestProgram UUID_SOURCE_SINK =
            TableTestProgram.of("uuid-source-sink", "round-trips a UUID column including null")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("id UUID")
                                    .producedValues(Row.of(UUID_A), Row.of(UUID_B), new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id UUID")
                                    .consumedValues(Row.of(UUID_A), Row.of(UUID_B), new Row(1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id FROM t")
                    .build();

    public static final TableTestProgram UUID_LITERAL =
            TableTestProgram.of("uuid-literal", "materializes a UUID literal")
                    .setupTableSource(singleRowDriver())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("u UUID")
                                    .consumedValues(Row.of(UUID_A))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT UUID '" + LITERAL_A + "' FROM t")
                    .build();

    public static final TableTestProgram UUID_ARRAY =
            TableTestProgram.of("uuid-array", "reads UUID array elements")
                    .setupTableSource(singleRowDriver())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("arr ARRAY<UUID>")
                                    .consumedValues(Row.of((Object) new UUID[] {UUID_A, UUID_B}))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT ARRAY[UUID '"
                                    + LITERAL_A
                                    + "', UUID '"
                                    + LITERAL_B
                                    + "'] FROM t")
                    .build();

    public static final TableTestProgram UUID_MAP =
            TableTestProgram.of("uuid-map", "reads a UUID map value")
                    .setupTableSource(singleRowDriver())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("m MAP<STRING, UUID>")
                                    .consumedValues(Row.of(Map.of("a", UUID_A)))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT MAP['a', UUID '" + LITERAL_A + "'] FROM t")
                    .build();

    public static final TableTestProgram UUID_NESTED_ROW =
            TableTestProgram.of("uuid-nested-row", "reads a UUID field of a nested row")
                    .setupTableSource(singleRowDriver())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r ROW<f0 UUID, f1 INT>")
                                    .consumedValues(Row.of(Row.of(UUID_A, 42)))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT (UUID '" + LITERAL_A + "', 42) FROM t")
                    .build();

    private static SourceTestStep comparisonSource() {
        return SourceTestStep.newBuilder("t")
                .addSchema("a UUID", "b UUID")
                .producedValues(
                        Row.of(UUID_ZERO, UUID_ZERO),
                        Row.of(UUID_ZERO, UUID_HIGH_BIT),
                        Row.of(UUID_HIGH_BIT, UUID_ZERO),
                        Row.of(UUID_HIGH_BIT, UUID_MAX),
                        Row.of(UUID_MAX, UUID_MAX))
                .build();
    }

    public static final TableTestProgram UUID_EQUALITY =
            TableTestProgram.of("uuid-equality", "compares two UUID columns for equality")
                    .setupTableSource(comparisonSource())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a UUID")
                                    .consumedValues(Row.of(UUID_ZERO), Row.of(UUID_MAX))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a FROM t WHERE a = b")
                    .build();

    // Both 0x00 < 0x80 and 0x80 < 0xff hold under unsigned ordering. A signed byte comparison would
    // drop the first pair (0x00 < 0x80 becomes 0 < -128), so this pins down the unsigned semantics.
    public static final TableTestProgram UUID_COMPARISON =
            TableTestProgram.of(
                            "uuid-comparison",
                            "compares two UUID columns using unsigned big-endian byte ordering")
                    .setupTableSource(comparisonSource())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a UUID", "b UUID")
                                    .consumedValues(
                                            Row.of(UUID_ZERO, UUID_HIGH_BIT),
                                            Row.of(UUID_HIGH_BIT, UUID_MAX))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b FROM t WHERE a < b")
                    .build();

    public static final TableTestProgram UUID_LITERAL_FILTER =
            TableTestProgram.of(
                            "uuid-literal-filter",
                            "filters UUID rows with a range predicate against a UUID literal")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("id UUID")
                                    .producedValues(
                                            Row.of(UUID_ZERO),
                                            Row.of(UUID_HIGH_BIT),
                                            Row.of(UUID_MAX),
                                            new Row(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id UUID")
                                    .consumedValues(Row.of(UUID_HIGH_BIT), Row.of(UUID_MAX))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT id FROM t "
                                    + "WHERE id > UUID '7fffffff-ffff-ffff-ffff-ffffffffffff'")
                    .build();

    public static final TableTestProgram UUID_INVALID_LITERAL =
            TableTestProgram.of("uuid-invalid-literal", "rejects a malformed UUID literal")
                    .setupTableSource(singleRowDriver())
                    .runFailingSql(
                            "SELECT UUID 'abcd' FROM t",
                            ValidationException.class,
                            "Invalid UUID string: abcd")
                    .build();

    private static SourceTestStep groupingSource() {
        return SourceTestStep.newBuilder("t")
                .addSchema("id UUID")
                .producedValues(
                        Row.of(UUID_ZERO),
                        Row.of(UUID_ZERO),
                        Row.of(UUID_A),
                        Row.of(UUID_MAX),
                        Row.of(UUID_MAX),
                        Row.of(UUID_MAX))
                .build();
    }

    // The row number encodes the sort position, so a set comparison still pins down the order:
    // ZERO < UUID_A (0x55) < HIGH_BIT (0x80) < MAX (0xff) holds only under the unsigned ordering. A
    // bounded Top-N (rn <= 4) is used instead of a plain ORDER BY so the program is also valid in
    // streaming mode, where a global sort on a non-time attribute is not supported.
    public static final TableTestProgram UUID_ORDER_BY =
            TableTestProgram.of(
                            "uuid-order-by",
                            "orders UUID rows by the unsigned big-endian byte comparison")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("id UUID")
                                    .producedValues(
                                            Row.of(UUID_MAX),
                                            Row.of(UUID_ZERO),
                                            Row.of(UUID_HIGH_BIT),
                                            Row.of(UUID_A))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id UUID", "rn BIGINT", "PRIMARY KEY (rn) NOT ENFORCED")
                                    .testMaterializedData()
                                    .consumedValues(
                                            Row.of(UUID_ZERO, 1L),
                                            Row.of(UUID_A, 2L),
                                            Row.of(UUID_HIGH_BIT, 3L),
                                            Row.of(UUID_MAX, 4L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT id, rn FROM "
                                    + "(SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) "
                                    + "WHERE rn <= 4")
                    .build();

    public static final TableTestProgram UUID_GROUP_BY =
            TableTestProgram.of("uuid-group-by", "groups rows by a UUID key")
                    .setupTableSource(groupingSource())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id UUID",
                                            "cnt BIGINT",
                                            "PRIMARY KEY (id) NOT ENFORCED")
                                    .testMaterializedData()
                                    .consumedValues(
                                            Row.of(UUID_ZERO, 2L),
                                            Row.of(UUID_A, 1L),
                                            Row.of(UUID_MAX, 3L))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, COUNT(*) FROM t GROUP BY id")
                    .build();

    public static final TableTestProgram UUID_JOIN =
            TableTestProgram.of("uuid-join", "joins two tables on a UUID key")
                    .setupTableSource(
                            SourceTestStep.newBuilder("l")
                                    .addSchema("id UUID")
                                    .producedValues(
                                            Row.of(UUID_ZERO),
                                            Row.of(UUID_A),
                                            Row.of(UUID_HIGH_BIT),
                                            Row.of(UUID_MAX))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("r")
                                    .addSchema("id UUID", "tag STRING")
                                    .producedValues(
                                            Row.of(UUID_A, "a"),
                                            Row.of(UUID_HIGH_BIT, "h"),
                                            Row.of(UUID_MAX, "m"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id UUID", "tag STRING")
                                    .consumedValues(
                                            Row.of(UUID_A, "a"),
                                            Row.of(UUID_HIGH_BIT, "h"),
                                            Row.of(UUID_MAX, "m"))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT l.id, r.tag FROM l JOIN r ON l.id = r.id")
                    .build();
}
