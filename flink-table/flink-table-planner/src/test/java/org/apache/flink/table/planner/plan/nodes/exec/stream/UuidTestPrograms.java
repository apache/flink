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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

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

    private static SourceTestStep singleRowDriver() {
        return SourceTestStep.newBuilder("t").addSchema("d INT").producedValues(Row.of(1)).build();
    }

    static final TableTestProgram UUID_SOURCE_SINK =
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

    static final TableTestProgram UUID_LITERAL =
            TableTestProgram.of("uuid-literal", "materializes a UUID literal")
                    .setupTableSource(singleRowDriver())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("u UUID")
                                    .consumedValues(Row.of(UUID_A))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT UUID '" + LITERAL_A + "' FROM t")
                    .build();

    static final TableTestProgram UUID_ARRAY =
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

    static final TableTestProgram UUID_MAP =
            TableTestProgram.of("uuid-map", "reads a UUID map value")
                    .setupTableSource(singleRowDriver())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("m MAP<STRING, UUID>")
                                    .consumedValues(Row.of(Map.of("a", UUID_A)))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT MAP['a', UUID '" + LITERAL_A + "'] FROM t")
                    .build();

    static final TableTestProgram UUID_NESTED_ROW =
            TableTestProgram.of("uuid-nested-row", "reads a UUID field of a nested row")
                    .setupTableSource(singleRowDriver())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r ROW<f0 UUID, f1 INT>")
                                    .consumedValues(Row.of(Row.of(UUID_A, 42)))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT (UUID '" + LITERAL_A + "', 42) FROM t")
                    .build();

    static final TableTestProgram UUID_INVALID_LITERAL =
            TableTestProgram.of("uuid-invalid-literal", "rejects a malformed UUID literal")
                    .setupTableSource(singleRowDriver())
                    .runFailingSql(
                            "SELECT UUID 'abcd' FROM t",
                            ValidationException.class,
                            "Invalid UUID string: abcd")
                    .build();
}
