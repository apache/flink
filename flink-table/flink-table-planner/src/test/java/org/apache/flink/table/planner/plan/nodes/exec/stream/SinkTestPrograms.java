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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.LocalDateTime;

/** Tests for verifying sink semantics. */
public class SinkTestPrograms {

    public static final TableTestProgram INSERT_RETRACT_WITHOUT_PK =
            TableTestProgram.of(
                            "insert-retract-without-pk",
                            "The sink accepts retract input. Retract is directly passed through.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("name STRING", "score INT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 3),
                                            Row.ofKind(RowKind.INSERT, "Bob", 5),
                                            Row.ofKind(RowKind.INSERT, "Bob", 6),
                                            Row.ofKind(RowKind.INSERT, "Charly", 33))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "score BIGINT")
                                    .addOption("sink-changelog-mode-enforced", "I,UB,UA,D")
                                    .consumedValues(
                                            "+I[Alice, 3]",
                                            "+I[Bob, 5]",
                                            "-U[Bob, 5]",
                                            "+U[Bob, 11]",
                                            "+I[Charly, 33]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT name, SUM(score) FROM source_t GROUP BY name")
                    .build();

    public static final TableTestProgram INSERT_RETRACT_WITH_PK =
            TableTestProgram.of(
                            "insert-retract-with-pk",
                            "The sink accepts retract input. Although upsert keys (name) and primary keys (UPPER(name))"
                                    + "don't match, the retract changelog is passed through.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("name STRING", "score INT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 3),
                                            Row.ofKind(RowKind.INSERT, "Bob", 5),
                                            Row.ofKind(RowKind.INSERT, "Bob", 6),
                                            Row.ofKind(RowKind.INSERT, "Charly", 33))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addOption("sink-changelog-mode-enforced", "I,UB,UA,D")
                                    .consumedValues(
                                            "+I[ALICE, 3]",
                                            "+I[BOB, 5]",
                                            "-U[BOB, 5]",
                                            "+U[BOB, 11]",
                                            "+I[CHARLY, 33]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT UPPER(name), SUM(score) FROM source_t GROUP BY name")
                    .build();

    // --- ON CONFLICT tests ---

    public static final TableTestProgram ON_CONFLICT_DO_NOTHING_KEEPS_FIRST =
            TableTestProgram.of(
                            "sink-on-conflict-do-nothing-keeps-first",
                            "ON CONFLICT DO NOTHING keeps the first record when multiple records have the same PK.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "ts TIMESTAMP(3)",
                                            "WATERMARK FOR ts AS ts")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    1,
                                                    10L,
                                                    LocalDateTime.of(2024, 1, 1, 0, 0, 1)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    1,
                                                    20L,
                                                    LocalDateTime.of(2024, 1, 1, 0, 0, 2)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    2,
                                                    30L,
                                                    LocalDateTime.of(2024, 1, 1, 0, 0, 3)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "b BIGINT")
                                    .consumedValues("+I[1, 10]", "+I[2, 30]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b FROM source_t ON CONFLICT DO NOTHING")
                    .build();

    public static final TableTestProgram ON_CONFLICT_DO_ERROR_NO_CONFLICT =
            TableTestProgram.of(
                            "sink-on-conflict-do-error-no-conflict",
                            "ON CONFLICT DO ERROR with no conflicts passes through all records.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT",
                                            "b BIGINT",
                                            "ts TIMESTAMP(3)",
                                            "WATERMARK FOR ts AS ts")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    1,
                                                    10L,
                                                    LocalDateTime.of(2024, 1, 1, 0, 0, 1)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    2,
                                                    20L,
                                                    LocalDateTime.of(2024, 1, 1, 0, 0, 2)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "b BIGINT")
                                    .consumedValues("+I[1, 10]", "+I[2, 20]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b FROM source_t ON CONFLICT DO ERROR")
                    .build();

    public static final TableTestProgram UPSERT_KEY_DIFFERS_FROM_PK_WITHOUT_ON_CONFLICT =
            TableTestProgram.of(
                            "sink-upsert-key-differs-from-pk-without-on-conflict",
                            "When upsert key differs from sink PK, ON CONFLICT must be specified.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("person STRING", "votes INT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(Row.ofKind(RowKind.INSERT, "Alice", 10))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("award_t")
                                    .addSchema("votes BIGINT", "prize DOUBLE")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(Row.ofKind(RowKind.INSERT, 10L, 100.0))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "person STRING PRIMARY KEY NOT ENFORCED",
                                            "sum_votes BIGINT",
                                            "prize DOUBLE")
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t "
                                    + "SELECT T.person, T.sum_votes, award_t.prize FROM "
                                    + "(SELECT person, SUM(votes) AS sum_votes FROM source_t GROUP BY person) T, award_t "
                                    + "WHERE T.sum_votes = award_t.votes",
                            ValidationException.class,
                            "The query has an upsert key that differs from the primary key of the sink table")
                    .build();

    public static final TableTestProgram UPSERT_KEY_DIFFERS_FROM_PK_WITH_ON_CONFLICT =
            TableTestProgram.of(
                            "sink-upsert-key-differs-from-pk-with-on-conflict",
                            "When upsert key differs from sink PK but ON CONFLICT is specified, no error.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("person STRING", "votes INT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 10),
                                            Row.ofKind(RowKind.INSERT, "Bob", 20))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("award_t")
                                    .addSchema("votes BIGINT", "prize DOUBLE")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 10L, 100.0),
                                            Row.ofKind(RowKind.INSERT, 20L, 200.0))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "person STRING PRIMARY KEY NOT ENFORCED",
                                            "sum_votes BIGINT",
                                            "prize DOUBLE")
                                    .consumedValues("+I[Alice, 10, 100.0]", "+I[Bob, 20, 200.0]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t "
                                    + "SELECT T.person, T.sum_votes, award_t.prize FROM "
                                    + "(SELECT person, SUM(votes) AS sum_votes FROM source_t GROUP BY person) T, award_t "
                                    + "WHERE T.sum_votes = award_t.votes "
                                    + "ON CONFLICT DO DEDUPLICATE")
                    .build();

    public static final TableTestProgram UPSERT_KEY_MATCHES_PK_WITHOUT_ON_CONFLICT =
            TableTestProgram.of(
                            "sink-upsert-key-matches-pk-without-on-conflict",
                            "When upsert key matches sink PK, no ON CONFLICT is needed.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10L),
                                            Row.ofKind(RowKind.INSERT, 1, 20L),
                                            Row.ofKind(RowKind.INSERT, 2, 5L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "cnt BIGINT")
                                    .consumedValues("+I[1, 1]", "+U[1, 2]", "+I[2, 1]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, COUNT(*) AS cnt FROM source_t GROUP BY a")
                    .build();

    public static final TableTestProgram APPEND_ONLY_WITH_PK_WITHOUT_ON_CONFLICT =
            TableTestProgram.of(
                            "sink-append-only-with-pk-without-on-conflict",
                            "Append-only query to sink with PK requires ON CONFLICT to specify duplicate handling.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(Row.ofKind(RowKind.INSERT, 1, 10L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "b BIGINT")
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t",
                            ValidationException.class,
                            "The query has an upsert key that differs from the primary key of the sink table")
                    .build();

    public static final TableTestProgram APPEND_ONLY_WITH_PK_WITH_ON_CONFLICT =
            TableTestProgram.of(
                            "sink-append-only-with-pk-with-on-conflict",
                            "Append-only query to sink with PK and ON CONFLICT should not throw.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10L),
                                            Row.ofKind(RowKind.INSERT, 2, 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "b BIGINT")
                                    .consumedValues("+I[1, 10]", "+I[2, 20]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t ON CONFLICT DO DEDUPLICATE")
                    .build();

    public static final TableTestProgram UPSERT_KEY_DIFFERS_FROM_PK_WITHOUT_ON_CONFLICT_DISABLED =
            TableTestProgram.of(
                            "sink-upsert-key-differs-from-pk-without-on-conflict-disabled",
                            "When require-on-conflict config is disabled, no ON CONFLICT is needed even if upsert key differs from PK.")
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_SINK_REQUIRE_ON_CONFLICT, false)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, 1, 10L),
                                            Row.ofKind(RowKind.INSERT, 2, 20L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "b BIGINT")
                                    .consumedValues("+I[1, 10]", "+I[2, 20]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, b FROM source_t")
                    .build();

    public static final TableTestProgram ON_CONFLICT_NOT_ALLOWED_FOR_APPEND_ONLY_SINK =
            TableTestProgram.of(
                            "sink-on-conflict-not-allowed-for-append-only-sink",
                            "ON CONFLICT clause is not allowed for append-only sinks (no primary key).")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(Row.ofKind(RowKind.INSERT, 1, 10L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("sink-insert-only", "true")
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t ON CONFLICT DO NOTHING",
                            ValidationException.class,
                            "ON CONFLICT clause is only allowed for upsert sinks. "
                                    + "The sink 'default_catalog.default_database.sink_t' is not an upsert sink "
                                    + "because it only accepts INSERT (append-only) changes.")
                    .build();

    public static final TableTestProgram ON_CONFLICT_NOT_ALLOWED_FOR_RETRACT_SINK =
            TableTestProgram.of(
                            "sink-on-conflict-not-allowed-for-retract-sink",
                            "ON CONFLICT clause is not allowed for retract sinks (requires UPDATE_BEFORE).")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(Row.ofKind(RowKind.INSERT, 1, 10L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "cnt BIGINT")
                                    .addOption("sink-changelog-mode-enforced", "I,UB,UA,D")
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT a, COUNT(*) AS cnt FROM source_t GROUP BY a ON CONFLICT DO DEDUPLICATE",
                            ValidationException.class,
                            "ON CONFLICT clause is only allowed for upsert sinks. "
                                    + "The sink 'default_catalog.default_database.sink_t' is not an upsert sink "
                                    + "because it requires UPDATE_BEFORE (retract mode).")
                    .build();

    public static final TableTestProgram ON_CONFLICT_DO_NOTHING_REQUIRES_WATERMARKS =
            TableTestProgram.of(
                            "sink-on-conflict-do-nothing-requires-watermarks",
                            "ON CONFLICT DO NOTHING requires sources to have watermarks.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(Row.ofKind(RowKind.INSERT, 1, 10L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "b BIGINT")
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t ON CONFLICT DO NOTHING",
                            ValidationException.class,
                            "requires all source tables to define watermarks")
                    .build();

    public static final TableTestProgram ON_CONFLICT_DO_ERROR_REQUIRES_WATERMARKS =
            TableTestProgram.of(
                            "sink-on-conflict-do-error-requires-watermarks",
                            "ON CONFLICT DO ERROR requires sources to have watermarks.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(Row.ofKind(RowKind.INSERT, 1, 10L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT PRIMARY KEY NOT ENFORCED", "b BIGINT")
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t ON CONFLICT DO ERROR",
                            ValidationException.class,
                            "requires all source tables to define watermarks")
                    .build();
}
