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
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/** {@link TableTestProgram} definitions for testing the built-in DEDUPLICATE_KEEP_FIRST PTF. */
public class DeduplicateKeepFirstTestPrograms {

    private static final String[] USER_EVENTS_SCHEMA = {"user_name STRING", "action STRING"};

    private static final String[] TIMED_EVENTS_SCHEMA = {
        "user_name STRING",
        "action STRING",
        "ts TIMESTAMP_LTZ(3)",
        "WATERMARK FOR ts AS ts - INTERVAL '10' SECOND"
    };

    public static final TableTestProgram KEYED =
            TableTestProgram.of(
                            "deduplicate-keep-first-keyed",
                            "only the first row per PARTITION BY key is emitted")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(Row.of("Vas", "login"), Row.of("Vas", "click"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();

    public static final TableTestProgram TABLE_API =
            TableTestProgram.of(
                            "deduplicate-keep-first-table-api",
                            "the generic Table API process() call invokes the PTF, matching "
                                    + "the SQL form")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(Row.of("Vas", "login"), Row.of("Vas", "click"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("user_events")
                                            .partitionBy($("user_name"))
                                            .process("DEDUPLICATE_KEEP_FIRST"),
                            "sink")
                    .build();

    public static final TableTestProgram TABLE_API_WITH_ARGS =
            TableTestProgram.of(
                            "deduplicate-keep-first-table-api-with-args",
                            "the Table API process() call passes the optional state_ttl and "
                                    + "reset_ttl_on_duplicate as named arguments")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(Row.of("Vas", "login"), Row.of("Vas", "click"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("user_events")
                                            .partitionBy($("user_name"))
                                            .process(
                                                    "DEDUPLICATE_KEEP_FIRST",
                                                    lit(Duration.ofSeconds(5))
                                                            .asArgument("state_ttl"),
                                                    lit(false)
                                                            .asArgument("reset_ttl_on_duplicate")),
                            "sink")
                    .build();

    public static final TableTestProgram MULTI_KEY =
            TableTestProgram.of(
                            "deduplicate-keep-first-multi-key",
                            "each PARTITION BY key keeps its own first row, independently of "
                                    + "interleaving with other keys")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Vas", "login"),
                                            Row.of("Alice", "click"),
                                            Row.of("Vas", "click"),
                                            Row.of("Alice", "view"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"),
                                            Row.ofKind(RowKind.INSERT, "Alice", "click"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();

    public static final TableTestProgram MULTI_COLUMN_KEY =
            TableTestProgram.of(
                            "deduplicate-keep-first-multi-column-key",
                            "a multi-column PARTITION BY keeps the first row per composite key; "
                                    + "rows differing in either key column form distinct groups")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema("region STRING", "user_name STRING", "action STRING")
                                    .producedValues(
                                            Row.of("EU", "Vas", "login"),
                                            Row.of("EU", "Vas", "click"),
                                            Row.of("US", "Vas", "signup"),
                                            Row.of("EU", "Alice", "view"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("region STRING", "user_name STRING", "action STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, "EU", "Vas", "login"),
                                            Row.ofKind(RowKind.INSERT, "US", "Vas", "signup"),
                                            Row.ofKind(RowKind.INSERT, "EU", "Alice", "view"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY (region, user_name))")
                    .build();

    public static final TableTestProgram NULL_KEY =
            TableTestProgram.of(
                            "deduplicate-keep-first-null-key",
                            "a NULL PARTITION BY key forms its own dedup group, keeping only "
                                    + "its first row")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of(null, "login"),
                                            Row.of("Vas", "click"),
                                            Row.of(null, "click"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, null, "login"),
                                            Row.ofKind(RowKind.INSERT, "Vas", "click"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();

    public static final TableTestProgram NO_PARTITION_BY =
            TableTestProgram.of(
                            "deduplicate-keep-first-no-partition-by",
                            "without PARTITION BY the whole input is one group: only the "
                                    + "very first row is emitted, regardless of its content")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Vas", "login"),
                                            Row.of("Alice", "click"),
                                            Row.of("Bob", "view"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST(input => TABLE user_events)")
                    .build();

    public static final TableTestProgram RESET_WITH_ZERO_TTL =
            TableTestProgram.of(
                            "deduplicate-keep-first-reset-with-zero-ttl",
                            "reset_ttl_on_duplicate=TRUE with state_ttl=INTERVAL '0' (retention "
                                    + "disabled): only the first row per key is emitted")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Vas", "login"),
                                            Row.of("Vas", "click"),
                                            Row.of("Vas", "view"),
                                            Row.of("Vas", "logout"),
                                            Row.of("Vas", "login"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "state_ttl => INTERVAL '0' SECOND, "
                                    + "reset_ttl_on_duplicate => TRUE)")
                    .build();

    public static final TableTestProgram NO_TTL_RESET =
            TableTestProgram.of(
                            "deduplicate-keep-first-no-ttl-reset",
                            "reset_ttl_on_duplicate=FALSE still emits only the first row per key")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Vas", "login"),
                                            Row.of("Vas", "click"),
                                            Row.of("Vas", "view"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "state_ttl => INTERVAL '5' SECOND, "
                                    + "reset_ttl_on_duplicate => FALSE)")
                    .build();

    public static final TableTestProgram EVENT_TIME_COLUMN_CLASH =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time-column-clash",
                            "an input column named event_time is deduplicated like any other; "
                                    + "only the first row per key is emitted")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema("user_name STRING", "event_time BIGINT")
                                    .producedValues(Row.of("Vas", 100L), Row.of("Vas", 200L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_name STRING", "event_time BIGINT")
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", 100L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();

    public static final TableTestProgram WHOLE_ROW_KEY =
            TableTestProgram.of(
                            "deduplicate-keep-first-whole-row-key",
                            "PARTITION BY over every column deduplicates exact-duplicate rows, "
                                    + "keeping one row per distinct payload")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(Row.of("Vas", "login"), Row.of("Vas", "login"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY (user_name, action))")
                    .build();

    public static final TableTestProgram EVENT_TIME =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time",
                            "with on_time, the row with the smallest event time is kept per key, "
                                    + "not the first to arrive")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(TIMED_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Vas", "login", Instant.ofEpochMilli(3000)),
                                            Row.of("Vas", "click", Instant.ofEpochMilli(1000)),
                                            Row.of("Vas", "view", Instant.ofEpochMilli(5000)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_name STRING", "action STRING")
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "click"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT user_name, action FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram EVENT_TIME_MULTI_KEY =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time-multi-key",
                            "each key emits its earliest row with that row's event time as "
                                    + "rowtime; replacing one key's candidate leaves other keys' "
                                    + "timers intact, and on equal event times the first arrival "
                                    + "wins")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(TIMED_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Alice", "Hi", Instant.ofEpochMilli(1)),
                                            Row.of("Alice", "Hello", Instant.ofEpochMilli(3)),
                                            Row.of("Alice", "Hello world", Instant.ofEpochMilli(2)),
                                            Row.of("Bob", "I am fine.", Instant.ofEpochMilli(3)),
                                            Row.of("Bob", "Comment#1", Instant.ofEpochMilli(6)),
                                            Row.of("Carol", "Comment#2", Instant.ofEpochMilli(3)),
                                            Row.of("Carol", "Comment#3", Instant.ofEpochMilli(2)),
                                            Row.of("Dave", "Comment#4", Instant.ofEpochMilli(4)),
                                            Row.of("Dave", "Comment#5", Instant.ofEpochMilli(4)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "user_name STRING",
                                            "action STRING",
                                            "rowtime TIMESTAMP_LTZ(3)")
                                    .consumedValues(
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    "Alice",
                                                    "Hi",
                                                    Instant.ofEpochMilli(1)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    "Bob",
                                                    "I am fine.",
                                                    Instant.ofEpochMilli(3)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    "Carol",
                                                    "Comment#3",
                                                    Instant.ofEpochMilli(2)),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    "Dave",
                                                    "Comment#4",
                                                    Instant.ofEpochMilli(4)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT user_name, action, rowtime FROM "
                                    + "DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram EVENT_TIME_LATE_DROPPED =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time-late-dropped",
                            "a row below the watermark is dropped, even though its event time is "
                                    + "earlier than the already-finalized candidate; a late first "
                                    + "row for an unseen key is dropped without marking the key "
                                    + "as seen")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(TIMED_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Bob", "early", Instant.ofEpochMilli(1000)),
                                            Row.of("Bob", "advance", Instant.ofEpochMilli(20000)),
                                            Row.of("Bob", "late", Instant.ofEpochMilli(500)),
                                            Row.of("Carol", "late", Instant.ofEpochMilli(500)),
                                            Row.of("Carol", "on-time", Instant.ofEpochMilli(30000)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_name STRING", "action STRING")
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, "Bob", "early"),
                                            Row.ofKind(RowKind.INSERT, "Carol", "on-time"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT user_name, action FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram UPDATING_INPUT =
            TableTestProgram.of(
                            "deduplicate-keep-first-updating-input",
                            "an updating input is deduplicated into an insert-only result: a -D "
                                    + "before the key is seen is ignored, the first +I is emitted "
                                    + "once and every later change (duplicate, -U, +U, -D) is "
                                    + "swallowed")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .addOption("changelog-mode", "I,UB,UA,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.DELETE, "Vas", "logout"),
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"),
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Vas", "login"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Vas", "click"),
                                            Row.ofKind(RowKind.DELETE, "Vas", "click"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();

    public static final TableTestProgram UNSEEN_RETRACTION_IGNORED =
            TableTestProgram.of(
                            "deduplicate-keep-first-unseen-retraction-ignored",
                            "a -U or -D for a key that has not been seen is ignored and does not "
                                    + "mark the key as seen, so the next +I or +U for that key is "
                                    + "emitted as an insert")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .addOption("changelog-mode", "I,UB,UA,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.DELETE, "Vas", "logout"),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "Bob", "login"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "Bob", "click"),
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, "Bob", "click"),
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();

    public static final TableTestProgram UPDATING_EVENT_TIME_FAILS =
            TableTestProgram.of(
                            "deduplicate-keep-first-updating-event-time-fails",
                            "event-time mode (on_time) is rejected for updating input, because a "
                                    + "watermark asserts no earlier record will arrive while an "
                                    + "update can invalidate that")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(TIMED_EVENTS_SCHEMA)
                                    .addOption("changelog-mode", "I,UB,UA,D")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Vas", "login", null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_name STRING", "action STRING")
                                    .consumedValues(new Row[0])
                                    .build())
                    .runFailingSql(
                            "INSERT INTO sink SELECT user_name, action FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "on_time => DESCRIPTOR(ts))",
                            ValidationException.class,
                            "not supported for PTFs that consume or produce updates")
                    .build();

    public static final TableTestProgram NEGATIVE_TTL_FAILS =
            invalidArgumentProgram(
                    "deduplicate-keep-first-negative-ttl-fails",
                    "a negative state_ttl is rejected at planning time",
                    "state_ttl => INTERVAL '-1' SECOND",
                    "The 'state_ttl' argument must not be negative");

    public static final TableTestProgram NON_LITERAL_TTL_FAILS =
            invalidArgumentProgram(
                    "deduplicate-keep-first-non-literal-ttl-fails",
                    "a state_ttl that is not a literal is rejected at planning time",
                    "state_ttl => INTERVAL '1' SECOND * 2",
                    "The 'state_ttl' argument must be a constant INTERVAL literal.");

    public static final TableTestProgram NON_LITERAL_RESET_FAILS =
            invalidArgumentProgram(
                    "deduplicate-keep-first-non-literal-reset-fails",
                    "a reset_ttl_on_duplicate that is not a literal is rejected at planning time",
                    "reset_ttl_on_duplicate => 1 = 1",
                    "The 'reset_ttl_on_duplicate' argument must be a constant BOOLEAN literal.");

    private static TableTestProgram invalidArgumentProgram(
            final String id, final String description, final String argument, final String error) {
        return TableTestProgram.of(id, description)
                .setupTableSource(
                        SourceTestStep.newBuilder("user_events")
                                .addSchema(USER_EVENTS_SCHEMA)
                                .producedValues(Row.of("Vas", "login"))
                                .build())
                .setupTableSink(
                        SinkTestStep.newBuilder("sink")
                                .addSchema(USER_EVENTS_SCHEMA)
                                .consumedValues(new Row[0])
                                .build())
                .runFailingSql(
                        "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                + "input => TABLE user_events PARTITION BY user_name, "
                                + argument
                                + ")",
                        ValidationException.class,
                        error)
                .build();
    }
}
