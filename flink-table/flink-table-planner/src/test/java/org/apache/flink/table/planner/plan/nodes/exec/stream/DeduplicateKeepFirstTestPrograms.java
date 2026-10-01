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

    public static final TableTestProgram KEYED_KEEP_FIRST =
            TableTestProgram.of(
                            "deduplicate-keep-first-keyed-keep-first",
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

    public static final TableTestProgram KEYED_KEEP_FIRST_TABLE_API =
            TableTestProgram.of(
                            "deduplicate-keep-first-keyed-table-api",
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

    public static final TableTestProgram KEYED_KEEP_FIRST_TABLE_API_WITH_ARGS =
            TableTestProgram.of(
                            "deduplicate-keep-first-keyed-table-api-with-args",
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

    public static final TableTestProgram MULTI_KEY_INDEPENDENCE =
            TableTestProgram.of(
                            "deduplicate-keep-first-multi-key-independence",
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

    public static final TableTestProgram NULL_PARTITION_KEY =
            TableTestProgram.of(
                            "deduplicate-keep-first-null-partition-key",
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

    public static final TableTestProgram RESET_ON_TTL_OFF =
            TableTestProgram.of(
                            "deduplicate-keep-first-reset-on-ttl-off",
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

    public static final TableTestProgram RESET_TTL_ON_DUPLICATE_FALSE =
            TableTestProgram.of(
                            "deduplicate-keep-first-reset-ttl-on-duplicate-false",
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

    public static final TableTestProgram INPUT_COLUMN_NAMED_EVENT_TIME =
            TableTestProgram.of(
                            "deduplicate-keep-first-input-column-named-event-time",
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

    public static final TableTestProgram WHOLE_ROW_AS_KEY =
            TableTestProgram.of(
                            "deduplicate-keep-first-whole-row-as-key",
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

    public static final TableTestProgram EVENT_TIME_KEEP_EARLIEST =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time-keep-earliest",
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

    public static final TableTestProgram EVENT_TIME_LATER_ARRIVAL_EARLIER_WINS =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time-later-arrival-earlier-wins",
                            "a row that arrives later but carries an earlier event time replaces "
                                    + "the candidate before it is finalized")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(TIMED_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Vas", "first", Instant.ofEpochMilli(5000)),
                                            Row.of("Vas", "earlier", Instant.ofEpochMilli(2000)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_name STRING", "action STRING")
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Vas", "earlier"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT user_name, action FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram EVENT_TIME_LATE_EVENT_DROPPED =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time-late-event-dropped",
                            "a row below the watermark is dropped, even though its event time is "
                                    + "earlier than the already-finalized candidate")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(TIMED_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("Bob", "early", Instant.ofEpochMilli(1000)),
                                            Row.of("Bob", "advance", Instant.ofEpochMilli(20000)),
                                            Row.of("Bob", "late", Instant.ofEpochMilli(500)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("user_name STRING", "action STRING")
                                    .consumedValues(Row.ofKind(RowKind.INSERT, "Bob", "early"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT user_name, action FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram UPDATING_INPUT_SWALLOWED =
            TableTestProgram.of(
                            "deduplicate-keep-first-updating-input-swallowed",
                            "an updating input is deduplicated into an insert-only result: the "
                                    + "first record per key is emitted once and every later change "
                                    + "(duplicate, -U, +U, -D) is swallowed")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .addOption("changelog-mode", "I,UB,UA,D")
                                    .producedValues(
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

    public static final TableTestProgram ON_TIME_WITH_UPDATING_INPUT_FAILS =
            TableTestProgram.of(
                            "deduplicate-keep-first-on-time-with-updating-input-fails",
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
}
