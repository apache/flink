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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.time.Instant;

/** Restore {@link TableTestProgram}s for the built-in DEDUPLICATE_KEEP_FIRST PTF. */
public class DeduplicateKeepFirstRestoreTestPrograms {

    private static final String[] USER_EVENTS_SCHEMA = {"user_name STRING", "action STRING"};

    private static final String[] TIMED_EVENTS_SCHEMA = {
        "user_name STRING",
        "action STRING",
        "ts TIMESTAMP_LTZ(3)",
        "WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND"
    };

    public static final TableTestProgram KEYED_RESTORE =
            TableTestProgram.of(
                            "deduplicate-keep-first-keyed-restore",
                            "the keep-first 'seen' state restores via compiled plan + savepoint: "
                                    + "an already-seen key stays deduplicated after restore, and a "
                                    + "key first observed after restore is emitted")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of("Vas", "login"), Row.of("Vas", "click"))
                                    .producedAfterRestore(
                                            Row.of("Vas", "logout"), Row.of("Alice", "hello"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .consumedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Alice", "hello"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();

    public static final TableTestProgram EVENT_TIME_PENDING_RESTORE =
            TableTestProgram.of(
                            "deduplicate-keep-first-event-time-pending-restore",
                            "an event-time candidate buffered but not yet finalized (its timer has "
                                    + "not fired) survives the savepoint: after restore, the "
                                    + "watermark advances and the buffered candidate is emitted")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(TIMED_EVENTS_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of("Bob", "first", Instant.ofEpochMilli(5)))
                                    .producedAfterRestore(
                                            Row.of("Bob", "later", Instant.ofEpochMilli(100)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedBeforeRestore(new Row[0])
                                    .consumedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Bob", "first"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT user_name, action FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram TTL_EXPIRED_RESTORE =
            TableTestProgram.of(
                            "deduplicate-keep-first-ttl-expired-restore",
                            "the 'seen' state in the savepoint has outlived state_ttl by the time "
                                    + "the test restores it, so an already-seen key is emitted "
                                    + "again in a new deduplication period")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of("Vas", "login"), Row.of("Vas", "click"))
                                    .producedAfterRestore(Row.of("Vas", "logout"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .consumedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Vas", "logout"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name, "
                                    + "state_ttl => INTERVAL '10' SECOND)")
                    .build();

    public static final TableTestProgram GLOBAL_TTL_EXPIRED_RESTORE =
            TableTestProgram.of(
                            "deduplicate-keep-first-global-ttl-expired-restore",
                            "without state_ttl, the 'seen' state falls back to "
                                    + "table.exec.state.ttl and has expired by the time the test "
                                    + "restores it, so an already-seen key is emitted again")
                    .setupConfig(
                            ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofSeconds(10))
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of("Vas", "login"), Row.of("Vas", "click"))
                                    .producedAfterRestore(Row.of("Vas", "logout"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedBeforeRestore(
                                            Row.ofKind(RowKind.INSERT, "Vas", "login"))
                                    .consumedAfterRestore(
                                            Row.ofKind(RowKind.INSERT, "Vas", "logout"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE_KEEP_FIRST("
                                    + "input => TABLE user_events PARTITION BY user_name)")
                    .build();
}
