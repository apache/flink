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

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/** {@link TableTestProgram} definitions for testing the built-in DEDUPLICATE PTF. */
public class DeduplicateTestPrograms {

    private static final String[] USER_EVENTS_SCHEMA = {"user_id STRING", "event STRING"};

    private static final String[] USER_EVENTS_UPSERT_SCHEMA = {
        "user_id STRING PRIMARY KEY NOT ENFORCED", "event STRING"
    };

    private static final String[] CUSTOMER_UPSERT_SCHEMA = {
        "customer_id STRING PRIMARY KEY NOT ENFORCED", "city STRING", "lsn BIGINT"
    };

    private static final String[] LABEL_UPSERT_SCHEMA = {
        "id STRING PRIMARY KEY NOT ENFORCED", "label STRING"
    };

    private static final String[] PRICES_UPSERT_SCHEMA = {
        "ticker STRING PRIMARY KEY NOT ENFORCED", "price INT", "ts TIMESTAMP_LTZ(3)"
    };

    public static final TableTestProgram ARRIVAL_ORDER =
            TableTestProgram.of(
                            "deduplicate-arrival-order",
                            "'last' replaces the kept record with every new one, 'first' drops "
                                    + "every record after the first, an identical record emits "
                                    + "nothing, and a consumer that needs the previous record "
                                    + "receives -U before +U")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("u1", "login"),
                                            Row.of("u2", "login"),
                                            Row.of("u1", "click"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("profile_updates")
                                    .addSchema("user_id STRING", "city STRING", "phone STRING")
                                    .producedValues(
                                            Row.of("u1", "Berlin", "111"),
                                            Row.of("u1", "Berlin", "111"),
                                            Row.of("u1", "Munich", null))
                                    .build())
                    .setupTableSink(
                            upsertSink(
                                    "sink_last",
                                    USER_EVENTS_UPSERT_SCHEMA,
                                    "+I[u1, login]",
                                    "+I[u2, login]",
                                    "+U[u1, click]"))
                    .setupTableSink(
                            upsertSink(
                                    "sink_first",
                                    USER_EVENTS_UPSERT_SCHEMA,
                                    "+I[u1, login]",
                                    "+I[u2, login]"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("retract_sink")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .consumedValues(
                                            "+I[u1, login]",
                                            "+I[u2, login]",
                                            "-U[u1, login]",
                                            "+U[u1, click]")
                                    .build())
                    .setupTableSink(
                            upsertSink(
                                    "latest_profiles",
                                    new String[] {
                                        "user_id STRING PRIMARY KEY NOT ENFORCED",
                                        "city STRING",
                                        "phone STRING"
                                    },
                                    "+I[u1, Berlin, 111]",
                                    "+U[u1, Munich, null]"))
                    .runSql(
                            "INSERT INTO sink_last SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE user_events PARTITION BY user_id, "
                                    + "keep => 'last')")
                    .runSql(
                            "INSERT INTO sink_first SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE user_events PARTITION BY user_id)")
                    .runSql(
                            "INSERT INTO retract_sink SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE user_events PARTITION BY user_id, "
                                    + "keep => 'last')")
                    .runSql(
                            "INSERT INTO latest_profiles SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE profile_updates PARTITION BY user_id, "
                                    + "keep => 'last')")
                    .build();

    public static final TableTestProgram INSERT_ONLY_OUTPUT =
            TableTestProgram.of(
                            "deduplicate-insert-only-output",
                            "over insert-only input, keeping the first arrival emits only inserts, "
                                    + "so the result can be written to an append-only sink, also "
                                    + "when dropping redelivered copies")
                    .setupTableSource(
                            SourceTestStep.newBuilder("user_events")
                                    .addSchema(USER_EVENTS_SCHEMA)
                                    .producedValues(
                                            Row.of("u1", "login"),
                                            Row.of("u2", "login"),
                                            Row.of("u1", "click"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("payments")
                                    .addSchema(
                                            "payment_id STRING",
                                            "amount INT",
                                            "ingested_at TIMESTAMP(3)")
                                    .producedValues(
                                            Row.of(
                                                    "p1",
                                                    50,
                                                    LocalDateTime.of(1970, 1, 1, 10, 0, 1)),
                                            Row.of(
                                                    "p2",
                                                    20,
                                                    LocalDateTime.of(1970, 1, 1, 10, 0, 2)),
                                            Row.of(
                                                    "p1",
                                                    50,
                                                    LocalDateTime.of(1970, 1, 1, 10, 0, 7)))
                                    .build())
                    .setupTableSink(
                            appendOnlySink(
                                    "first_arrivals",
                                    USER_EVENTS_SCHEMA,
                                    "+I[u1, login]",
                                    "+I[u2, login]"))
                    .setupTableSink(
                            appendOnlySink(
                                    "unique_payments",
                                    new String[] {
                                        "payment_id STRING",
                                        "amount INT",
                                        "ingested_at TIMESTAMP(3)"
                                    },
                                    "+I[p1, 50, 1970-01-01T10:00:01]",
                                    "+I[p2, 20, 1970-01-01T10:00:02]"))
                    .runSql(
                            "INSERT INTO first_arrivals SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE user_events PARTITION BY user_id)")
                    .runSql(
                            "INSERT INTO unique_payments SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE payments PARTITION BY payment_id, "
                                    + "state_ttl => INTERVAL '1' HOUR)")
                    .build();

    public static final TableTestProgram ON_TIME =
            TableTestProgram.of(
                            "deduplicate-on-time",
                            "with on_time, the first arrival keeps its rowtime, so an event-time "
                                    + "window aggregation can consume the deduplicated stream")
                    .setupTableSource(
                            SourceTestStep.newBuilder("clicks")
                                    .addSchema(
                                            "event_id STRING",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "WATERMARK FOR ts AS ts")
                                    .producedValues(
                                            Row.of("e1", at("10:01")),
                                            Row.of("e2", at("10:02")),
                                            Row.of("e1", at("10:03")),
                                            Row.of("e3", at("10:12")))
                                    .build())
                    .setupTableSink(
                            appendOnlySink(
                                    "unique_clicks_per_window",
                                    new String[] {"window_start TIMESTAMP(3)", "events BIGINT"},
                                    "+I[1970-01-01T10:00, 2]",
                                    "+I[1970-01-01T10:10, 1]"))
                    .runSql(
                            "INSERT INTO unique_clicks_per_window "
                                    + "WITH unique_clicks AS (SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE clicks PARTITION BY event_id, "
                                    + "on_time => DESCRIPTOR(ts))) "
                                    + "SELECT window_start, COUNT(*) FROM TUMBLE("
                                    + "TABLE unique_clicks, DESCRIPTOR(rowtime), "
                                    + "INTERVAL '10' MINUTES) "
                                    + "GROUP BY window_start, window_end")
                    .build();

    public static final TableTestProgram COMPOSITE_AND_NULL_KEY =
            TableTestProgram.of(
                            "deduplicate-composite-and-null-key",
                            "a PARTITION BY of non-adjacent columns is the leading output columns "
                                    + "in its own order, and a NULL key column forms its own key")
                    .setupTableSource(
                            SourceTestStep.newBuilder("sessions")
                                    .addSchema("user_id STRING", "page STRING", "device STRING")
                                    .producedValues(
                                            Row.of("u1", "a", "web"),
                                            Row.of("u1", "b", "mobile"),
                                            Row.of("u1", "c", "web"),
                                            Row.of(null, "d", "web"),
                                            Row.of(null, "e", "web"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("device STRING", "user_id STRING", "page STRING")
                                    .consumedValues(
                                            "+I[web, u1, a]",
                                            "+I[mobile, u1, b]",
                                            "-U[web, u1, a]",
                                            "+U[web, u1, c]",
                                            "+I[web, null, d]",
                                            "-U[web, null, d]",
                                            "+U[web, null, e]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE sessions PARTITION BY (device, user_id), "
                                    + "keep => 'last')")
                    .build();

    public static final TableTestProgram SEQUENCE_BY =
            TableTestProgram.of(
                            "deduplicate-sequence-by",
                            "with sequence_by, 'last' keeps the highest and 'first' the lowest "
                                    + "value, a tie goes to the later arrival for 'last' and to "
                                    + "the earlier one for 'first', later columns break ties, a "
                                    + "NULL orders below every other value, NaN ties with every "
                                    + "value, and a watermarked column orders without watermark "
                                    + "semantics and is emitted as a regular timestamp")
                    .setupTableSource(
                            SourceTestStep.newBuilder("customer_changes")
                                    .addSchema("customer_id STRING", "city STRING", "lsn BIGINT")
                                    .producedValues(
                                            Row.of("c1", "Berlin", 100L),
                                            Row.of("c1", "Munich", 120L),
                                            Row.of("c1", "Hamburg", 110L))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("prices")
                                    .addSchema(
                                            "ticker STRING",
                                            "price INT",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "WATERMARK FOR ts AS ts - INTERVAL '1' HOUR")
                                    .producedValues(
                                            Row.of("AAPL", 10, at("10:02")),
                                            Row.of("MSFT", 20, at("10:01")),
                                            Row.of("AAPL", 12, at("10:08")),
                                            Row.of("AAPL", 11, at("10:05")),
                                            Row.of("AAPL", 9, at("10:00")),
                                            Row.of("AAPL", 14, at("10:08")),
                                            Row.of("AAPL", 8, at("10:00")))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("measurements")
                                    .addSchema("id STRING", "v DOUBLE", "label STRING")
                                    .producedValues(
                                            Row.of("m1", 1.0, "r1"),
                                            Row.of("m1", Double.NaN, "r2"),
                                            Row.of("m1", 0.5, "r3"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("versions")
                                    .addSchema(
                                            "id STRING",
                                            "s STRING",
                                            "b BOOLEAN",
                                            "t TIMESTAMP(3)",
                                            "y BYTES",
                                            "label STRING")
                                    .producedValues(
                                            versionRow("a", false, 1, 1, "r1"),
                                            versionRow("b", false, 1, 1, "r2"),
                                            versionRow("a", true, 1, 1, "r3"),
                                            versionRow("b", true, 1, 1, "r4"),
                                            versionRow("b", true, 0, 9, "r5"),
                                            versionRow("b", true, 2, 1, "r6"),
                                            versionRow("b", true, 2, 0, "r7"),
                                            versionRow("b", true, 2, 2, "r8"),
                                            versionRow(null, true, 9, 9, "r9"),
                                            versionRow(null, true, 9, 9, "r10"),
                                            versionRow("b", true, 2, 2, "r11"),
                                            versionRow("b", true, 2, null, "r12"))
                                    .build())
                    .setupTableSink(
                            upsertSink(
                                    "customers_last",
                                    CUSTOMER_UPSERT_SCHEMA,
                                    "+I[c1, Berlin, 100]",
                                    "+U[c1, Munich, 120]"))
                    .setupTableSink(
                            upsertSink(
                                    "prices_last",
                                    PRICES_UPSERT_SCHEMA,
                                    "+I[AAPL, 10, 1970-01-01T10:02:00Z]",
                                    "+I[MSFT, 20, 1970-01-01T10:01:00Z]",
                                    "+U[AAPL, 12, 1970-01-01T10:08:00Z]",
                                    "+U[AAPL, 14, 1970-01-01T10:08:00Z]"))
                    .setupTableSink(
                            upsertSink(
                                    "prices_first",
                                    PRICES_UPSERT_SCHEMA,
                                    "+I[AAPL, 10, 1970-01-01T10:02:00Z]",
                                    "+I[MSFT, 20, 1970-01-01T10:01:00Z]",
                                    "+U[AAPL, 9, 1970-01-01T10:00:00Z]"))
                    .setupTableSink(
                            upsertSink(
                                    "measurements_last",
                                    LABEL_UPSERT_SCHEMA,
                                    "+I[m1, r1]",
                                    "+U[m1, r2]",
                                    "+U[m1, r3]"))
                    .setupTableSink(
                            upsertSink(
                                    "versions_last",
                                    LABEL_UPSERT_SCHEMA,
                                    "+I[c1, r1]",
                                    "+U[c1, r2]",
                                    "+U[c1, r4]",
                                    "+U[c1, r6]",
                                    "+U[c1, r8]",
                                    "+U[c1, r11]"))
                    .setupTableSink(
                            upsertSink(
                                    "versions_first",
                                    LABEL_UPSERT_SCHEMA,
                                    "+I[c1, r1]",
                                    "+U[c1, r9]"))
                    .runSql(
                            "INSERT INTO customers_last SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE customer_changes PARTITION BY customer_id, "
                                    + "keep => 'last', "
                                    + "sequence_by => DESCRIPTOR(lsn), "
                                    + "state_ttl => INTERVAL '1' DAY, "
                                    + "uid => 'customer-changes-dedup')")
                    .runSql(
                            "INSERT INTO prices_last SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE prices PARTITION BY ticker, "
                                    + "keep => 'last', "
                                    + "sequence_by => DESCRIPTOR(ts))")
                    .runSql(
                            "INSERT INTO prices_first SELECT * FROM DEDUPLICATE("
                                    + "input => TABLE prices PARTITION BY ticker, "
                                    + "keep => 'first', "
                                    + "sequence_by => DESCRIPTOR(ts))")
                    .runSql(
                            "INSERT INTO measurements_last SELECT id, label FROM DEDUPLICATE("
                                    + "input => TABLE measurements PARTITION BY id, "
                                    + "keep => 'last', "
                                    + "sequence_by => DESCRIPTOR(v))")
                    .runSql(
                            "INSERT INTO versions_last SELECT id, label FROM DEDUPLICATE("
                                    + "input => TABLE versions PARTITION BY id, "
                                    + "keep => 'last', "
                                    + "sequence_by => DESCRIPTOR(s, b, t, y))")
                    .runSql(
                            "INSERT INTO versions_first SELECT id, label FROM DEDUPLICATE("
                                    + "input => TABLE versions PARTITION BY id, "
                                    + "keep => 'first', "
                                    + "sequence_by => DESCRIPTOR(s, b, t, y))")
                    .build();

    private static SinkTestStep upsertSink(
            final String name, final String[] schema, final String... expected) {
        return SinkTestStep.newBuilder(name)
                .addSchema(schema)
                .addOption("sink-changelog-mode-enforced", "I,UA,D")
                .consumedValues(expected)
                .build();
    }

    private static SinkTestStep appendOnlySink(
            final String name, final String[] schema, final String... expected) {
        return SinkTestStep.newBuilder(name)
                .addSchema(schema)
                .addOption("sink-changelog-mode-enforced", "I")
                .consumedValues(expected)
                .build();
    }

    private static Row versionRow(
            @Nullable final String s,
            final boolean b,
            final int minute,
            @Nullable final Integer y,
            final String label) {
        return Row.of(
                "c1",
                s,
                b,
                LocalDateTime.ofEpochSecond(minute * 60L, 0, ZoneOffset.UTC),
                y == null ? null : new byte[] {y.byteValue()},
                label);
    }

    private static Instant at(final String time) {
        return Instant.parse("1970-01-01T" + time + ":00Z");
    }
}
