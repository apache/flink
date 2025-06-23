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

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.operators.process.AbstractProcessTableOperator.RunnerContext;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.annotation.ArgumentTrait.OPTIONAL_PARTITION_BY;
import static org.apache.flink.table.annotation.ArgumentTrait.PASS_COLUMNS_THROUGH;
import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_FULL_DELETE;
import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_UPDATE_BEFORE;
import static org.apache.flink.table.annotation.ArgumentTrait.SUPPORT_UPDATES;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_ROW;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing functions for {@link ProcessTableFunction}. */
@SuppressWarnings("unused")
public class ProcessTableFunctionTestUtils {

    public static final String BASIC_VALUES =
            "CREATE VIEW t AS SELECT * FROM "
                    + "(VALUES ('Bob', 12), ('Alice', 42)) AS T(name, score)";

    public static final String MULTI_VALUES =
            "CREATE VIEW t AS SELECT * FROM "
                    + "(VALUES ('Bob', 12), ('Alice', 42), ('Bob', 99), ('Bob', 100), ('Alice', 400)) AS T(name, score)";

    public static final String CITY_VALUES =
            "CREATE VIEW city AS SELECT * FROM "
                    + "(VALUES ('Bob', 'London'), ('Alice', 'Berlin'), ('Charly', 'Paris')) AS T(name, city)";

    public static final SourceTestStep TIMED_CITY_SOURCE =
            SourceTestStep.newBuilder("city")
                    .addSchema(
                            "name STRING",
                            "city STRING",
                            "ts TIMESTAMP_LTZ(3)",
                            "WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND")
                    .producedValues(Row.of("Bob", "London", Instant.ofEpochMilli(0)))
                    .build();

    public static final String UPDATING_VALUES =
            "CREATE VIEW t AS SELECT name, COUNT(*) FROM "
                    + "(VALUES ('Bob', 12), ('Alice', 42), ('Bob', 14)) AS T(name, score) "
                    + "GROUP BY name";

    public static final SourceTestStep TIMED_SOURCE =
            SourceTestStep.newBuilder("t")
                    .addSchema(
                            "name STRING",
                            "score INT",
                            "ts TIMESTAMP_LTZ(3)",
                            "WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND")
                    .producedValues(
                            Row.of("Bob", 1, Instant.ofEpochMilli(0)),
                            Row.of("Alice", 1, Instant.ofEpochMilli(1)),
                            Row.of("Bob", 2, Instant.ofEpochMilli(2)),
                            Row.of("Bob", 3, Instant.ofEpochMilli(3)),
                            Row.of("Bob", 4, Instant.ofEpochMilli(4)),
                            Row.of("Bob", 5, Instant.ofEpochMilli(5)),
                            Row.of("Bob", 6, Instant.ofEpochMilli(6)))
                    .build();

    public static final SourceTestStep TIMED_SOURCE_LATE_EVENTS =
            SourceTestStep.newBuilder("t")
                    .addSchema(
                            "name STRING",
                            "score INT",
                            "ts TIMESTAMP_LTZ(3)",
                            "WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND")
                    .producedValues(
                            Row.of("Bob", 1, Instant.ofEpochMilli(0)),
                            Row.of("Alice", 1, Instant.ofEpochMilli(1)),
                            Row.of("Bob", 2, Instant.ofEpochMilli(99999)),
                            Row.of("Bob", 3, Instant.ofEpochMilli(3)),
                            Row.of("Bob", 4, Instant.ofEpochMilli(4)))
                    .build();

    /** Corresponds to {@link AppendProcessTableFunctionBase}. */
    public static final List<String> BASE_SINK_SCHEMA = List.of("`out` STRING");

    /** Corresponds to {@link AppendProcessTableFunctionBase}. */
    public static final List<String> TIMED_BASE_SINK_SCHEMA =
            List.of("`out` STRING", "`rowtime` TIMESTAMP_LTZ(3)");

    /** Corresponds to {@link AppendProcessTableFunctionBase}. */
    public static final List<String> KEYED_TIMED_BASE_SINK_SCHEMA =
            List.of("`name` STRING", "`out` STRING", "`rowtime` TIMESTAMP_LTZ(3)");

    /** Corresponds to {@link AppendProcessTableFunctionBase}. */
    public static final List<String> KEYED_BASE_SINK_SCHEMA =
            List.of("`name` STRING", "`out` STRING");

    /** Corresponds to {@link AppendProcessTableFunctionBase}. */
    public static final List<String> MULTI_BASE_SINK_SCHEMA =
            List.of("`name` STRING", "`name0` STRING", "`out` STRING");

    /** Corresponds to {@link AppendProcessTableFunctionBase}. */
    public static final List<String> TIMED_MULTI_BASE_SINK_SCHEMA =
            List.of(
                    "`name` STRING",
                    "`name0` STRING",
                    "`out` STRING",
                    "`rowtime` TIMESTAMP_LTZ(3)");

    /** Corresponds to {@link AppendProcessTableFunctionBase}. */
    public static final List<String> PASS_THROUGH_BASE_SINK_SCHEMA =
            List.of("`name` STRING", "`score` INT", "`out` STRING");

    /** Testing function. */
    public static class AtomicTypeWrappingFunction extends ProcessTableFunction<Integer> {
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r) {
            collect(r.getFieldAs(1));
        }
    }

    @DataTypeHint("ROW<`out` STRING>")
    public abstract static class AppendProcessTableFunctionBase extends ProcessTableFunction<Row> {
        protected void collectObjects(Object... objects) {
            // Row.toString is useful because it can handle all common objects,
            // but we use '{}' to indicate that this is a custom string output.
            final String asString = Row.of(objects).toString();
            final String objectsAsString = asString.substring(3, asString.length() - 1);
            collect(Row.of("{" + objectsAsString + "}"));
        }

        protected void collectEvalEvent(TimeContext<Long> timeCtx, Row r) {
            collectObjects(
                    String.format(
                            "Processing input row %s at time %s watermark %s",
                            r, timeCtx.time(), timeCtx.currentWatermark()));
        }

        protected void collectOnTimerEvent(OnTimerContext ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectObjects(
                    String.format(
                            "Timer %s fired at time %s watermark %s",
                            ctx.currentTimer(), timeCtx.time(), timeCtx.currentWatermark()));
        }

        protected void collectCreateTimer(TimeContext<Long> timeCtx, String name, long time) {
            collectObjects(
                    String.format(
                            "Registering timer %s for %s at time %s watermark %s",
                            name, time, timeCtx.time(), timeCtx.currentWatermark()));
            timeCtx.registerOnTime(name, time);
        }

        protected void collectCreateTimer(TimeContext<Long> timeCtx, long time) {
            collectObjects(
                    String.format(
                            "Registering timer for %s at time %s watermark %s",
                            time, timeCtx.time(), timeCtx.currentWatermark()));
            timeCtx.registerOnTime(time);
        }

        protected void collectClearTimer(TimeContext<Long> timeCtx, String name) {
            collectObjects(
                    String.format(
                            "Clearing timer %s at time %s watermark %s",
                            name, timeCtx.time(), timeCtx.currentWatermark()));
            timeCtx.clearTimer(name);
        }

        protected void collectClearTimer(TimeContext<Long> timeCtx, long time) {
            collectObjects(
                    String.format(
                            "Clearing timer %s at time %s watermark %s",
                            time, timeCtx.time(), timeCtx.currentWatermark()));
            timeCtx.clearTimer(time);
        }

        protected void collectClearAllTimers(TimeContext<Long> timeCtx) {
            collectObjects(
                    String.format(
                            "Clearing all timers at time %s watermark %s",
                            timeCtx.time(), timeCtx.currentWatermark()));
            timeCtx.clearAllTimers();
        }
    }

    /** Testing function. */
    public static class ScalarArgsFunction extends AppendProcessTableFunctionBase {
        public void eval(Integer i, Boolean b) {
            collectObjects(i, b);
        }
    }

    /** Testing function. */
    public static class TableAsRowFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TypedTableAsRowFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) User u, Integer i) {
            collectObjects(u, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TypedTableAsSetFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_SET) User u, Integer i) {
            collectObjects(u, i);
        }
    }

    /** Testing function. */
    public static class PojoArgsFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_ROW) User input, User scalar) {
            collectObjects(input, scalar);
        }
    }

    /** Testing function. */
    public static class EmptyArgFunction extends AppendProcessTableFunctionBase {
        public void eval() {
            collectObjects("empty");
        }
    }

    /** Testing function. */
    public static class TableAsRowPassThroughFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_ROW, PASS_COLUMNS_THROUGH}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetPassThroughFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetUpdatingArgFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY, SUPPORT_UPDATES}) Row r) {
            collectObjects(
                    r,
                    toModeSummary(
                            ctx.tableSemanticsFor("r")
                                    .changelogMode()
                                    .orElseThrow(IllegalStateException::new)));
        }
    }

    /** Testing function. */
    public static class TableAsSetRetractArgFunction extends AppendProcessTableFunctionBase {
        public void eval(
                @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES, REQUIRE_UPDATE_BEFORE}) Row r) {
            collectObjects(r);
        }
    }

    /** Testing function. */
    public static class TableAsSetFullDeletesArgFunction extends AppendProcessTableFunctionBase {
        public void eval(
                @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES, REQUIRE_FULL_DELETE}) Row r) {
            collectObjects(r);
        }
    }

    /** Testing function. */
    public static class TableAsSetOptionalPartitionFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class ContextFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint(TABLE_AS_SET) Row r, String s) {
            final TableSemantics semantics = ctx.tableSemanticsFor("r");
            collectObjects(
                    r,
                    s,
                    semantics.partitionByColumns(),
                    semantics.changelogMode().orElse(null),
                    semantics.dataType());
        }
    }

    /** Testing function. */
    public static class PojoStateFunction extends AppendProcessTableFunctionBase {
        public void eval(@StateHint Score s, @ArgumentHint(TABLE_AS_SET) Row r) {
            collectObjects(s, r);
            if (r.getFieldAs("name").equals("Bob")) {
                s.s = r.getFieldAs("name");
            }
            if (s.i == null) {
                s.i = 0;
            } else {
                s.i++;
            }
        }
    }

    /** Testing function. */
    public static class PojoWithDefaultStateFunction extends AppendProcessTableFunctionBase {
        public void eval(@StateHint ScoreWithDefaults s, @ArgumentHint(TABLE_AS_SET) Row r) {
            collectObjects(s, r);
            if (r.getFieldAs("name").equals("Bob")) {
                s.s = r.getFieldAs("name");
            }
            if (s.i == 99) {
                s.i = 0;
            } else {
                s.i++;
            }
        }
    }

    /** Testing function. */
    public static class MultiStateFunction extends AppendProcessTableFunctionBase {
        public void eval(
                @StateHint(type = @DataTypeHint("ROW<i INT>")) Row s1,
                @StateHint(type = @DataTypeHint("ROW<s STRING>")) Row s2,
                @ArgumentHint(TABLE_AS_SET) Row r) {
            collectObjects(s1, s2, r);
            Integer i = s1.<Integer>getFieldAs("i");
            if (i == null) {
                i = 0;
            }
            s2.setField("s", i.toString());
            s1.setField("i", i + 1);
        }
    }

    /** Testing function. */
    public static class ClearStateFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx, @StateHint ScoreWithDefaults s, @ArgumentHint(TABLE_AS_SET) Row r) {
            collectObjects(s, r);
            if (r.getFieldAs("name").equals("Bob") && s.i == 100) {
                ctx.clearState("s");
            } else {
                s.i++;
            }
        }
    }

    /** Testing function. */
    public static class TimeToLiveStateFunction extends ProcessTableFunction<String> {
        public void eval(
                Context ctx,
                @StateHint(type = @DataTypeHint("ROW<emitted BOOLEAN>")) Row s0,
                @StateHint(ttl = "5 days") Score s1,
                @StateHint(ttl = "0") Score s2,
                @StateHint Score s3,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r) {
            final RunnerContext internalContext = (RunnerContext) ctx;
            if (s0.getFieldAs("emitted") == null) {
                collect(
                        String.format(
                                "s1=%s, s2=%s, s3=%s",
                                internalContext.getStateDescriptor("s1").getTtlConfig(),
                                internalContext.getStateDescriptor("s2").getTtlConfig(),
                                internalContext.getStateDescriptor("s3").getTtlConfig()));
                s0.setField("emitted", true);
            }
        }
    }

    /** Testing function. */
    public static class DescriptorFunction extends AppendProcessTableFunctionBase {
        public void eval(
                ColumnList columnList1,
                @ArgumentHint(isOptional = true) ColumnList columnList2,
                @DataTypeHint("DESCRIPTOR NOT NULL") ColumnList columnList3) {
            collectObjects(columnList1, columnList2, columnList3);
        }
    }

    /** Testing function. */
    public static class RequiredTimeFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint({ArgumentTrait.TABLE_AS_ROW, REQUIRE_ON_TIME}) Row r) {
            collectObjects(r);
        }
    }

    /** Testing function. */
    public static class TimeConversionsFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint({TABLE_AS_ROW, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> asLong = ctx.timeContext(Long.class);
            final TimeContext<Instant> asInstant = ctx.timeContext(Instant.class);
            final TimeContext<LocalDateTime> asLocalDateTime = ctx.timeContext(LocalDateTime.class);
            // read
            collectObjects(
                    String.format(
                            "Time (Long: %s, Instant: %s, LocalDateTime: %s), "
                                    + "Watermark (Long: %s, Instant: %s, LocalDateTime: %s)",
                            asLong.time(),
                            asInstant.time(),
                            asLocalDateTime.time(),
                            asLong.currentWatermark(),
                            asInstant.currentWatermark(),
                            asLocalDateTime.currentWatermark()));
        }
    }

    /** Testing function. */
    @SuppressWarnings("SameParameterValue")
    public static class NamedTimersFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);

            if (timeCtx.time() == 0) {
                // create
                collectCreateTimer(timeCtx, "timeout1", timeCtx.time() + 1);
                collectCreateTimer(timeCtx, "timeout2", timeCtx.time() + 2);
                collectCreateTimer(timeCtx, "timeout3", timeCtx.time() + 3);
                collectCreateTimer(timeCtx, "timeout4", Long.MAX_VALUE);
                collectCreateTimer(timeCtx, "timeout5", Long.MAX_VALUE);
            }
        }

        public void onTimer(OnTimerContext ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            // read
            collectOnTimerEvent(ctx);
            if (ctx.currentTimer().equals("timeout1")) {
                // replace
                collectCreateTimer(timeCtx, "timeout2", timeCtx.time() + 4);
                // clear
                collectClearTimer(timeCtx, "timeout3");
            } else if (ctx.currentTimer().equals("timeout2")) {
                // clear all
                collectClearAllTimers(timeCtx);
            }
        }
    }

    /** Testing function. */
    @SuppressWarnings("SameParameterValue")
    public static class UnnamedTimersFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);

            if (timeCtx.time() == 0) {
                // create
                collectCreateTimer(timeCtx, timeCtx.time() + 1);
                collectCreateTimer(timeCtx, timeCtx.time() + 2);
                collectCreateTimer(timeCtx, Long.MAX_VALUE);
            }
        }

        public void onTimer(OnTimerContext ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            // read
            collectOnTimerEvent(ctx);
            if (timeCtx.time() == 1) {
                // re-create
                collectCreateTimer(timeCtx, timeCtx.time() + 4);
                // clear
                collectClearTimer(timeCtx, 2);
            } else if (timeCtx.time() == 5) {
                // clear all
                collectClearAllTimers(timeCtx);
            }
        }
    }

    /** Testing function. */
    public static class LateTimersFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);
            // all timers should be executed once
            if (timeCtx.time() == 99998) {
                // will never be fired because it's late
                collectCreateTimer(timeCtx, "late", 1L);
            }
            collectCreateTimer(timeCtx, "t", 0L);
            collectCreateTimer(timeCtx, 0L);
        }

        public void onTimer(OnTimerContext ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectOnTimerEvent(ctx);
            // will never be fired because it's late
            collectCreateTimer(timeCtx, "again", timeCtx.time());
        }
    }

    /** Testing function. */
    public static class ScalarArgsTimeFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectObjects(
                    String.format(
                            "Time %s and watermark %s",
                            timeCtx.time(), timeCtx.currentWatermark()));
        }
    }

    /** Testing function. */
    public static class OptionalPartitionOnTimeFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);
            if (timeCtx.time() == 0) {
                collectCreateTimer(timeCtx, "t", timeCtx.time() + 1);
            }
        }

        public void onTimer(OnTimerContext ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectOnTimerEvent(ctx);
            if (ctx.currentTimer().equals("t")) {
                collectCreateTimer(timeCtx, "again", timeCtx.time() + 1);
            }
        }
    }

    /** Testing function. */
    public static class OptionalOnTimeFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint(TABLE_AS_SET) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);
            collectCreateTimer(timeCtx, "t", 2);
        }

        public void onTimer(OnTimerContext ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectOnTimerEvent(ctx);
        }
    }

    /** Testing function. */
    public static class PojoStateTimeFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @StateHint Score s,
                @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectObjects(s, r);
            if (s.i == null) {
                s.i = 1;
                collectCreateTimer(timeCtx, "t", timeCtx.time() + 2);
            } else {
                s.i += 1;
            }
        }

        public void onTimer(OnTimerContext ctx, Score s) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectOnTimerEvent(ctx);
            s.i *= 10;
        }
    }

    /** Testing function. */
    public static class ChainedSendingFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);
            collectCreateTimer(timeCtx, "t", timeCtx.time() + 1);
        }

        public void onTimer(OnTimerContext ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectOnTimerEvent(ctx);
            collectObjects(timeCtx.time());
        }
    }

    /** Testing function. */
    public static class ChainedReceivingFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);
        }
    }

    /** Testing function. */
    public static class InvalidTableAsRowTimersFunction extends AppendProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint(TABLE_AS_ROW) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            timeCtx.registerOnTime(42L);
        }
    }

    /** Testing function. */
    public static class InvalidPassThroughTimersFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            timeCtx.registerOnTime(42L);
        }
    }

    /** Testing function. */
    public static class OptionalFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx, @ArgumentHint(value = TABLE_AS_ROW, isOptional = true) Row r) {
            collectObjects(r);
        }
    }

    /** Testing function. */
    public static class ListStateFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @StateHint ListView<String> s,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r)
                throws Exception {
            collectObjects(s.getList(), s.getClass().getSimpleName(), r);

            // get
            int count = s.getList().size();

            // create
            s.add(String.valueOf(count));

            // null behavior
            assertThatThrownBy(() -> s.add(null))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("List views don't support null values.");
            assertThatThrownBy(() -> s.addAll(Arrays.asList("item0", null)))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("List views don't support null values.");

            // clear
            if (count == 2) {
                ctx.clearState("s");
            }
        }
    }

    /** Testing function. */
    public static class MapStateFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @StateHint MapView<String, Integer> s,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r)
                throws Exception {
            final String viewToString =
                    s.getMap().entrySet().stream()
                            .map(Objects::toString)
                            .sorted()
                            .collect(Collectors.joining(", ", "{", "}"));
            collectObjects(viewToString, s.getClass().getSimpleName(), r);

            // get
            final String name = r.getFieldAs("name");
            int count = 1;
            if (s.contains(name)) {
                count = s.get(name);
            }

            // create
            s.put("old" + name, count);
            s.put(name, count + 1);

            // null behavior
            assertThatThrownBy(() -> s.put(null, 42))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Map views don't support null keys.");
            final Map<String, Integer> mapWithNull = new HashMap<>();
            mapWithNull.put("key", 42);
            mapWithNull.put(null, 42);
            assertThatThrownBy(() -> s.putAll(mapWithNull))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Map views don't support null keys.");
            s.put("nullValue", null);

            // clear
            if (count == 2) {
                ctx.clearState("s");
            }
        }
    }

    @DataTypeHint("ROW<name STRING, count BIGINT, mode STRING>")
    public abstract static class ChangelogProcessTableFunctionBase extends ProcessTableFunction<Row>
            implements ChangelogFunction {

        void collectUpdate(Context ctx, Row r) {
            collect(
                    Row.ofKind(
                            r.getKind(),
                            r.getField(0),
                            r.getField(1),
                            toModeSummary(ctx.getChangelogMode())));
        }
    }

    /** Testing function. */
    public static class UpdatingRetractFunction extends ChangelogProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES, REQUIRE_UPDATE_BEFORE}) Row r) {
            collectUpdate(ctx, r);
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.all();
        }
    }

    /** Testing function. */
    public static class UpdatingUpsertFunction extends ChangelogProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES}) Row r) {
            collectUpdate(ctx, r);
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.upsert(
                    changelogContext.getRequiredChangelogMode().keyOnlyDeletes());
        }
    }

    /** Testing function. */
    public static class UpdatingUpsertFullDeletesFunction
            extends ChangelogProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES, REQUIRE_FULL_DELETE}) Row r) {
            collectUpdate(ctx, r);
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.upsert(false);
        }
    }

    /** Testing function. */
    public static class InvalidUpdatingSemanticsFunction extends ChangelogProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint({TABLE_AS_ROW, SUPPORT_UPDATES}) Row r) {
            collectUpdate(ctx, r);
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.all();
        }
    }

    /** Testing function. */
    public static class InvalidRowKindFunction extends AppendProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_ROW) Row r) {
            collect(Row.ofKind(RowKind.DELETE, "invalidate"));
        }
    }

    /** Testing function. */
    public static class MultiInputFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint(TABLE_AS_SET) Row in1,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row in2)
                throws Exception {
            collectObjects(in1, in2);
        }
    }

    /** Testing function. */
    public static class TimedJoinFunction extends AppendProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @StateHint Tuple1<Integer> score,
                @StateHint Tuple1<String> city,
                @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row scoreTable,
                @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row cityTable)
                throws Exception {
            final TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
            if (scoreTable != null) {
                score.f0 = scoreTable.getFieldAs("score");
                timeCtx.registerOnTime("timeout", timeCtx.time().plusMillis(1000));
            }
            if (cityTable != null) {
                city.f0 = cityTable.getFieldAs("city");
            }
            if (score.f0 != null && city.f0 != null) {
                collect(Row.of(score.f0 + " score in city " + city.f0));
                ctx.clearAllTimers();
            }
        }

        public void onTimer(OnTimerContext ctx, Tuple1<Integer> score, Tuple1<String> city) {
            collect(Row.of("no city found for score " + score.f0));
            score.f0 = null;
        }
    }

    /**
     * Implements a custom join that acts like kind of an outer join and never produces deletions.
     * Both the score and city can change at any time. The join will output an update if a matching
     * pair could be found.
     */
    @DataTypeHint("ROW<out STRING>")
    public static class UpdatingJoinFunction extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                @StateHint Tuple1<Integer> score,
                @StateHint Tuple1<String> city,
                @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES}) Row scoreTable,
                @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES}) Row cityTable)
                throws Exception {
            final boolean wasMatch = isMatch(score, city);
            if (isDelete(scoreTable) || isDelete(cityTable)) {
                if (wasMatch) {
                    collect(Row.ofKind(RowKind.DELETE, (Object) null));
                }
            }

            if (scoreTable != null) {
                apply(score, scoreTable.getFieldAs("score"), scoreTable.getKind());
            }
            if (cityTable != null) {
                apply(city, cityTable.getFieldAs("city"), cityTable.getKind());
            }
            if (isMatch(score, city)) {
                collect(
                        Row.ofKind(
                                wasMatch ? RowKind.UPDATE_AFTER : RowKind.INSERT,
                                "score " + score.f0 + " in city " + city.f0));
            }
        }

        public boolean isDelete(Row r) {
            return r != null && r.getKind() == RowKind.DELETE;
        }

        public boolean isMatch(Tuple1<Integer> score, Tuple1<String> city) {
            return score.f0 != null && city.f0 != null;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.upsert();
        }

        private static <T> void apply(Tuple1<T> t, T o, RowKind op) {
            if (op == RowKind.INSERT || op == RowKind.UPDATE_AFTER) {
                t.f0 = o;
            } else {
                t.f0 = null;
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /** POJO for typed tables. */
    public static class User {
        public String s;
        public Integer i;

        public User(String s, Integer i) {
            this.s = s;
            this.i = i;
        }

        @Override
        public String toString() {
            return String.format("User(s='%s', i=%s)", s, i);
        }
    }

    public static class PojoCreatingFunction extends ScalarFunction {
        public User eval(String s, Integer i) {
            return new User(s, i);
        }
    }

    /** POJO for state. All fields nullable. */
    public static class Score {
        public String s;
        public Integer i;

        @Override
        public String toString() {
            return String.format("Score(s='%s', i=%s)", s, i);
        }
    }

    /** POJO for state. One field is not nullable and pre-initialized. */
    public static class ScoreWithDefaults {
        public String s;
        public int i = 99;

        @Override
        public String toString() {
            return String.format("ScoreWithDefaults(s='%s', i=%s)", s, i);
        }
    }

    private static final Map<String, String> MODE_SUMMARY =
            Map.ofEntries(
                    Map.entry("[INSERT, UPDATE_BEFORE, UPDATE_AFTER]", "retract-no-delete"),
                    Map.entry("[INSERT, UPDATE_AFTER]", "upsert-no-delete"),
                    Map.entry("[INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE]", "retract"),
                    Map.entry("[INSERT, UPDATE_AFTER, DELETE]", "upsert-full-delete"),
                    Map.entry("[INSERT, UPDATE_AFTER, ~DELETE]", "upsert-partial-delete"));

    private static String toModeSummary(ChangelogMode mode) {
        return MODE_SUMMARY.get(mode.toString());
    }
}
