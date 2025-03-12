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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.operators.process.ProcessTableOperator;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.LocalDateTime;

import static org.apache.flink.table.annotation.ArgumentTrait.OPTIONAL_PARTITION_BY;
import static org.apache.flink.table.annotation.ArgumentTrait.PASS_COLUMNS_THROUGH;
import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.SUPPORT_UPDATES;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_ROW;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;

/** Testing functions for {@link ProcessTableFunction}. */
@SuppressWarnings("unused")
public class ProcessTableFunctionTestUtils {

    /** Testing function. */
    public static class AtomicTypeWrappingFunction extends ProcessTableFunction<Integer> {
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r) {
            collect(r.getFieldAs(1));
        }
    }

    @DataTypeHint("ROW<`out` STRING>")
    public abstract static class TestProcessTableFunctionBase extends ProcessTableFunction<Row> {
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
    public static class ScalarArgsFunction extends TestProcessTableFunctionBase {
        public void eval(Integer i, Boolean b) {
            collectObjects(i, b);
        }
    }

    /** Testing function. */
    public static class TableAsRowFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TypedTableAsRowFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) User u, Integer i) {
            collectObjects(u, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TypedTableAsSetFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_SET) User u, Integer i) {
            collectObjects(u, i);
        }
    }

    /** Testing function. */
    public static class PojoArgsFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint(TABLE_AS_ROW) User input, User scalar) {
            collectObjects(input, scalar);
        }
    }

    /** Testing function. */
    public static class EmptyArgFunction extends TestProcessTableFunctionBase {
        public void eval() {
            collectObjects("empty");
        }
    }

    /** Testing function. */
    public static class TableAsRowPassThroughFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_ROW, PASS_COLUMNS_THROUGH}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetPassThroughFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class TableAsSetUpdatingArgFunction extends TestProcessTableFunctionBase {
        public void eval(
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY, SUPPORT_UPDATES}) Row r) {
            collectObjects(r);
        }
    }

    /** Testing function. */
    public static class TableAsSetOptionalPartitionFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r, Integer i) {
            collectObjects(r, i);
        }
    }

    /** Testing function. */
    public static class ContextFunction extends TestProcessTableFunctionBase {
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
    public static class PojoStateFunction extends TestProcessTableFunctionBase {
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
    public static class PojoWithDefaultStateFunction extends TestProcessTableFunctionBase {
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
    public static class MultiStateFunction extends TestProcessTableFunctionBase {
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
    public static class ClearStateFunction extends TestProcessTableFunctionBase {
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
            final ProcessTableOperator.RunnerContext internalContext =
                    (ProcessTableOperator.RunnerContext) ctx;
            if (s0.getFieldAs("emitted") == null) {
                collect(
                        String.format(
                                "s1=%s, s2=%s, s3=%s",
                                internalContext.getValueStateDescriptor("s1").getTtlConfig(),
                                internalContext.getValueStateDescriptor("s2").getTtlConfig(),
                                internalContext.getValueStateDescriptor("s3").getTtlConfig()));
                s0.setField("emitted", true);
            }
        }
    }

    /** Testing function. */
    public static class DescriptorFunction extends TestProcessTableFunctionBase {
        public void eval(
                ColumnList columnList1,
                @ArgumentHint(isOptional = true) ColumnList columnList2,
                @DataTypeHint("DESCRIPTOR NOT NULL") ColumnList columnList3) {
            collectObjects(columnList1, columnList2, columnList3);
        }
    }

    /** Testing function. */
    public static class RequiredTimeFunction extends TestProcessTableFunctionBase {
        public void eval(@ArgumentHint({ArgumentTrait.TABLE_AS_ROW, REQUIRE_ON_TIME}) Row r) {
            collectObjects(r);
        }
    }

    /** Testing function. */
    public static class TimeConversionsFunction extends TestProcessTableFunctionBase {
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
    public static class NamedTimersFunction extends TestProcessTableFunctionBase {
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
    public static class UnnamedTimersFunction extends TestProcessTableFunctionBase {
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
    public static class LateTimersFunction extends TestProcessTableFunctionBase {
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
    public static class ScalarArgsTimeFunction extends TestProcessTableFunctionBase {
        public void eval(Context ctx) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectObjects(
                    String.format(
                            "Time %s and watermark %s",
                            timeCtx.time(), timeCtx.currentWatermark()));
        }
    }

    /** Testing function. */
    public static class OptionalPartitionOnTimeFunction extends TestProcessTableFunctionBase {
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
    public static class OptionalOnTimeFunction extends TestProcessTableFunctionBase {
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
    public static class PojoStateTimeFunction extends TestProcessTableFunctionBase {
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
    public static class ChainedSendingFunction extends TestProcessTableFunctionBase {
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
    public static class ChainedReceivingFunction extends TestProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            collectEvalEvent(timeCtx, r);
        }
    }

    /** Testing function. */
    public static class InvalidTableAsRowTimersFunction extends TestProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint(TABLE_AS_ROW) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            timeCtx.registerOnTime(42L);
        }
    }

    /** Testing function. */
    public static class InvalidPassThroughTimersFunction extends TestProcessTableFunctionBase {
        public void eval(
                Context ctx,
                @ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH, REQUIRE_ON_TIME}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            timeCtx.registerOnTime(42L);
        }
    }

    /** Testing function. */
    public static class InvalidUpdatingTimersFunction extends TestProcessTableFunctionBase {
        public void eval(Context ctx, @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES}) Row r) {
            final TimeContext<Long> timeCtx = ctx.timeContext(Long.class);
            timeCtx.registerOnTime(42L);
        }
    }

    /** Testing function. */
    public static class OptionalFunction extends TestProcessTableFunctionBase {
        public void eval(
                Context ctx, @ArgumentHint(value = TABLE_AS_ROW, isOptional = true) Row r) {
            collectObjects(r);
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
}
