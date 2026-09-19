/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Optional;

/**
 * Shared PTF test fixtures referenced by {@link ProcessTableFunctionTestHarnessTest}, {@link
 * ProcessTableFunctionTestHarnessChangelogModeTest} and {@link PtfChangelogModeValidatorTest}.
 *
 * <p>Mirrors the role of {@code ProcessTableFunctionTestUtils} in the upstream planner tests.
 *
 * <h2>Recurring trait combinations</h2>
 *
 * <p>Two declaration constraints explain why several fixtures below look more elaborate than the
 * behavior they exercise:
 *
 * <ul>
 *   <li>{@code REQUIRE_UPDATE_BEFORE} and {@code REQUIRE_FULL_DELETE} are documented as valid only
 *       on {@code SET_SEMANTIC_TABLE} arguments, so fixtures carrying them must use set semantics
 *       to describe a reachable PTF configuration.
 *   <li>Multiple table arguments require <em>all</em> of them to use set semantics ({@code
 *       SystemTypeInference#checkMultipleTableArgs}).
 * </ul>
 *
 * <p>{@code OPTIONAL_PARTITION_BY} accompanies {@code SET_SEMANTIC_TABLE} wherever a test does not
 * care about partitioning, so that no {@code .partitionBy(...)} call is needed.
 */
class PtfTestFunctions {

    static final String VALUE_ROW = "ROW<value INT>";
    static final String KEY_VALUE_ROW = "ROW<key INT, value INT>";
    static final String TIMED_ROW = "ROW<partition STRING, ts TIMESTAMP(3)>";

    /**
     * Registers the sole table argument under the name {@code input}, as every single-argument
     * fixture here declares it.
     */
    static ProcessTableFunctionTestHarness.Builder<Row> onInput(
            Class<? extends ProcessTableFunction<Row>> functionClass, String inputType) {
        return ProcessTableFunctionTestHarness.<Row>ofClass(functionClass)
                .withTableArgument(inputArg(inputType).build());
    }

    /**
     * Returns a builder for the sole table argument named {@code input}, for callers that need to
     * attach further per-argument configuration (e.g. {@code changelogMode(...)}) before building.
     */
    static ProcessTableFunctionTestHarness.TableArgument.Builder inputArg(String inputType) {
        return ProcessTableFunctionTestHarness.TableArgument.forName("input")
                .type(DataTypes.of(inputType));
    }

    /**
     * Mirrors a passthrough PTF's declared output mode onto whatever changelog mode its (single)
     * table argument carries — the honest declaration for a function that forwards its input
     * unchanged, and the shape the planner would infer for it.
     */
    private static ChangelogMode echoInputMode(ChangelogFunction.ChangelogContext ctx) {
        ChangelogMode inputMode = ctx.getTableChangelogMode(0);
        return inputMode == null ? ChangelogMode.insertOnly() : inputMode;
    }

    // -------------------------------------------------------------------------
    // Passthrough inputs
    // -------------------------------------------------------------------------

    @DataTypeHint("ROW<value INT>")
    public static class PassthroughPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }
    }

    /** Passthrough PTF with {@code SUPPORT_UPDATES} and {@code REQUIRE_UPDATE_BEFORE}. */
    @DataTypeHint("ROW<value INT>")
    public static class UpdatingPassthroughPTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY,
                            ArgumentTrait.SUPPORT_UPDATES,
                            ArgumentTrait.REQUIRE_UPDATE_BEFORE
                        })
                        Row input) {
            collect(input);
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return echoInputMode(changelogContext);
        }
    }

    /**
     * Like {@link UpdatingPassthroughPTF} but without {@code REQUIRE_UPDATE_BEFORE}/{@code
     * REQUIRE_FULL_DELETE}, so any updating {@link ChangelogMode} is trait-consistent and only the
     * configured mode itself constrains which {@link RowKind}s are accepted.
     */
    @DataTypeHint("ROW<value INT>")
    public static class PlainUpdatingPassthroughPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint({ArgumentTrait.ROW_SEMANTIC_TABLE, ArgumentTrait.SUPPORT_UPDATES})
                        Row input) {
            collect(input);
        }
    }

    /** Two-column set-semantic variant, for upsert-key and key-only-delete scenarios. */
    @DataTypeHint("ROW<key INT, value INT>")
    public static class UpdatingTwoColumnPassthroughPTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY,
                            ArgumentTrait.SUPPORT_UPDATES
                        })
                        Row input) {
            collect(input);
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return echoInputMode(changelogContext);
        }
    }

    @DataTypeHint("ROW<value INT>")
    public static class RequireFullDeletePassthroughPTF extends ProcessTableFunction<Row> {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY,
                            ArgumentTrait.SUPPORT_UPDATES,
                            ArgumentTrait.REQUIRE_FULL_DELETE
                        })
                        Row input) {
            collect(input);
        }
    }

    // -------------------------------------------------------------------------
    // TableSemantics probes
    // -------------------------------------------------------------------------

    /**
     * Renders what {@link TableSemantics} reports to {@code eval()} for its single table argument:
     * the changelog mode, and the upsert key columns as resolved field indices.
     */
    @DataTypeHint("ROW<mode STRING, upsertKeys STRING>")
    public static class TableSemanticsProbePTF extends ProcessTableFunction<Row> {
        public void eval(
                Context ctx,
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY,
                            ArgumentTrait.SUPPORT_UPDATES
                        })
                        Row input) {
            TableSemantics semantics = ctx.tableSemanticsFor("input");
            collect(
                    Row.of(
                            semantics.changelogMode().map(Object::toString).orElse("empty"),
                            Arrays.deepToString(semantics.upsertKeyColumns().toArray())));
        }
    }

    /**
     * Reports the changelog mode each of its two table arguments sees: one that declares {@code
     * SUPPORT_UPDATES} and one that does not.
     */
    @DataTypeHint("ROW<retractingMode STRING, plainMode STRING>")
    public static class TwoTableChangelogModePTF extends ProcessTableFunction<Row> {
        public void eval(
                Context ctx,
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY,
                            ArgumentTrait.SUPPORT_UPDATES
                        })
                        Row retracting,
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row plain) {
            collect(Row.of(modeOf(ctx, "retracting"), modeOf(ctx, "plain")));
        }

        private static String modeOf(Context ctx, String argName) {
            return ctx.tableSemanticsFor(argName)
                    .changelogMode()
                    .map(Object::toString)
                    .orElse("empty");
        }
    }

    /**
     * A PTF whose custom output {@link org.apache.flink.table.types.inference.TypeStrategy} reads
     * {@link TableSemantics#changelogMode()} during type inference and captures the result in a
     * static field for assertion.
     */
    @DataTypeHint("ROW<value INT>")
    public static class TypeInferenceChangelogModeProbePTF extends ProcessTableFunction<Row> {
        static volatile Optional<ChangelogMode> capturedChangelogMode = null;

        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(input);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            TypeInference base = super.getTypeInference(typeFactory);
            return TypeInference.newBuilder()
                    .staticArguments(base.getStaticArguments().orElse(null))
                    .outputTypeStrategy(
                            ctx -> {
                                capturedChangelogMode =
                                        ctx.getTableSemantics(0)
                                                .map(TableSemantics::changelogMode)
                                                .orElse(null);
                                return base.getOutputTypeStrategy().inferType(ctx);
                            })
                    .build();
        }
    }

    // -------------------------------------------------------------------------
    // Output changelog mode
    // -------------------------------------------------------------------------

    /**
     * Emits a DELETE without implementing {@link ChangelogFunction}, so the harness must fall back
     * to its insert-only default output mode and reject the row.
     */
    @DataTypeHint("ROW<value INT>")
    public static class InvalidRowKindPTF extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            collect(Row.ofKind(RowKind.DELETE, 1));
        }
    }

    /**
     * Collects the resolver-derived output mode into its first field so a test can observe it. Its
     * {@code getChangelogMode()} ties the output mode to the input mode.
     */
    @DataTypeHint("ROW<output STRING>")
    public static class InputDrivenChangelogModePTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                Context ctx,
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.SUPPORT_UPDATES,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row input) {
            collect(Row.of(ctx.getChangelogMode().toString()));
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return echoInputMode(changelogContext);
        }
    }

    /** Declares upsert output on a row-semantic argument, which the planner rejects. */
    @DataTypeHint("ROW<value INT>")
    public static class UpdatingOutputPTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(Context ctx, @ArgumentHint(ArgumentTrait.ROW_SEMANTIC_TABLE) Row input) {
            int v = input.getFieldAs("value");
            collect(Row.ofKind(RowKind.UPDATE_AFTER, v));
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.upsert(true);
        }
    }

    /** Emits upsert-style output, which set semantics allow but row semantics do not. */
    @DataTypeHint("ROW<key INT, value INT>")
    public static class UpsertSetSemanticPTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                Context ctx,
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row input) {
            int key = input.getFieldAs("key");
            Integer value = input.getFieldAs("value");
            if (value == null) {
                collect(Row.ofKind(RowKind.DELETE, key, null));
            } else {
                collect(Row.ofKind(RowKind.UPDATE_AFTER, key, value));
            }
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.upsert(true);
        }
    }

    // -------------------------------------------------------------------------
    // Timers
    // -------------------------------------------------------------------------

    /** Used only in tests that assert a build() failure — the body is unreachable. */
    @DataTypeHint("ROW<message STRING>")
    public static class TimerWithUpdatingInputPTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                Context ctx,
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.SUPPORT_UPDATES,
                            ArgumentTrait.REQUIRE_ON_TIME
                        })
                        Row input) {
            throw new UnsupportedOperationException("Never invoked by this test");
        }

        // Insert-only output keeps the on-time rejection attributable to the updating input.
        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.insertOnly();
        }
    }

    /**
     * Insert-only (not a {@link ChangelogFunction}); its timer emits a DELETE the harness rejects.
     */
    @DataTypeHint("ROW<value INT>")
    public static class TimerEmitsInvalidRowKindPTF extends ProcessTableFunction<Row> {
        public void eval(
                Context ctx,
                @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                        Row input) {
            TimeContext<LocalDateTime> t = ctx.timeContext(LocalDateTime.class);
            t.registerOnTime("test", t.time().plus(Duration.ofSeconds(1)));
        }

        public void onTimer(OnTimerContext ctx) {
            collect(Row.ofKind(RowKind.DELETE, 1));
        }
    }

    /** Declares updating output alongside {@code REQUIRE_ON_TIME}, which the harness rejects. */
    @DataTypeHint("ROW<value INT>")
    public static class OnTimeUpdatingOutputPTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                Context ctx,
                @ArgumentHint({ArgumentTrait.SET_SEMANTIC_TABLE, ArgumentTrait.REQUIRE_ON_TIME})
                        Row input) {
            throw new UnsupportedOperationException("Never invoked by this test");
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.all();
        }
    }

    /**
     * Declares pass-through columns alongside updating output, which the harness rejects to mirror
     * production's {@code verifyPassThroughColumnsForUpdates}.
     */
    @DataTypeHint("ROW<value INT>")
    public static class PassThroughUpdatingOutputPTF extends ProcessTableFunction<Row>
            implements ChangelogFunction {
        public void eval(
                Context ctx,
                @ArgumentHint({
                            ArgumentTrait.ROW_SEMANTIC_TABLE,
                            ArgumentTrait.PASS_COLUMNS_THROUGH
                        })
                        Row input) {
            throw new UnsupportedOperationException("Never invoked by this test");
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext changelogContext) {
            return ChangelogMode.all();
        }
    }
}
