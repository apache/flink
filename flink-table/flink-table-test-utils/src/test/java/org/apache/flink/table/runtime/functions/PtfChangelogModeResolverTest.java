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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.functions.ProcessTableFunctionTestHarness.TableArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PtfChangelogModeResolver}. */
class PtfChangelogModeResolverTest {

    /** A {@link ChangelogFunction} whose answer is driven by a supplied behavior function. */
    private static class StubChangelogFunction implements ChangelogFunction {
        private final Function<ChangelogContext, ChangelogMode> behavior;

        StubChangelogFunction(Function<ChangelogContext, ChangelogMode> behavior) {
            this.behavior = behavior;
        }

        /** Returns the same mode for every context. */
        static StubChangelogFunction fixed(ChangelogMode mode) {
            return new StubChangelogFunction(ctx -> mode);
        }

        /** Answers based on the call index (1-based), for probe-dependent behavior. */
        static StubChangelogFunction perCall(Function<Integer, ChangelogMode> byCallIndex) {
            int[] calls = {0};
            return new StubChangelogFunction(ctx -> byCallIndex.apply(++calls[0]));
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogContext ctx) {
            return behavior.apply(ctx);
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            throw new UnsupportedOperationException("Not used by these tests.");
        }

        @Override
        public FunctionKind getKind() {
            throw new UnsupportedOperationException("Not used by these tests.");
        }
    }

    // -------------------------------------------------------------------------
    // Resolved mode for fixed answers
    // -------------------------------------------------------------------------

    @Test
    void testInsertOnlyAnswerResolvesToInsertOnly() {
        StubChangelogFunction fn = StubChangelogFunction.fixed(ChangelogMode.insertOnly());

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.insertOnly());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("upsertAnswerCases")
    void testUpsertAnswerResolvesToItself(String name, ChangelogMode fixedAnswer) {
        StubChangelogFunction fn = StubChangelogFunction.fixed(fixedAnswer);

        assertThat(resolve(fn)).isEqualTo(fixedAnswer);
    }

    private static Stream<Arguments> upsertAnswerCases() {
        return Stream.of(
                Arguments.of("key-only deletes preserved", ChangelogMode.upsert(true)),
                Arguments.of(
                        "full deletes preserved when key-only hint is ignored",
                        ChangelogMode.upsert(false)));
    }

    @Test
    void testRetractAnswerResolvesToItself() {
        StubChangelogFunction fn = StubChangelogFunction.fixed(ChangelogMode.all());

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.all());
    }

    @Test
    void testUpdateModeWithoutDeleteHasNoKeyOnlyDeletes() {
        // A mode without DELETE has no key-only distinction to settle.
        StubChangelogFunction fn =
                StubChangelogFunction.fixed(
                        ChangelogMode.newBuilder()
                                .addContainedKind(RowKind.INSERT)
                                .addContainedKind(RowKind.UPDATE_AFTER)
                                .build());

        ChangelogMode result = resolve(fn);

        assertThat(result.contains(RowKind.DELETE)).isFalse();
        assertThat(result.keyOnlyDeletes()).isFalse();
    }

    // -------------------------------------------------------------------------
    // Assembling the resolved mode from hint-dependent answers
    // -------------------------------------------------------------------------

    @Test
    void testUpdateBeforeAddedWhenOnlyTheUpdateBeforeProbeReportsIt() {
        // The first probe answers upsert(true), later probes answer all(). UPDATE_BEFORE must come
        // from the later answer.
        StubChangelogFunction fn =
                StubChangelogFunction.perCall(
                        call -> call == 1 ? ChangelogMode.upsert(true) : ChangelogMode.all());

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.all());
    }

    @Test
    void testUpdateBeforeNotLeakedFromHintWhenNotRequested() {
        // UPDATE_BEFORE is never requested, so it must not appear.
        StubChangelogFunction fn =
                new StubChangelogFunction(
                        ctx ->
                                ctx.getRequiredChangelogMode().contains(RowKind.UPDATE_BEFORE)
                                        ? ChangelogMode.all()
                                        : ChangelogMode.upsert(true));

        ChangelogMode result = resolve(fn);

        assertThat(result).isEqualTo(ChangelogMode.upsert(true));
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isFalse();
    }

    @Test
    void testUpdateBeforeNotAddedWhenEmittedKindsLackUpdateAfter() {
        // UPDATE_BEFORE is only valid alongside UPDATE_AFTER, so an update-before-probe answer of
        // all() must not introduce it when the emitted kinds were [INSERT, DELETE].
        StubChangelogFunction fn =
                StubChangelogFunction.perCall(
                        call ->
                                call == 1
                                        ? ChangelogMode.newBuilder()
                                                .addContainedKind(RowKind.INSERT)
                                                .addContainedKind(RowKind.DELETE)
                                                .build()
                                        : ChangelogMode.all());

        ChangelogMode result = resolve(fn);

        assertThat(result.contains(RowKind.UPDATE_AFTER)).isFalse();
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isFalse();
        assertThat(result.contains(RowKind.DELETE)).isTrue();
    }

    @Test
    void testDeleteShapeProbeOnlyChangesKeyOnlyDeletesNotEmittedKinds() {
        // Only keyOnlyDeletes may change. The emitted kinds must survive.
        StubChangelogFunction fn =
                new StubChangelogFunction(
                        ctx -> {
                            ChangelogMode hint = ctx.getRequiredChangelogMode();
                            if (hint.contains(RowKind.UPDATE_BEFORE) || !hint.keyOnlyDeletes()) {
                                return ChangelogMode.upsert(false);
                            }
                            return ChangelogMode.insertOnly();
                        });

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.upsert(false));
    }

    // -------------------------------------------------------------------------
    // ChangelogContext exposed to the function
    // -------------------------------------------------------------------------

    @Test
    void testContextTableChangelogModesReflectConfiguredInputModes() {
        // Position 1 is a scalar, so it reports null.
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Arrays.asList(
                        new ProcessTableFunctionTestHarness.TableArgumentInfo(
                                TableArgument.forName("t1")
                                        .changelogMode(ChangelogMode.all())
                                        .build(),
                                DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT())),
                                EnumSet.of(
                                        StaticArgumentTrait.ROW_SEMANTIC_TABLE,
                                        StaticArgumentTrait.SUPPORT_UPDATES)),
                        new ProcessTableFunctionTestHarness.ScalarArgumentInfo(
                                "s", DataTypes.INT(), 1),
                        new ProcessTableFunctionTestHarness.TableArgumentInfo(
                                TableArgument.forName("t2").build(),
                                DataTypes.ROW(DataTypes.FIELD("n", DataTypes.STRING())),
                                EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)));

        Map<Integer, ChangelogMode> observed = new HashMap<>();
        StubChangelogFunction fn =
                new StubChangelogFunction(
                        ctx -> {
                            for (int pos = 0; pos < arguments.size(); pos++) {
                                observed.put(pos, ctx.getTableChangelogMode(pos));
                            }
                            return ChangelogMode.insertOnly();
                        });

        resolve(fn, arguments);

        assertThat(observed.get(0)).isEqualTo(ChangelogMode.all());
        assertThat(observed.get(1)).isNull();
        assertThat(observed.get(2)).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void testContextPositionsSkipStateArguments() {
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Arrays.asList(
                        new ProcessTableFunctionTestHarness.StateArgumentInfo(
                                "state",
                                DataTypes.ROW(DataTypes.FIELD("count", DataTypes.INT())),
                                Duration.ZERO),
                        new ProcessTableFunctionTestHarness.TableArgumentInfo(
                                TableArgument.forName("t")
                                        .changelogMode(ChangelogMode.all())
                                        .build(),
                                DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT())),
                                EnumSet.of(
                                        StaticArgumentTrait.ROW_SEMANTIC_TABLE,
                                        StaticArgumentTrait.SUPPORT_UPDATES)));

        ChangelogMode[] observed = new ChangelogMode[1];
        StubChangelogFunction fn =
                new StubChangelogFunction(
                        ctx -> {
                            observed[0] = ctx.getTableChangelogMode(0);
                            return ChangelogMode.insertOnly();
                        });

        resolve(fn, arguments);

        assertThat(observed[0]).isEqualTo(ChangelogMode.all());
    }

    @Test
    void testContextTableSemanticsReflectConfiguredPartitionAndUpsertKeys() {
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Collections.singletonList(
                        new ProcessTableFunctionTestHarness.TableArgumentInfo(
                                TableArgument.forName("input")
                                        .partitionBy("k")
                                        .upsertKey("k")
                                        .build(),
                                DataTypes.ROW(
                                        DataTypes.FIELD("k", DataTypes.STRING()),
                                        DataTypes.FIELD("v", DataTypes.INT())),
                                EnumSet.of(StaticArgumentTrait.SET_SEMANTIC_TABLE)));

        Map<Integer, TableSemantics> observed = new HashMap<>();
        StubChangelogFunction fn =
                new StubChangelogFunction(
                        ctx -> {
                            ctx.getTableSemantics(0).ifPresent(sem -> observed.put(0, sem));
                            return ChangelogMode.upsert(true);
                        });

        resolve(fn, arguments);

        TableSemantics semantics = observed.get(0);
        assertThat(semantics).isNotNull();
        assertThat(semantics.partitionByColumns()).containsExactly(0);
        assertThat(semantics.upsertKeyColumns()).hasSize(1);
        assertThat(semantics.upsertKeyColumns().get(0)).containsExactly(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("argumentValueCases")
    void testContextArgumentValue(
            String name,
            @Nullable Object configuredValue,
            Class<?> requestedClass,
            @Nullable Object expected) {
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Collections.singletonList(
                        new ProcessTableFunctionTestHarness.ScalarArgumentInfo(
                                "arg", DataTypes.INT(), configuredValue));

        Object[] captured = new Object[1];
        StubChangelogFunction fn =
                new StubChangelogFunction(
                        ctx -> {
                            captured[0] = ctx.getArgumentValue(0, requestedClass).orElse(null);
                            return ChangelogMode.insertOnly();
                        });

        resolve(fn, arguments);

        assertThat(captured[0]).isEqualTo(expected);
    }

    private static Stream<Arguments> argumentValueCases() {
        return Stream.of(
                Arguments.of("configured value is returned", 42, Integer.class, 42),
                Arguments.of("null value yields empty", null, Integer.class, null),
                Arguments.of("incompatible class yields empty", 42, String.class, null),
                Arguments.of("primitive class matches boxed value", 5, int.class, 5));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ChangelogMode resolve(ChangelogFunction fn) {
        return resolve(fn, Collections.emptyList());
    }

    private static ChangelogMode resolve(
            ChangelogFunction fn, List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments) {
        return new PtfChangelogModeResolver(fn, arguments).resolve();
    }
}
