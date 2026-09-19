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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PtfChangelogModeResolver}, exercising it directly against mock {@link
 * ChangelogFunction} implementations rather than through {@link
 * ProcessTableFunctionTestHarness.Builder}.
 *
 * <p>The resolver probes the function up to three times with different required-mode hints to
 * settle whether the output carries UPDATE_BEFORE and whether deletes are key-only. These tests
 * assert the resolved mode and the {@link ChangelogFunction.ChangelogContext} the resolver exposes.
 */
class PtfChangelogModeResolverTest {

    /** A {@link ChangelogFunction} whose answer is driven by a supplied behavior function. */
    private static class StubChangelogFunction implements ChangelogFunction {
        private final Function<ChangelogContext, ChangelogMode> behavior;

        StubChangelogFunction(Function<ChangelogContext, ChangelogMode> behavior) {
            this.behavior = behavior;
        }

        /**
         * Always returns the same {@link ChangelogMode}, regardless of context — a valid pattern
         * per {@link ChangelogFunction}'s own Javadoc.
         */
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
        // A fixed upsert answer is preserved as-is. The upsert(false) case additionally proves that
        // a function ignoring the key-only-deletes hint keeps its full deletes, since a full delete
        // is a strictly compatible superset of a key-only delete.
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
        // A retract answer keeps UPDATE_BEFORE (a retract stream always carries full deletes).
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
        // The emitted-kinds probe answers upsert(true) (no UPDATE_BEFORE); the update-before probe
        // answers all(). The resolved mode must gain UPDATE_BEFORE from that later answer.
        StubChangelogFunction fn =
                StubChangelogFunction.perCall(
                        call -> call == 1 ? ChangelogMode.upsert(true) : ChangelogMode.all());

        assertThat(resolve(fn)).isEqualTo(ChangelogMode.all());
    }

    @Test
    void testUpdateBeforeNotLeakedFromHintWhenFunctionDoesNotAskForIt() {
        // The reverse direction: the function returns all() only when UPDATE_BEFORE is requested,
        // and the derived hints never request it. Without proper filtering, UPDATE_BEFORE would
        // leak into the result.
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
        // The delete-shape probe may only flip the keyOnlyDeletes bit, never the kind set from the
        // emitted-kinds probe. Here it degrades to insertOnly() when asked for key-only deletes;
        // the
        // upsert kind set must survive with keyOnlyDeletes=false.
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

    @Test
    void testInsertOnlyShortCircuitStripsStrayKeyOnlyDeletesFlag() {
        // The emitted-kinds probe answers [INSERT] with keyOnlyDeletes=true (an impossible
        // combination constructable programmatically but never produced in practice). The
        // insert-only short-circuit must still route through the assembler to strip the stray flag.
        StubChangelogFunction fn =
                StubChangelogFunction.fixed(
                        ChangelogMode.newBuilder()
                                .addContainedKind(RowKind.INSERT)
                                .keyOnlyDeletes(true)
                                .build());

        ChangelogMode result = resolve(fn);

        assertThat(result.contains(RowKind.INSERT)).isTrue();
        assertThat(result.keyOnlyDeletes()).isFalse();
    }

    // -------------------------------------------------------------------------
    // ChangelogContext exposed to the function
    // -------------------------------------------------------------------------

    @Test
    void testContextTableChangelogModesReflectConfiguredInputModes() {
        // A multi-argument PTF: table "t1" (configured to all()), a scalar in between (must report
        // null per ChangelogContext#getTableChangelogMode's own Javadoc), and table "t2" (left at
        // its insertOnly() default). Position 0 also confirms that the SQL-operand-ordered list the
        // resolver receives maps positionally onto configured modes.
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Arrays.asList(
                        tableArgWithChangelogMode(
                                "t1",
                                DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT())),
                                ChangelogMode.all(),
                                StaticArgumentTrait.ROW_SEMANTIC_TABLE,
                                StaticArgumentTrait.SUPPORT_UPDATES),
                        new ProcessTableFunctionTestHarness.ScalarArgumentInfo(
                                "s", DataTypes.INT(), 1),
                        tableArg(
                                "t2",
                                DataTypes.ROW(DataTypes.FIELD("n", DataTypes.STRING())),
                                null,
                                StaticArgumentTrait.ROW_SEMANTIC_TABLE));

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
    void testContextTableSemanticsReflectConfiguredPartitionAndUpsertKeys() {
        // The recorded TableSemantics must expose both the configured partition key and the upsert
        // key as resolved field indices, mirroring what production TableSemantics provides.
        List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments =
                Collections.singletonList(
                        tableArgWithUpsertKey(
                                "input",
                                DataTypes.ROW(
                                        DataTypes.FIELD("k", DataTypes.STRING()),
                                        DataTypes.FIELD("v", DataTypes.INT())),
                                new String[] {"k"},
                                new String[] {"k"},
                                StaticArgumentTrait.SET_SEMANTIC_TABLE));

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

    private static ProcessTableFunctionTestHarness.TableArgumentInfo tableArg(
            String name,
            DataType dataType,
            @Nullable String[] partitionColumnNames,
            StaticArgumentTrait... traits) {
        return new ProcessTableFunctionTestHarness.TableArgumentInfo(
                name,
                dataType,
                EnumSet.copyOf(Arrays.asList(traits)),
                partitionColumnNames,
                null,
                null);
    }

    private static ProcessTableFunctionTestHarness.TableArgumentInfo tableArgWithChangelogMode(
            String name,
            DataType dataType,
            ChangelogMode changelogMode,
            StaticArgumentTrait... traits) {
        return new ProcessTableFunctionTestHarness.TableArgumentInfo(
                name, dataType, EnumSet.copyOf(Arrays.asList(traits)), null, changelogMode, null);
    }

    private static ProcessTableFunctionTestHarness.TableArgumentInfo tableArgWithUpsertKey(
            String name,
            DataType dataType,
            @Nullable String[] partitionColumnNames,
            String[] upsertKey,
            StaticArgumentTrait... traits) {
        return new ProcessTableFunctionTestHarness.TableArgumentInfo(
                name,
                dataType,
                EnumSet.copyOf(Arrays.asList(traits)),
                partitionColumnNames,
                null,
                Collections.singletonList(upsertKey));
    }
}
