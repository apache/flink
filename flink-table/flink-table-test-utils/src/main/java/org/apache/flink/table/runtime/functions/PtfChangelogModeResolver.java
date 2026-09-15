/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.types.RowKind;

import org.apache.commons.lang3.ClassUtils;

import java.util.List;
import java.util.Optional;

/**
 * Derives the {@link ChangelogMode} a {@link ChangelogFunction}-implementing PTF would report from
 * {@link org.apache.flink.table.functions.ProcessTableFunction.Context#getChangelogMode()}.
 */
@Internal
final class PtfChangelogModeResolver {

    private static final ChangelogMode MOST_PERMISSIVE_UPSERT_HINT = ChangelogMode.upsert(false);

    private final ChangelogFunction function;
    private final List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments;

    PtfChangelogModeResolver(
            ChangelogFunction function,
            List<ProcessTableFunctionTestHarness.ArgumentInfo> arguments) {
        this.function = function;
        this.arguments = arguments;
    }

    ChangelogMode resolve() {
        ChangelogMode emittedKinds = probeEmittedKinds();
        if (emittedKinds.containsOnly(RowKind.INSERT)) {
            return assembleChangelogMode(emittedKinds, false, false);
        }

        boolean requiresUpdateBefore = probeRequiresUpdateBefore(emittedKinds);
        boolean keyOnlyDeletes =
                !requiresUpdateBefore
                        && emittedKinds.contains(RowKind.DELETE)
                        && probeUsesKeyOnlyDeletes(emittedKinds);

        return assembleChangelogMode(emittedKinds, requiresUpdateBefore, keyOnlyDeletes);
    }

    /**
     * Probes with the most permissive upsert hint, which never offers UPDATE_BEFORE, so the
     * function's intrinsic output is revealed: an append-only function answers INSERT-only.
     */
    private ChangelogMode probeEmittedKinds() {
        return function.getChangelogMode(contextFor(MOST_PERMISSIVE_UPSERT_HINT));
    }

    private boolean probeRequiresUpdateBefore(ChangelogMode emittedKinds) {
        ChangelogMode hint = containedKindsExceptUpdateBefore(emittedKinds).build();
        return function.getChangelogMode(contextFor(hint)).contains(RowKind.UPDATE_BEFORE);
    }

    private boolean probeUsesKeyOnlyDeletes(ChangelogMode emittedKinds) {
        ChangelogMode hint =
                containedKindsExceptUpdateBefore(emittedKinds).keyOnlyDeletes(true).build();
        return function.getChangelogMode(contextFor(hint)).keyOnlyDeletes();
    }

    private static ChangelogMode.Builder containedKindsExceptUpdateBefore(ChangelogMode mode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : mode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder;
    }

    private static ChangelogMode assembleChangelogMode(
            ChangelogMode emittedKinds, boolean requiresUpdateBefore, boolean keyOnlyDeletes) {
        ChangelogMode.Builder builder = containedKindsExceptUpdateBefore(emittedKinds);
        if (emittedKinds.contains(RowKind.DELETE)) {
            builder.keyOnlyDeletes(keyOnlyDeletes);
        }
        if (requiresUpdateBefore && emittedKinds.contains(RowKind.UPDATE_AFTER)) {
            builder.addContainedKind(RowKind.UPDATE_BEFORE);
        }
        return builder.build();
    }

    private <T extends ProcessTableFunctionTestHarness.ArgumentInfo> Optional<T> argumentAt(
            int pos, Class<T> type) {
        if (pos < 0 || pos >= arguments.size()) {
            return Optional.empty();
        }
        ProcessTableFunctionTestHarness.ArgumentInfo arg = arguments.get(pos);
        if (!type.isInstance(arg)) {
            return Optional.empty();
        }
        return Optional.of(type.cast(arg));
    }

    private ChangelogFunction.ChangelogContext contextFor(ChangelogMode syntheticRequiredMode) {
        return new ChangelogFunction.ChangelogContext() {
            @Override
            public ChangelogMode getTableChangelogMode(int pos) {
                return argumentAt(pos, ProcessTableFunctionTestHarness.TableArgumentInfo.class)
                        .map(arg -> arg.effectiveChangelogMode())
                        .orElse(null);
            }

            @Override
            public ChangelogMode getRequiredChangelogMode() {
                return syntheticRequiredMode;
            }

            @Override
            public Optional<TableSemantics> getTableSemantics(int pos) {
                return argumentAt(pos, ProcessTableFunctionTestHarness.TableArgumentInfo.class)
                        .map(
                                tableArg ->
                                        ProcessTableFunctionTestHarness.buildTableSemantics(
                                                tableArg, -1));
            }

            @Override
            public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
                return argumentAt(pos, ProcessTableFunctionTestHarness.ScalarArgumentInfo.class)
                        .flatMap(
                                arg -> {
                                    Object value = arg.value;
                                    if (value == null) {
                                        return Optional.empty();
                                    }
                                    Class<?> targetType = ClassUtils.primitiveToWrapper(clazz);
                                    if (targetType.isInstance(value)) {
                                        @SuppressWarnings("unchecked")
                                        T result = (T) targetType.cast(value);
                                        return Optional.of(result);
                                    }
                                    return Optional.empty();
                                });
            }
        };
    }
}
