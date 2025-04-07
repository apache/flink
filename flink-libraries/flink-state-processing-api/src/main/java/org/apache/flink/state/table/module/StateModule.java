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

package org.apache.flink.state.table.module;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.state.table.SavepointMetadataTableFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.types.inference.TypeStrategies;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.FunctionKind.TABLE;

/** Module of state in Flink. */
@Experimental
public class StateModule implements Module {

    public static final String IDENTIFIER = "state";

    public static final BuiltInFunctionDefinition SAVEPOINT_METADATA =
            BuiltInFunctionDefinition.newBuilder()
                    .name("savepoint_metadata")
                    .kind(TABLE)
                    .runtimeClass(SavepointMetadataTableFunction.class.getName())
                    .outputTypeStrategy(
                            TypeStrategies.explicit(
                                    DataTypes.ROW(
                                            DataTypes.FIELD(
                                                    "checkpoint-id", DataTypes.BIGINT().notNull()),
                                            DataTypes.FIELD("operator-name", DataTypes.STRING()),
                                            DataTypes.FIELD("operator-uid", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "operator-uid-hash",
                                                    DataTypes.STRING().notNull()),
                                            DataTypes.FIELD(
                                                    "operator-parallelism",
                                                    DataTypes.INT().notNull()),
                                            DataTypes.FIELD(
                                                    "operator-max-parallelism",
                                                    DataTypes.INT().notNull()),
                                            DataTypes.FIELD(
                                                    "operator-subtask-state-count",
                                                    DataTypes.INT().notNull()),
                                            DataTypes.FIELD(
                                                    "operator-coordinator-state-size-in-bytes",
                                                    DataTypes.BIGINT().notNull()),
                                            DataTypes.FIELD(
                                                    "operator-total-size-in-bytes",
                                                    DataTypes.BIGINT().notNull()))))
                    .build();

    public static final StateModule INSTANCE = new StateModule();

    private final Map<String, BuiltInFunctionDefinition> normalizedFunctions;
    private final Set<String> functionNamesWithInternal;
    private final Set<String> functionNamesWithoutInternal;

    private StateModule() {
        final List<BuiltInFunctionDefinition> definitions =
                Collections.singletonList(SAVEPOINT_METADATA);
        this.normalizedFunctions =
                definitions.stream()
                        .collect(
                                Collectors.toMap(
                                        f -> f.getName().toUpperCase(Locale.ROOT),
                                        Function.identity()));
        this.functionNamesWithInternal =
                definitions.stream()
                        .map(BuiltInFunctionDefinition::getName)
                        .collect(Collectors.toSet());
        this.functionNamesWithoutInternal =
                definitions.stream()
                        .filter(f -> !f.isInternal())
                        .map(BuiltInFunctionDefinition::getName)
                        .collect(Collectors.toSet());
    }

    @Override
    public Set<String> listFunctions() {
        return listFunctions(false);
    }

    @Override
    public Set<String> listFunctions(boolean includeHiddenFunctions) {
        if (includeHiddenFunctions) {
            return functionNamesWithInternal;
        } else {
            return functionNamesWithoutInternal;
        }
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        final String normalizedName = name.toUpperCase(Locale.ROOT);
        return Optional.ofNullable(normalizedFunctions.get(normalizedName));
    }
}
