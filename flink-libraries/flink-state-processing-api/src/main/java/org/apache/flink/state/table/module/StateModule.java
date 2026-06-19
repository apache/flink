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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.state.table.SavepointMetadataTableFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.DynamicBuiltInFunctionDefinitionFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Module of state in Flink. */
@Experimental
public class StateModule implements Module {

    private static final Logger LOG = LoggerFactory.getLogger(StateModule.class);

    public static final String IDENTIFIER = "state";

    public static final StateModule INSTANCE = new StateModule();

    private final Map<String, BuiltInFunctionDefinition> normalizedFunctions;
    private final Set<String> functionNamesWithInternal;
    private final Set<String> functionNamesWithoutInternal;

    private StateModule() {
        final List<BuiltInFunctionDefinition> definitions = new ArrayList<>();

        definitions.add(SavepointMetadataTableFunction.SAVEPOINT_METADATA);
        ServiceLoader.load(DynamicBuiltInFunctionDefinitionFactory.class)
                .iterator()
                .forEachRemaining(
                        f -> {
                            if (f.factoryIdentifier().startsWith(IDENTIFIER + ".")) {
                                definitions.addAll(f.getBuiltInFunctionDefinitions());
                            }
                        });
        checkDuplicatedFunctions(definitions);

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

    @VisibleForTesting
    static void checkDuplicatedFunctions(List<BuiltInFunctionDefinition> definitions) {
        Set<String> seen = new HashSet<>();
        Set<String> duplicates = new HashSet<>();

        for (BuiltInFunctionDefinition definition : definitions) {
            String name = definition.getName();
            if (!seen.add(name)) {
                duplicates.add(name);
            }
        }

        if (!duplicates.isEmpty()) {
            String error = "Duplicate function names found: " + String.join(",", duplicates);
            LOG.error(error);
            throw new IllegalStateException(error);
        }
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
