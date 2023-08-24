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

package org.apache.flink.table.module;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Module of default core metadata in Flink. */
@PublicEvolving
public class CoreModule implements Module {

    public static final CoreModule INSTANCE = new CoreModule();

    private final Map<String, BuiltInFunctionDefinition> normalizedFunctions;
    private final Set<String> functionNamesWithInternal;
    private final Set<String> functionNamesWithoutInternal;

    private CoreModule() {
        final List<BuiltInFunctionDefinition> definitions =
                BuiltInFunctionDefinitions.getDefinitions();
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
