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

package org.apache.flink.table.utils;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.types.inference.utils.FunctionDefinitionMock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Mocking {@link Module} for tests. */
public class ModuleMock implements Module {

    public static final String DUMMY_FUNCTION_NAME = "dummy";
    public static final FunctionDefinition DUMMY_FUNCTION_DEFINITION =
            new FunctionDefinitionMock(FunctionKind.SCALAR, null);

    public static final String INTERNAL_FUNCTION_NAME = "internal";
    public static final FunctionDefinition INTERNAL_FUNCTION_DEFINITION =
            new FunctionDefinitionMock(FunctionKind.OTHER, null);

    private static final Set<String> BUILT_IN_FUNCTIONS =
            Collections.unmodifiableSet(
                    new HashSet<>(Collections.singletonList(DUMMY_FUNCTION_NAME)));
    private static final Set<String> BUILT_IN_FUNCTIONS_WITH_INTERNAL =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(DUMMY_FUNCTION_NAME, INTERNAL_FUNCTION_NAME)));

    private final String type;

    public ModuleMock(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public Set<String> listFunctions() {
        return listFunctions(false);
    }

    @Override
    public Set<String> listFunctions(boolean includeHiddenFunctions) {
        if (includeHiddenFunctions) {
            return BUILT_IN_FUNCTIONS_WITH_INTERNAL;
        } else {
            return BUILT_IN_FUNCTIONS;
        }
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        switch (name) {
            case DUMMY_FUNCTION_NAME:
                return Optional.of(DUMMY_FUNCTION_DEFINITION);
            case INTERNAL_FUNCTION_NAME:
                return Optional.of(INTERNAL_FUNCTION_DEFINITION);
        }

        return Optional.empty();
    }
}
