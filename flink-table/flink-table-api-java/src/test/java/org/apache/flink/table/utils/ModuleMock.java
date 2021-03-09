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

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Mocking {@link Module} for tests. */
public class ModuleMock implements Module {
    private static final Set<String> BUILT_IN_FUNCTIONS =
            Collections.unmodifiableSet(new HashSet<>(Collections.singletonList("dummy")));
    private final String type;
    private final FunctionDefinitionMock functionDef;

    public ModuleMock(String type) {
        this.type = type;
        functionDef = new FunctionDefinitionMock();
        functionDef.functionKind = FunctionKind.OTHER;
    }

    public String getType() {
        return type;
    }

    @Override
    public Set<String> listFunctions() {
        return BUILT_IN_FUNCTIONS;
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        if (BUILT_IN_FUNCTIONS.contains(name)) {
            return Optional.of(functionDef);
        }
        return Optional.empty();
    }
}
