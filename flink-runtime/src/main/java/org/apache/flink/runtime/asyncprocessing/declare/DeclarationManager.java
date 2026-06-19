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

package org.apache.flink.runtime.asyncprocessing.declare;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.asyncprocessing.RecordContext;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** The manager holds all the declaration information and manage the building procedure. */
public class DeclarationManager {

    private final Map<String, NamedCallback> knownCallbacks;

    private final Map<String, DeclaredVariable<?>> knownVariables;

    private RecordContext<?> currentContext;

    private int nextValidNameSequence = 0;

    private int contextVariableCount = 0;

    public DeclarationManager() {
        this.knownCallbacks = new HashMap<>();
        this.knownVariables = new HashMap<>();
    }

    <T extends NamedCallback> T register(T knownCallback) throws DeclarationException {
        if (knownCallbacks.put(knownCallback.getName(), knownCallback) != null) {
            throw new DeclarationException("Duplicated function key " + knownCallback.getName());
        }
        return knownCallback;
    }

    <T> ContextVariable<T> registerVariable(@Nullable Supplier<T> initializer)
            throws DeclarationException {
        return new ContextVariable<>(this, contextVariableCount++, initializer);
    }

    <T> DeclaredVariable<T> registerVariable(
            TypeSerializer<T> serializer, String name, @Nullable Supplier<T> initializer)
            throws DeclarationException {
        if (knownVariables.containsKey(name)) {
            throw new DeclarationException("Duplicated variable key " + name);
        }
        DeclaredVariable<T> variable =
                new DeclaredVariable<>(this, contextVariableCount++, serializer, name, initializer);
        knownVariables.put(name, variable);
        return variable;
    }

    public void setCurrentContext(RecordContext<?> currentContext) {
        this.currentContext = currentContext;
    }

    public <T> T getVariableValue(int ordinal) {
        if (currentContext == null) {
            throw new UnsupportedOperationException("There is no current keyed context.");
        }
        return currentContext.getVariable(ordinal);
    }

    public <T> void setVariableValue(int ordinal, T value) {
        if (currentContext == null) {
            throw new UnsupportedOperationException("There is no current keyed context.");
        }
        currentContext.setVariable(ordinal, value);
    }

    public int variableCount() {
        return contextVariableCount;
    }

    String nextAssignedName(String prefix) {
        String name;
        do {
            name = String.format("___%s%d___", prefix, nextValidNameSequence++);
        } while (knownCallbacks.containsKey(name) || knownVariables.containsKey(name));
        return name;
    }
}
