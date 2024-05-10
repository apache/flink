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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** The manager holds all the declaration information and manage the building procedure. */
public class DeclarationManager {

    private final Map<String, NamedCallback> knownCallbacks;

    private final Map<String, DeclaredVariable> knownVariables;

    private int nextValidNameSequence = 0;

    public DeclarationManager() {
        this.knownCallbacks = new HashMap<>();
        this.knownVariables = new HashMap<>();
    }

    <T extends NamedCallback> T register(T knownCallback) throws DeclarationException {
        if (knownCallbacks.put(knownCallback.getName(), knownCallback) != null) {
            throw new DeclarationException("Duplicated key " + knownCallback.getName());
        }
        return knownCallback;
    }

    <T> DeclaredVariable<T> register(
            TypeInformation<T> typeInformation, String name, Supplier<T> initializer)
            throws DeclarationException {
        DeclaredVariable<T> variable = new DeclaredVariable<>(typeInformation, name, initializer);
        if (knownVariables.put(name, variable) != null) {
            throw new DeclarationException("Duplicated key " + name);
        }
        return variable;
    }

    String nextAssignedName() {
        String name;
        do {
            name = String.format("___%d___", nextValidNameSequence++);
        } while (knownCallbacks.containsKey(name));
        return name;
    }

    public void assignVariables(RecordContext<?> context) {
        int i = 0;
        for (DeclaredVariable variable : knownVariables.values()) {
            AtomicReference reference = context.getVariableReference(i);
            if (reference == null) {
                reference = new AtomicReference(variable.initializer.get());
                context.setVariableReference(i, reference);
            }
            variable.setReference(reference);
        }
    }

    public int variableCount() {
        return knownVariables.size();
    }

    public <T> ThrowingConsumer<T, Exception> buildProcess(
            FunctionWithException<
                            DeclarationContext,
                            ThrowingConsumer<T, Exception>,
                            DeclarationException>
                    declaringMethod) {
        final DeclarationContext context = new DeclarationContext(this);
        try {
            return declaringMethod.apply(context);
        } catch (DeclarationException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
