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

import javax.annotation.Nullable;

import java.util.function.Supplier;

/** A value that will have different values across different contexts. */
public class ContextVariable<T> {

    final DeclarationManager manager;

    final int ordinal;

    @Nullable final Supplier<T> initializer;

    boolean initialized = false;

    ContextVariable(DeclarationManager manager, int ordinal, Supplier<T> initializer) {
        this.manager = manager;
        this.ordinal = ordinal;
        this.initializer = initializer;
    }

    public T get() {
        if (!initialized && initializer != null) {
            manager.setVariableValue(ordinal, initializer.get());
            initialized = true;
        }
        return manager.getVariableValue(ordinal);
    }

    public void set(T newValue) {
        initialized = true;
        manager.setVariableValue(ordinal, newValue);
    }
}
