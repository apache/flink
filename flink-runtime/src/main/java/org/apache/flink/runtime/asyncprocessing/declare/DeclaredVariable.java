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

import javax.annotation.Nullable;

import java.util.function.Supplier;

/** A variable declared in async state processing. The value could be persisted in checkpoint. */
public class DeclaredVariable<T> {

    final DeclarationManager manager;

    final int ordinal;

    final TypeSerializer<T> typeSerializer;

    final String name;

    @Nullable final Supplier<T> initializer;

    DeclaredVariable(
            DeclarationManager manager,
            int ordinal,
            TypeSerializer<T> typeSerializer,
            String name,
            @Nullable Supplier<T> initializer) {
        this.manager = manager;
        this.ordinal = ordinal;
        this.typeSerializer = typeSerializer;
        this.name = name;
        this.initializer = initializer;
    }

    public T get() {
        T t = manager.getVariableValue(ordinal);
        if (t == null && initializer != null) {
            t = initializer.get();
            manager.setVariableValue(ordinal, t);
        }
        return t;
    }

    public void set(T newValue) {
        manager.setVariableValue(ordinal, newValue);
    }
}
