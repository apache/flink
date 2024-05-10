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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** A variable declared in async state processing. The value could be persisted in checkpoint. */
public class DeclaredVariable<T> {

    final TypeInformation<T> typeInformation;

    final String name;

    final Supplier<T> initializer;

    AtomicReference<T> reference;

    DeclaredVariable(TypeInformation<T> typeInformation, String name, Supplier<T> initializer) {
        this.typeInformation = typeInformation;
        this.name = name;
        this.initializer = initializer;
    }

    void setReference(AtomicReference<T> reference) {
        this.reference = reference;
    }

    public T get() {
        return reference.get();
    }

    public void set(T newValue) {
        reference.set(newValue);
    }
}
