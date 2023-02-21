/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util.function;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.function.Supplier;

/** A {@link Supplier} that returns a single, lazily instantiated, value. */
@NotThreadSafe
public class CachingSupplier<T> implements Supplier<T> {
    private final Supplier<T> backingSupplier;
    private @Nullable T cachedValue;

    public CachingSupplier(Supplier<T> backingSupplier) {
        this.backingSupplier = backingSupplier;
    }

    @Override
    public T get() {
        if (cachedValue == null) {
            cachedValue = backingSupplier.get();
        }
        return cachedValue;
    }
}
