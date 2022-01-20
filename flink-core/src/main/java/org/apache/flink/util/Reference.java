/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.util.Optional;

/**
 * Wrapper class that allows to express whether the value is borrowed or owned.
 *
 * @param <T> type of the value
 */
public final class Reference<T> {
    private final T value;

    private final boolean isOwned;

    private Reference(T value, boolean isOwned) {
        this.value = value;
        this.isOwned = isOwned;
    }

    public T deref() {
        return value;
    }

    /** Returns the value if it is owned. */
    public Optional<T> owned() {
        return isOwned ? Optional.of(value) : Optional.empty();
    }

    public boolean isOwned() {
        return isOwned;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    public static <V> Reference<V> owned(V value) {
        return new Reference<>(value, true);
    }

    public static <V> Reference<V> borrowed(V value) {
        return new Reference<>(value, false);
    }
}
