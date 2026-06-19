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

package org.apache.flink.types;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/** Serializable {@link Optional}. */
public final class SerializableOptional<T extends Serializable> implements Serializable {
    private static final long serialVersionUID = -3312769593551775940L;

    private static final SerializableOptional<?> EMPTY = new SerializableOptional<>(null);

    @Nullable private final T value;

    private SerializableOptional(@Nullable T value) {
        this.value = value;
    }

    public T get() {
        if (value == null) {
            throw new NoSuchElementException("No value present");
        }
        return value;
    }

    public boolean isPresent() {
        return value != null;
    }

    public void ifPresent(Consumer<? super T> consumer) {
        if (value != null) {
            consumer.accept(value);
        }
    }

    public <R extends Serializable> SerializableOptional<R> map(
            Function<? super T, ? extends R> mapper) {
        if (value == null) {
            return empty();
        } else {
            return ofNullable(mapper.apply(value));
        }
    }

    public Optional<T> toOptional() {
        return Optional.ofNullable(value);
    }

    public static <T extends Serializable> SerializableOptional<T> of(@Nonnull T value) {
        return new SerializableOptional<>(value);
    }

    public static <T extends Serializable> SerializableOptional<T> ofNullable(@Nullable T value) {
        if (value == null) {
            return empty();
        } else {
            return of(value);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> SerializableOptional<T> empty() {
        return (SerializableOptional<T>) EMPTY;
    }
}
