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

package org.apache.flink.util;

import org.apache.flink.util.function.ThrowingRunnable;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Utility class which allows to run code depending on whether the optional has a value or is empty.
 *
 * <p>This code has been copied from: https://stackoverflow.com/a/29395324/4815083.
 *
 * @param <T> type of the optional
 */
public class OptionalConsumer<T> {
    private final Optional<T> optional;

    private OptionalConsumer(Optional<T> optional) {
        this.optional = Preconditions.checkNotNull(optional);
    }

    public static <T> OptionalConsumer<T> of(Optional<T> optional) {
        return new OptionalConsumer<>(optional);
    }

    public OptionalConsumer<T> ifPresent(Consumer<T> c) {
        optional.ifPresent(c);
        return this;
    }

    public <E extends Exception> OptionalConsumer<T> ifNotPresent(ThrowingRunnable<E> r) throws E {
        if (!optional.isPresent()) {
            r.run();
        }

        return this;
    }
}
