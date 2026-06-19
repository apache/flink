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

import org.apache.flink.annotation.Internal;

import java.util.Optional;
import java.util.stream.Stream;

/** Utilities for working with {@link Optional}. */
@Internal
public class OptionalUtils {

    /**
     * Converts the given {@link Optional} into a {@link Stream}.
     *
     * <p>This is akin to {@code Optional#stream} available in JDK9+.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> Stream<T> stream(Optional<T> opt) {
        return opt.map(Stream::of).orElseGet(Stream::empty);
    }

    /** Returns the first {@link Optional} which is present. */
    @SafeVarargs
    public static <T> Optional<T> firstPresent(Optional<T>... opts) {
        for (Optional<T> opt : opts) {
            if (opt.isPresent()) {
                return opt;
            }
        }

        return Optional.empty();
    }

    private OptionalUtils() {}
}
