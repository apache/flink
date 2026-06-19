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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Shared {@code error_handling} parameter values for built-in process table functions. Mode names
 * are case-sensitive — values must be spelled in upper case.
 *
 * <ul>
 *   <li>{@code FAIL} — throw an exception when an error condition is encountered (default, strict
 *       mode)
 *   <li>{@code SKIP} — silently drop the offending row
 * </ul>
 */
@Internal
public enum ErrorHandlingMode {
    FAIL,
    SKIP;

    public static final ErrorHandlingMode DEFAULT_MODE = FAIL;

    /**
     * Returns the mode whose name exactly matches {@code name}, or {@link Optional#empty()} if none
     * matches. Matching is case-sensitive.
     */
    public static Optional<ErrorHandlingMode> fromName(final String name) {
        return Arrays.stream(values()).filter(v -> v.name().equals(name)).findFirst();
    }

    public static String validNames() {
        return Arrays.stream(values()).map(Enum::name).collect(Collectors.joining(", "));
    }
}
