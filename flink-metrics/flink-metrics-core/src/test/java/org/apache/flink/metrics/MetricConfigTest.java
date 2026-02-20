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

package org.apache.flink.metrics;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MetricConfig}. */
class MetricConfigTest {

    @ParameterizedTest
    @MethodSource("fromStringCases")
    void testGetFromString(
            final String storedValue, final TypedGetter getter, final Object expected) {
        final MetricConfig config = new MetricConfig();
        config.setProperty("key", storedValue);
        assertThat(getter.get(config, "key")).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("nativeTypeCases")
    void testGetFromNativeType(
            final Object storedValue, final TypedGetter getter, final Object expected) {
        final MetricConfig config = new MetricConfig();
        config.put("key", storedValue);
        assertThat(getter.get(config, "key")).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("crossTypeCases")
    void testGetCrossType(
            final Object storedValue, final TypedGetter getter, final Object expected) {
        final MetricConfig config = new MetricConfig();
        config.put("key", storedValue);
        assertThat(getter.get(config, "key")).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("defaultValueCases")
    void testGetDefault(final TypedGetter getter, final Object expected) {
        final MetricConfig config = new MetricConfig();
        assertThat(getter.get(config, "missing")).isEqualTo(expected);
    }

    @FunctionalInterface
    private interface TypedGetter {
        Object get(MetricConfig config, String key);
    }

    private static Stream<Arguments> fromStringCases() {
        return Stream.of(
                Arguments.of("42", (TypedGetter) (c, k) -> c.getInteger(k, 0), 42),
                Arguments.of(
                        "123456789012345",
                        (TypedGetter) (c, k) -> c.getLong(k, 0L),
                        123456789012345L),
                Arguments.of("3.14", (TypedGetter) (c, k) -> c.getFloat(k, 0.0f), 3.14f),
                Arguments.of(
                        "2.718281828", (TypedGetter) (c, k) -> c.getDouble(k, 0.0), 2.718281828),
                Arguments.of("true", (TypedGetter) (c, k) -> c.getBoolean(k, false), true));
    }

    private static Stream<Arguments> nativeTypeCases() {
        return Stream.of(
                Arguments.of(42, (TypedGetter) (c, k) -> c.getInteger(k, 0), 42),
                Arguments.of(
                        123456789012345L,
                        (TypedGetter) (c, k) -> c.getLong(k, 0L),
                        123456789012345L),
                Arguments.of(3.14f, (TypedGetter) (c, k) -> c.getFloat(k, 0.0f), 3.14f),
                Arguments.of(2.718281828, (TypedGetter) (c, k) -> c.getDouble(k, 0.0), 2.718281828),
                Arguments.of(true, (TypedGetter) (c, k) -> c.getBoolean(k, false), true));
    }

    private static Stream<Arguments> crossTypeCases() {
        return Stream.of(
                Arguments.of(42, (TypedGetter) (c, k) -> c.getLong(k, 0L), 42L),
                Arguments.of(100L, (TypedGetter) (c, k) -> c.getInteger(k, 0), 100),
                Arguments.of(42, (TypedGetter) (c, k) -> c.getDouble(k, 0.0), 42.0),
                Arguments.of(42, (TypedGetter) (c, k) -> c.getFloat(k, 0.0f), 42.0f));
    }

    private static Stream<Arguments> defaultValueCases() {
        return Stream.of(
                Arguments.of((TypedGetter) (c, k) -> c.getInteger(k, 99), 99),
                Arguments.of((TypedGetter) (c, k) -> c.getLong(k, 99L), 99L),
                Arguments.of((TypedGetter) (c, k) -> c.getFloat(k, 1.5f), 1.5f),
                Arguments.of((TypedGetter) (c, k) -> c.getDouble(k, 1.5), 1.5),
                Arguments.of((TypedGetter) (c, k) -> c.getBoolean(k, true), true),
                Arguments.of((TypedGetter) (c, k) -> c.getBoolean(k, false), false),
                Arguments.of((TypedGetter) (c, k) -> c.getString(k, "default"), "default"));
    }
}
