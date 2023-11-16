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

package org.apache.flink.configuration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests read access ({@link ReadableConfig}) to {@link Configuration}. There are 4 different test
 * scenarios:
 *
 * <ol>
 *   <li>Tests reading an object that is kept as an object (when set directly through {@link
 *       Configuration#set(ConfigOption, Object)}.
 *   <li>Tests reading an object that was read from a config file, thus is stored as a string.
 *   <li>Tests using the {@link ConfigOption#defaultValue()} if no key is present in the {@link
 *       Configuration}.
 *   <li>Tests that the {@link ConfigOption#defaultValue()} is not used when calling {@link
 *       ReadableConfig#getOptional(ConfigOption)}.
 * </ol>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class ReadableWritableConfigurationTest {

    @Parameters(name = "testSpec = {0}, standardYaml = {1}")
    public static Collection<Object[]> parameters() {
        List<TestSpec<?>> testSpecs =
                Arrays.asList(
                        new TestSpec<>(ConfigOptions.key("int").intType().defaultValue(-1))
                                .valueEquals(12345, "12345", "12345")
                                .checkDefaultOverride(5),
                        new TestSpec<>(ConfigOptions.key("long").longType().defaultValue(-1L))
                                .valueEquals(12345L, "12345", "12345")
                                .checkDefaultOverride(5L),
                        new TestSpec<>(ConfigOptions.key("float").floatType().defaultValue(0.01F))
                                .valueEquals(0.003F, "0.003", "0.003")
                                .checkDefaultOverride(1.23F),
                        new TestSpec<>(ConfigOptions.key("double").doubleType().defaultValue(0.01D))
                                .valueEquals(0.003D, "0.003", "0.003")
                                .checkDefaultOverride(1.23D),
                        new TestSpec<>(
                                        ConfigOptions.key("boolean")
                                                .booleanType()
                                                .defaultValue(false))
                                .valueEquals(true, "true", "true")
                                .checkDefaultOverride(true),
                        new TestSpec<>(
                                        ConfigOptions.key("list<int>")
                                                .intType()
                                                .asList()
                                                .defaultValues(-1, 2, 3))
                                .valueEquals(
                                        Arrays.asList(1, 2, 3, 4, 5),
                                        "1;2;3;4;5",
                                        "[1, 2, 3, 4, 5]")
                                .checkDefaultOverride(Arrays.asList(1, 2)),
                        new TestSpec<>(
                                        ConfigOptions.key("list<string>")
                                                .stringType()
                                                .asList()
                                                .defaultValues("A", "B", "C"))
                                .valueEquals(Arrays.asList("A;B", "C"), "'A;B';C", "['A;B', C]")
                                .checkDefaultOverride(Collections.singletonList("C")),
                        new TestSpec<>(
                                        ConfigOptions.key("interval")
                                                .durationType()
                                                .defaultValue(Duration.ofHours(3)))
                                .valueEquals(Duration.ofMinutes(3), "3 min", "3 min")
                                .checkDefaultOverride(Duration.ofSeconds(1)),
                        new TestSpec<>(
                                        ConfigOptions.key("memory")
                                                .memoryType()
                                                .defaultValue(new MemorySize(1024)))
                                .valueEquals(new MemorySize(1024 * 1024 * 1024), "1g", "1g")
                                .checkDefaultOverride(new MemorySize(2048)),
                        new TestSpec<>(
                                        ConfigOptions.key("properties")
                                                .mapType()
                                                .defaultValue(
                                                        asMap(
                                                                Collections.singletonList(
                                                                        Tuple2.of(
                                                                                "prop1",
                                                                                "value1")))))
                                .valueEquals(
                                        asMap(
                                                Arrays.asList(
                                                        Tuple2.of("key1", "value1"),
                                                        Tuple2.of("key2", "value2"))),
                                        "key1:value1,key2:value2",
                                        "{key1: value1, key2: value2}")
                                .checkDefaultOverride(Collections.emptyMap()),
                        new TestSpec<>(
                                        ConfigOptions.key("list<properties>")
                                                .mapType()
                                                .asList()
                                                .defaultValues(
                                                        asMap(
                                                                Collections.singletonList(
                                                                        Tuple2.of(
                                                                                "prop1",
                                                                                "value1")))))
                                .valueEquals(
                                        Arrays.asList(
                                                asMap(
                                                        Arrays.asList(
                                                                Tuple2.of("key1", "value1"),
                                                                Tuple2.of("key2", "value2"))),
                                                asMap(Arrays.asList(Tuple2.of("key3", "value3")))),
                                        "key1:value1,key2:value2;key3:value3",
                                        "[{key1: value1, key2: value2}, {key3: value3}]")
                                .checkDefaultOverride(Collections.emptyList()));
        List<Object[]> list = new ArrayList<>();
        for (TestSpec<?> testSpec : testSpecs) {
            list.add(new Object[] {testSpec, true});
            list.add(new Object[] {testSpec, false});
        }
        return list;
    }

    private static Map<String, String> asMap(List<Tuple2<String, String>> entries) {
        return entries.stream().collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    @Parameter TestSpec<?> testSpec;

    @Parameter(value = 1)
    boolean standardYaml;

    @TestTemplate
    void testGetOptionalFromObject() {
        Configuration configuration = new Configuration(standardYaml);
        testSpec.setValue(configuration);

        Optional<?> optional = configuration.getOptional(testSpec.getOption());
        assertThat(optional.get(), equalTo(testSpec.getValue()));
    }

    @TestTemplate
    void testGetOptionalFromString() {
        ConfigOption<?> option = testSpec.getOption();
        Configuration configuration = new Configuration(standardYaml);
        configuration.setString(option.key(), testSpec.getStringValue(standardYaml));

        Optional<?> optional = configuration.getOptional(option);
        assertThat(optional.get(), equalTo(testSpec.getValue()));
    }

    @TestTemplate
    void testGetDefaultValue() {
        Configuration configuration = new Configuration(standardYaml);

        ConfigOption<?> option = testSpec.getOption();
        Object value = configuration.get(option);
        assertThat(value, equalTo(option.defaultValue()));
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void testGetOptionalDefaultValueOverride() {
        ReadableConfig configuration = new Configuration(standardYaml);

        ConfigOption<?> option = testSpec.getOption();
        Object value =
                ((Optional<Object>) configuration.getOptional(option))
                        .orElse(testSpec.getDefaultValueOverride());
        assertThat(value, equalTo(testSpec.getDefaultValueOverride()));
    }

    private static class TestSpec<T> {
        private final ConfigOption<T> option;
        private T value;
        private String stringValue;
        private String yamlStringValue;
        private T defaultValueOverride;

        private TestSpec(ConfigOption<T> option) {
            this.option = option;
        }

        TestSpec<T> valueEquals(T objectValue, String stringValue, String yamlStringValue) {
            this.value = objectValue;
            this.stringValue = stringValue;
            this.yamlStringValue = yamlStringValue;
            return this;
        }

        TestSpec<T> checkDefaultOverride(T defaultValueOverride) {
            Preconditions.checkArgument(
                    !Objects.equals(defaultValueOverride, option.defaultValue()),
                    "Default value override should be different from the config option default.");
            this.defaultValueOverride = defaultValueOverride;
            return this;
        }

        ConfigOption<T> getOption() {
            return option;
        }

        T getValue() {
            return value;
        }

        String getStringValue(boolean standardYaml) {
            if (standardYaml) {
                return yamlStringValue;
            } else {
                return stringValue;
            }
        }

        T getDefaultValueOverride() {
            return defaultValueOverride;
        }

        /**
         * Workaround to set the value in the configuration. We cannot set in the test itself as the
         * type of the TypeSpec is erased, because it used for parameterizing the test suite.
         */
        void setValue(Configuration configuration) {
            configuration.set(option, value);
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "option="
                    + option
                    + ", value="
                    + value
                    + ", stringValue='"
                    + stringValue
                    + ", yamlStringValue='"
                    + yamlStringValue
                    + '\''
                    + ", defaultValueOverride="
                    + defaultValueOverride
                    + '}';
        }
    }
}
