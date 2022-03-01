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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for configuring {@link CheckpointConfig} via {@link
 * CheckpointConfig#configure(ReadableConfig)}.
 */
public class CheckpointConfigFromConfigurationTest {

    private static Stream<TestSpec<?>> specs() {
        return Stream.of(
                TestSpec.testValue(CheckpointingMode.AT_LEAST_ONCE)
                        .whenSetFromFile("execution.checkpointing.mode", "AT_LEAST_ONCE")
                        .viaSetter(CheckpointConfig::setCheckpointingMode)
                        .getterVia(CheckpointConfig::getCheckpointingMode)
                        .nonDefaultValue(CheckpointingMode.AT_LEAST_ONCE),
                TestSpec.testValue(10000L)
                        .whenSetFromFile("execution.checkpointing.interval", "10 s")
                        .viaSetter(CheckpointConfig::setCheckpointInterval)
                        .getterVia(CheckpointConfig::getCheckpointInterval)
                        .nonDefaultValue(100L),
                TestSpec.testValue(12000L)
                        .whenSetFromFile("execution.checkpointing.timeout", "12 s")
                        .viaSetter(CheckpointConfig::setCheckpointTimeout)
                        .getterVia(CheckpointConfig::getCheckpointTimeout)
                        .nonDefaultValue(100L),
                TestSpec.testValue(12)
                        .whenSetFromFile("execution.checkpointing.max-concurrent-checkpoints", "12")
                        .viaSetter(CheckpointConfig::setMaxConcurrentCheckpoints)
                        .getterVia(CheckpointConfig::getMaxConcurrentCheckpoints)
                        .nonDefaultValue(100),
                TestSpec.testValue(1000L)
                        .whenSetFromFile("execution.checkpointing.min-pause", "1 s")
                        .viaSetter(CheckpointConfig::setMinPauseBetweenCheckpoints)
                        .getterVia(CheckpointConfig::getMinPauseBetweenCheckpoints)
                        .nonDefaultValue(100L),
                TestSpec.testValue(
                                CheckpointConfig.ExternalizedCheckpointCleanup
                                        .RETAIN_ON_CANCELLATION)
                        .whenSetFromFile(
                                "execution.checkpointing.externalized-checkpoint-retention",
                                "RETAIN_ON_CANCELLATION")
                        .viaSetter(CheckpointConfig::setExternalizedCheckpointCleanup)
                        .getterVia(CheckpointConfig::getExternalizedCheckpointCleanup)
                        .nonDefaultValue(
                                CheckpointConfig.ExternalizedCheckpointCleanup
                                        .DELETE_ON_CANCELLATION),
                TestSpec.testValue(12)
                        .whenSetFromFile(
                                "execution.checkpointing.tolerable-failed-checkpoints", "12")
                        .viaSetter(CheckpointConfig::setTolerableCheckpointFailureNumber)
                        .getterVia(CheckpointConfig::getTolerableCheckpointFailureNumber)
                        .nonDefaultValue(100),
                TestSpec.testValue(true)
                        .whenSetFromFile("execution.checkpointing.unaligned", "true")
                        .viaSetter(CheckpointConfig::enableUnalignedCheckpoints)
                        .getterVia(CheckpointConfig::isUnalignedCheckpointsEnabled)
                        .nonDefaultValue(true),
                TestSpec.testValue(
                                (CheckpointStorage)
                                        new FileSystemCheckpointStorage(
                                                "file:///path/to/checkpoint/dir"))
                        .whenSetFromFile(
                                CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                                "file:///path/to/checkpoint/dir")
                        .viaSetter(CheckpointConfig::setCheckpointStorage)
                        .getterVia(CheckpointConfig::getCheckpointStorage)
                        .nonDefaultValue(
                                new FileSystemCheckpointStorage("file:///path/to/checkpoint/dir"))
                        .customMatcher(
                                (actualValue, expectedValue) ->
                                        assertThat(actualValue)
                                                .hasSameClassAs(expectedValue)
                                                .asInstanceOf(
                                                        InstanceOfAssertFactories.type(
                                                                FileSystemCheckpointStorage.class))
                                                .extracting(
                                                        FileSystemCheckpointStorage
                                                                ::getCheckpointPath)
                                                .isEqualTo(
                                                        ((FileSystemCheckpointStorage)
                                                                        expectedValue)
                                                                .getCheckpointPath())));
    }

    @ParameterizedTest
    @MethodSource("specs")
    public void testLoadingFromConfiguration(TestSpec<?> spec) {
        CheckpointConfig configFromSetters = new CheckpointConfig();
        CheckpointConfig configFromFile = new CheckpointConfig();

        Configuration configuration = new Configuration();
        configuration.setString(spec.key, spec.value);
        configFromFile.configure(configuration);

        spec.setValue(configFromSetters);
        spec.assertEqual(configFromFile, configFromSetters);
    }

    @ParameterizedTest
    @MethodSource("specs")
    public void testNotOverridingIfNotSet(TestSpec<?> spec) {
        CheckpointConfig config = new CheckpointConfig();

        spec.setNonDefaultValue(config);
        Configuration configuration = new Configuration();
        config.configure(configuration);

        spec.assertEqualNonDefault(config);
    }

    private static class TestSpec<T> {
        private String key;
        private String value;
        private final T objectValue;
        private T nonDefaultValue;
        private BiConsumer<CheckpointConfig, T> setter;
        private Function<CheckpointConfig, T> getter;

        private BiConsumer<T, T> customAssertion =
                (actualValue, expectedValue) -> assertThat(actualValue).isEqualTo(expectedValue);

        private TestSpec(T value) {
            this.objectValue = value;
        }

        public static <T> TestSpec<T> testValue(T value) {
            return new TestSpec<>(value);
        }

        public TestSpec<T> whenSetFromFile(String key, String value) {
            this.key = key;
            this.value = value;
            return this;
        }

        public TestSpec<T> viaSetter(BiConsumer<CheckpointConfig, T> setter) {
            this.setter = setter;
            return this;
        }

        public TestSpec<T> getterVia(Function<CheckpointConfig, T> getter) {
            this.getter = getter;
            return this;
        }

        public TestSpec<T> nonDefaultValue(T nonDefaultValue) {
            this.nonDefaultValue = nonDefaultValue;
            return this;
        }

        public TestSpec<T> customMatcher(BiConsumer<T, T> customAssertion) {
            this.customAssertion = customAssertion;
            return this;
        }

        public void setValue(CheckpointConfig config) {
            setter.accept(config, objectValue);
        }

        public void setNonDefaultValue(CheckpointConfig config) {
            setter.accept(config, nonDefaultValue);
        }

        public void assertEqual(
                CheckpointConfig configFromFile, CheckpointConfig configFromSetters) {
            customAssertion.accept(getter.apply(configFromFile), getter.apply(configFromSetters));
        }

        public void assertEqualNonDefault(CheckpointConfig configFromFile) {
            customAssertion.accept(getter.apply(configFromFile), nonDefaultValue);
        }

        @Override
        public String toString() {
            return "key='" + key + '\'';
        }
    }
}
