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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.CheckpointingMode;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
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
                TestSpec.testValue(org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE)
                        .whenSetFromFile(Map.of("execution.checkpointing.mode", "AT_LEAST_ONCE"))
                        .viaSetter(CheckpointConfig::setCheckpointingMode)
                        .getterVia(CheckpointConfig::getCheckpointingMode)
                        .nonDefaultValue(
                                org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE),
                TestSpec.testValue(CheckpointingMode.AT_LEAST_ONCE)
                        .whenSetFromFile(Map.of("execution.checkpointing.mode", "AT_LEAST_ONCE"))
                        .viaSetter(CheckpointConfig::setCheckpointingConsistencyMode)
                        .getterVia(CheckpointConfig::getCheckpointingConsistencyMode)
                        .nonDefaultValue(CheckpointingMode.AT_LEAST_ONCE),
                TestSpec.testValue(CheckpointingMode.AT_LEAST_ONCE)
                        .whenSetFromFile(Map.of("execution.checkpointing.mode", "AT_LEAST_ONCE"))
                        .viaSetter(
                                (config, v) -> {
                                    config.setCheckpointingMode(
                                            org.apache.flink.streaming.api.CheckpointingMode
                                                    .valueOf(v.name()));
                                })
                        .getterVia(CheckpointConfig::getCheckpointingConsistencyMode)
                        .nonDefaultValue(CheckpointingMode.AT_LEAST_ONCE),
                TestSpec.testValue(org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE)
                        .whenSetFromFile(Map.of("execution.checkpointing.mode", "AT_LEAST_ONCE"))
                        .viaSetter(
                                (config, v) -> {
                                    config.setCheckpointingConsistencyMode(
                                            CheckpointingMode.valueOf(v.name()));
                                })
                        .getterVia(CheckpointConfig::getCheckpointingMode)
                        .nonDefaultValue(
                                org.apache.flink.streaming.api.CheckpointingMode.AT_LEAST_ONCE),
                TestSpec.testValue(10000L)
                        .whenSetFromFile(Map.of("execution.checkpointing.interval", "10 s"))
                        .viaSetter(CheckpointConfig::setCheckpointInterval)
                        .getterVia(CheckpointConfig::getCheckpointInterval)
                        .nonDefaultValue(100L),
                TestSpec.testValue(12000L)
                        .whenSetFromFile(Map.of("execution.checkpointing.timeout", "12 s"))
                        .viaSetter(CheckpointConfig::setCheckpointTimeout)
                        .getterVia(CheckpointConfig::getCheckpointTimeout)
                        .nonDefaultValue(100L),
                TestSpec.testValue(12)
                        .whenSetFromFile(
                                Map.of("execution.checkpointing.max-concurrent-checkpoints", "12"))
                        .viaSetter(CheckpointConfig::setMaxConcurrentCheckpoints)
                        .getterVia(CheckpointConfig::getMaxConcurrentCheckpoints)
                        .nonDefaultValue(100),
                TestSpec.testValue(1000L)
                        .whenSetFromFile(Map.of("execution.checkpointing.min-pause", "1 s"))
                        .viaSetter(CheckpointConfig::setMinPauseBetweenCheckpoints)
                        .getterVia(CheckpointConfig::getMinPauseBetweenCheckpoints)
                        .nonDefaultValue(100L),
                TestSpec.testValue(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
                        .whenSetFromFile(
                                Map.of(
                                        "execution.checkpointing.externalized-checkpoint-retention",
                                        "RETAIN_ON_CANCELLATION"))
                        .viaSetter(CheckpointConfig::setExternalizedCheckpointRetention)
                        .getterVia(CheckpointConfig::getExternalizedCheckpointRetention)
                        .nonDefaultValue(ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION),
                TestSpec.testValue(12)
                        .whenSetFromFile(
                                Map.of(
                                        "execution.checkpointing.tolerable-failed-checkpoints",
                                        "12"))
                        .viaSetter(CheckpointConfig::setTolerableCheckpointFailureNumber)
                        .getterVia(CheckpointConfig::getTolerableCheckpointFailureNumber)
                        .nonDefaultValue(100),
                TestSpec.testValue(true)
                        .whenSetFromFile(
                                Map.of(
                                        "execution.checkpointing.interval", "10s",
                                        "execution.checkpointing.unaligned.enabled", "true"))
                        .viaSetter(
                                (checkpointConfig, enabled) -> {
                                    checkpointConfig.setCheckpointInterval(10000);
                                    checkpointConfig.enableUnalignedCheckpoints(enabled);
                                })
                        .getterVia(CheckpointConfig::isUnalignedCheckpointsEnabled)
                        .nonDefaultValue(true),
                TestSpec.testValue(true)
                        .whenSetFromFile(
                                Map.of(
                                        "execution.checkpointing.interval",
                                        "10s",
                                        "execution.checkpointing.unaligned.enabled",
                                        "true",
                                        "execution.checkpointing.unaligned.interruptible-timers.enabled",
                                        "true"))
                        .viaSetter(
                                (checkpointConfig, enabled) -> {
                                    checkpointConfig.setCheckpointInterval(10000);
                                    checkpointConfig.enableUnalignedCheckpoints(enabled);
                                    checkpointConfig.enableUnalignedCheckpointsInterruptibleTimers(
                                            enabled);
                                })
                        .getterVia(
                                CheckpointConfig::isUnalignedCheckpointsInterruptibleTimersEnabled)
                        .nonDefaultValue(true));
    }

    @ParameterizedTest
    @MethodSource("specs")
    public void testLoadingFromConfiguration(TestSpec<?> spec) {
        CheckpointConfig configFromSetters = new CheckpointConfig();
        CheckpointConfig configFromFile = new CheckpointConfig();

        Configuration configuration = new Configuration();
        spec.configs.forEach(configuration::setString);
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
        private Map<String, String> configs;
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

        public TestSpec<T> whenSetFromFile(Map<String, String> configs) {
            this.configs = configs;
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
            return "configs='" + configs + '\'';
        }
    }
}
