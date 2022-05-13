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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionConfigTest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.TimeCharacteristic;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for configuring {@link StreamExecutionEnvironment} via {@link
 * StreamExecutionEnvironment#configure(ReadableConfig, ClassLoader)}.
 *
 * @see StreamExecutionEnvironmentComplexConfigurationTest
 */
@RunWith(Parameterized.class)
public class StreamExecutionEnvironmentConfigurationTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestSpec> specs() {
        return Arrays.asList(
                TestSpec.testValue(TimeCharacteristic.IngestionTime)
                        .whenSetFromFile("pipeline.time-characteristic", "IngestionTime")
                        .viaSetter(StreamExecutionEnvironment::setStreamTimeCharacteristic)
                        .getterVia(StreamExecutionEnvironment::getStreamTimeCharacteristic)
                        .nonDefaultValue(TimeCharacteristic.EventTime),
                TestSpec.testValue(60000L)
                        .whenSetFromFile("execution.buffer-timeout", "1 min")
                        .viaSetter(StreamExecutionEnvironment::setBufferTimeout)
                        .getterVia(StreamExecutionEnvironment::getBufferTimeout)
                        .nonDefaultValue(12000L),
                TestSpec.testValue(false)
                        .whenSetFromFile("pipeline.operator-chaining", "false")
                        .viaSetter(
                                (env, b) -> {
                                    if (b) {
                                        throw new IllegalArgumentException(
                                                "Cannot programmatically enable operator chaining");
                                    } else {
                                        env.disableOperatorChaining();
                                    }
                                })
                        .getterVia(StreamExecutionEnvironment::isChainingEnabled)
                        .nonDefaultValue(false),
                TestSpec.testValue(ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL)
                        .whenSetFromFile("pipeline.closure-cleaner-level", "TOP_LEVEL")
                        .viaSetter((env, v) -> env.getConfig().setClosureCleanerLevel(v))
                        .getterVia(env -> env.getConfig().getClosureCleanerLevel())
                        .nonDefaultValue(ExecutionConfig.ClosureCleanerLevel.NONE),
                TestSpec.testValue(12000L)
                        .whenSetFromFile("execution.checkpointing.timeout", "12 s")
                        .viaSetter((env, v) -> env.getCheckpointConfig().setCheckpointTimeout(v))
                        .getterVia(env -> env.getCheckpointConfig().getCheckpointTimeout())
                        .nonDefaultValue(100L));
    }

    @Parameterized.Parameter public TestSpec spec;

    @Test
    public void testLoadingFromConfiguration() {
        StreamExecutionEnvironment configFromSetters =
                StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment configFromFile =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration configuration = new Configuration();
        configuration.setString(spec.key, spec.value);
        configFromFile.configure(configuration, ExecutionConfigTest.class.getClassLoader());

        spec.setValue(configFromSetters);
        spec.assertEqual(configFromFile, configFromSetters);
    }

    @Test
    public void testNotOverridingIfNotSet() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        spec.setNonDefaultValue(environment);
        Configuration configuration = new Configuration();
        environment.configure(configuration, ExecutionConfigTest.class.getClassLoader());

        spec.assertEqualNonDefault(environment);
    }

    private static class TestSpec<T> {
        private String key;
        private String value;
        private final T objectValue;
        private T nonDefaultValue;
        private BiConsumer<StreamExecutionEnvironment, T> setter;
        private Function<StreamExecutionEnvironment, T> getter;

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

        public TestSpec<T> viaSetter(BiConsumer<StreamExecutionEnvironment, T> setter) {
            this.setter = setter;
            return this;
        }

        public TestSpec<T> getterVia(Function<StreamExecutionEnvironment, T> getter) {
            this.getter = getter;
            return this;
        }

        public TestSpec<T> nonDefaultValue(T nonDefaultValue) {
            this.nonDefaultValue = nonDefaultValue;
            return this;
        }

        public void setValue(StreamExecutionEnvironment config) {
            setter.accept(config, objectValue);
        }

        public void setNonDefaultValue(StreamExecutionEnvironment config) {
            setter.accept(config, nonDefaultValue);
        }

        public void assertEqual(
                StreamExecutionEnvironment configFromFile,
                StreamExecutionEnvironment configFromSetters) {
            assertThat(getter.apply(configFromFile), equalTo(getter.apply(configFromSetters)));
        }

        public void assertEqualNonDefault(StreamExecutionEnvironment configFromFile) {
            assertThat(getter.apply(configFromFile), equalTo(nonDefaultValue));
        }

        @Override
        public String toString() {
            return "key='" + key + '\'';
        }
    }
}
