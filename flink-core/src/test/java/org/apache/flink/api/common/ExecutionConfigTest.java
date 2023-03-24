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

package org.apache.flink.api.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.SerializedValue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExecutionConfigTest {

    @Test
    void testDoubleTypeRegistration() {
        ExecutionConfig config = new ExecutionConfig();
        List<Class<?>> types = Arrays.<Class<?>>asList(Double.class, Integer.class, Double.class);
        List<Class<?>> expectedTypes = Arrays.<Class<?>>asList(Double.class, Integer.class);

        for (Class<?> tpe : types) {
            config.registerKryoType(tpe);
        }

        int counter = 0;

        for (Class<?> tpe : config.getRegisteredKryoTypes()) {
            assertThat(tpe).isEqualTo(expectedTypes.get(counter++));
        }

        assertThat(expectedTypes.size()).isEqualTo(counter);
    }

    @Test
    void testConfigurationOfParallelism() {
        ExecutionConfig config = new ExecutionConfig();

        // verify explicit change in parallelism
        int parallelism = 36;
        config.setParallelism(parallelism);

        assertThat(parallelism).isEqualTo(config.getParallelism());

        // verify that parallelism is reset to default flag value
        parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
        config.setParallelism(parallelism);

        assertThat(parallelism).isEqualTo(config.getParallelism());
    }

    @Test
    void testDisableGenericTypes() {
        ExecutionConfig conf = new ExecutionConfig();
        TypeInformation<Object> typeInfo = new GenericTypeInfo<Object>(Object.class);

        // by default, generic types are supported
        TypeSerializer<Object> serializer = typeInfo.createSerializer(conf);
        assertThat(serializer instanceof KryoSerializer).isTrue();

        // expect an exception when generic types are disabled
        conf.disableGenericTypes();
        assertThatThrownBy(
                        () -> typeInfo.createSerializer(conf),
                        "should have failed with an exception")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
        final Random r = new Random();

        final int parallelism = 1 + r.nextInt(10);
        final boolean closureCleanerEnabled = r.nextBoolean(),
                forceAvroEnabled = r.nextBoolean(),
                forceKryoEnabled = r.nextBoolean(),
                disableGenericTypes = r.nextBoolean(),
                objectReuseEnabled = r.nextBoolean();

        final ExecutionConfig config = new ExecutionConfig();

        if (closureCleanerEnabled) {
            config.enableClosureCleaner();
        } else {
            config.disableClosureCleaner();
        }
        if (forceAvroEnabled) {
            config.enableForceAvro();
        } else {
            config.disableForceAvro();
        }
        if (forceKryoEnabled) {
            config.enableForceKryo();
        } else {
            config.disableForceKryo();
        }
        if (disableGenericTypes) {
            config.disableGenericTypes();
        } else {
            config.enableGenericTypes();
        }
        if (objectReuseEnabled) {
            config.enableObjectReuse();
        } else {
            config.disableObjectReuse();
        }
        config.setParallelism(parallelism);

        final ExecutionConfig copy1 = CommonTestUtils.createCopySerializable(config);
        final ExecutionConfig copy2 =
                new SerializedValue<>(config).deserializeValue(getClass().getClassLoader());

        assertThat(copy1).isNotNull();
        assertThat(copy2).isNotNull();

        assertThat(config).isEqualTo(copy1);
        assertThat(config).isEqualTo(copy2);

        assertThat(closureCleanerEnabled).isEqualTo(copy1.isClosureCleanerEnabled());
        assertThat(forceAvroEnabled).isEqualTo(copy1.isForceAvroEnabled());
        assertThat(forceKryoEnabled).isEqualTo(copy1.isForceKryoEnabled());
        assertThat(disableGenericTypes).isEqualTo(copy1.hasGenericTypesDisabled());
        assertThat(objectReuseEnabled).isEqualTo(copy1.isObjectReuseEnabled());
        assertThat(parallelism).isEqualTo(copy1.getParallelism());
    }

    @Test
    void testGlobalParametersNotNull() {
        final ExecutionConfig config = new ExecutionConfig();

        assertThat(config.getGlobalJobParameters()).isNotNull();
    }

    @Test
    void testGlobalParametersHashCode() {
        ExecutionConfig config = new ExecutionConfig();
        ExecutionConfig anotherConfig = new ExecutionConfig();

        assertThat(config.getGlobalJobParameters().hashCode())
                .isEqualTo(anotherConfig.getGlobalJobParameters().hashCode());
    }

    @Test
    void testReadingDefaultConfig() {
        ExecutionConfig executionConfig = new ExecutionConfig();
        Configuration configuration = new Configuration();

        // mutate config according to configuration
        executionConfig.configure(configuration, ExecutionConfigTest.class.getClassLoader());

        assertThat(executionConfig).isEqualTo(new ExecutionConfig());
    }

    @Test
    void testLoadingRegisteredKryoTypesFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.registerKryoType(ExecutionConfigTest.class);
        configFromSetters.registerKryoType(TestSerializer1.class);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.registered-kryo-types",
                "org.apache.flink.api.common.ExecutionConfigTest;"
                        + "org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1");

        // mutate config according to configuration
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration).isEqualTo(configFromSetters);
    }

    @Test
    void testLoadingRegisteredPojoTypesFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.registerPojoType(ExecutionConfigTest.class);
        configFromSetters.registerPojoType(TestSerializer1.class);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.registered-pojo-types",
                "org.apache.flink.api.common.ExecutionConfigTest;"
                        + "org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1");

        // mutate config according to configuration
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration).isEqualTo(configFromSetters);
    }

    @Test
    void testLoadingRestartStrategyFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, Time.minutes(2)));

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString("restart-strategy", "fixeddelay");
        configuration.setString("restart-strategy.fixed-delay.attempts", "10");
        configuration.setString("restart-strategy.fixed-delay.delay", "2 min");

        // mutate config according to configuration
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration).isEqualTo(configFromSetters);
    }

    @Test
    void testLoadingDefaultKryoSerializersFromConfiguration() {
        ExecutionConfig configFromSetters = new ExecutionConfig();
        configFromSetters.addDefaultKryoSerializer(
                ExecutionConfigTest.class, TestSerializer1.class);
        configFromSetters.addDefaultKryoSerializer(TestSerializer1.class, TestSerializer2.class);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.default-kryo-serializers",
                "class:org.apache.flink.api.common.ExecutionConfigTest,"
                        + "serializer:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1;"
                        + "class:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer1,"
                        + "serializer:org.apache.flink.api.common.ExecutionConfigTest$TestSerializer2");

        // mutate config according to configuration
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration).isEqualTo(configFromSetters);
    }

    @Test
    public void testLoadingSchedulerTypeFromConfiguration() {
        testLoadingSchedulerTypeFromConfiguration(JobManagerOptions.SchedulerType.AdaptiveBatch);
        testLoadingSchedulerTypeFromConfiguration(JobManagerOptions.SchedulerType.Default);
        testLoadingSchedulerTypeFromConfiguration(JobManagerOptions.SchedulerType.Adaptive);
    }

    private void testLoadingSchedulerTypeFromConfiguration(
            JobManagerOptions.SchedulerType schedulerType) {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, schedulerType);

        ExecutionConfig configFromConfiguration = new ExecutionConfig();
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration.getSchedulerType().get()).isEqualTo(schedulerType);
    }

    @Test
    void testNotOverridingRegisteredKryoTypesWithDefaultsFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        config.registerKryoType(ExecutionConfigTest.class);
        config.registerKryoType(TestSerializer1.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashSet<Object> set = new LinkedHashSet<>();
        set.add(ExecutionConfigTest.class);
        set.add(TestSerializer1.class);
        assertThat(config.getRegisteredKryoTypes()).isEqualTo(set);
    }

    @Test
    void testNotOverridingRegisteredPojoTypesWithDefaultsFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        config.registerPojoType(ExecutionConfigTest.class);
        config.registerPojoType(TestSerializer1.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashSet<Object> set = new LinkedHashSet<>();
        set.add(ExecutionConfigTest.class);
        set.add(TestSerializer1.class);
        assertThat(config.getRegisteredPojoTypes()).isEqualTo(set);
    }

    @Test
    void testNotOverridingRestartStrategiesWithDefaultsFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
                RestartStrategies.fixedDelayRestart(10, Time.minutes(2));
        config.setRestartStrategy(restartStrategyConfiguration);

        // mutate config according to configuration
        config.configure(new Configuration(), Thread.currentThread().getContextClassLoader());

        assertThat(config.getRestartStrategy()).isEqualTo(restartStrategyConfiguration);
    }

    @Test
    void testNotOverridingDefaultKryoSerializersFromConfiguration() {
        ExecutionConfig config = new ExecutionConfig();
        config.addDefaultKryoSerializer(ExecutionConfigTest.class, TestSerializer1.class);
        config.addDefaultKryoSerializer(TestSerializer1.class, TestSerializer2.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashMap<Class<?>, Class<? extends Serializer>> serialiers = new LinkedHashMap<>();
        serialiers.put(ExecutionConfigTest.class, TestSerializer1.class);
        serialiers.put(TestSerializer1.class, TestSerializer2.class);
        assertThat(config.getDefaultKryoSerializerClasses()).isEqualTo(serialiers);
    }

    private static class TestSerializer1 extends Serializer<ExecutionConfigTest>
            implements Serializable {
        @Override
        public void write(Kryo kryo, Output output, ExecutionConfigTest object) {}

        @Override
        public ExecutionConfigTest read(Kryo kryo, Input input, Class<ExecutionConfigTest> type) {
            return null;
        }
    }

    private static class TestSerializer2 extends Serializer<TestSerializer1>
            implements Serializable {
        @Override
        public void write(Kryo kryo, Output output, TestSerializer1 object) {}

        @Override
        public TestSerializer1 read(Kryo kryo, Input input, Class<TestSerializer1> type) {
            return null;
        }
    }
}
