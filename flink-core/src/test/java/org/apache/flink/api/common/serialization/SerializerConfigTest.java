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

package org.apache.flink.api.common.serialization;

import org.apache.flink.configuration.Configuration;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SerializerConfigTest {

    @Test
    void testReadingDefaultConfig() {
        SerializerConfig config = new SerializerConfig();
        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, SerializerConfigTest.class.getClassLoader());

        assertThat(config).isEqualTo(new SerializerConfig());
    }

    @Test
    void testDoubleTypeRegistration() {
        SerializerConfig config = new SerializerConfig();
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
    void testLoadingRegisteredKryoTypesFromConfiguration() {
        SerializerConfig configFromSetters = new SerializerConfig();
        configFromSetters.registerKryoType(SerializerConfigTest.class);
        configFromSetters.registerKryoType(SerializerConfigTest.TestSerializer1.class);

        SerializerConfig configFromConfiguration = new SerializerConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.registered-kryo-types",
                "org.apache.flink.api.common.serialization.SerializerConfigTest;"
                        + "org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer1");

        // mutate config according to configuration
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration.getRegisteredKryoTypes())
                .isEqualTo(configFromSetters.getRegisteredKryoTypes());
    }

    @Test
    void testLoadingRegisteredPojoTypesFromConfiguration() {
        SerializerConfig configFromSetters = new SerializerConfig();
        configFromSetters.registerPojoType(SerializerConfigTest.class);
        configFromSetters.registerPojoType(SerializerConfigTest.TestSerializer1.class);

        SerializerConfig configFromConfiguration = new SerializerConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.registered-pojo-types",
                "org.apache.flink.api.common.serialization.SerializerConfigTest;"
                        + "org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer1");

        // mutate config according to configuration
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration.getRegisteredPojoTypes())
                .isEqualTo(configFromSetters.getRegisteredPojoTypes());
    }

    @Test
    void testLoadingDefaultKryoSerializersFromConfiguration() {
        SerializerConfig configFromSetters = new SerializerConfig();
        configFromSetters.addDefaultKryoSerializer(
                SerializerConfigTest.class, SerializerConfigTest.TestSerializer1.class);
        configFromSetters.addDefaultKryoSerializer(
                SerializerConfigTest.TestSerializer1.class,
                SerializerConfigTest.TestSerializer2.class);

        SerializerConfig configFromConfiguration = new SerializerConfig();

        Configuration configuration = new Configuration();
        configuration.setString(
                "pipeline.default-kryo-serializers",
                "class:org.apache.flink.api.common.serialization.SerializerConfigTest,"
                        + "serializer:org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer1;"
                        + "class:org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer1,"
                        + "serializer:org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer2");

        // mutate config according to configuration
        configFromConfiguration.configure(
                configuration, Thread.currentThread().getContextClassLoader());

        assertThat(configFromConfiguration.getDefaultKryoSerializers())
                .isEqualTo(configFromSetters.getDefaultKryoSerializers());
    }

    @Test
    void testNotOverridingRegisteredKryoTypesWithDefaultsFromConfiguration() {
        SerializerConfig config = new SerializerConfig();
        config.registerKryoType(SerializerConfigTest.class);
        config.registerKryoType(SerializerConfigTest.TestSerializer1.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashSet<Object> set = new LinkedHashSet<>();
        set.add(SerializerConfigTest.class);
        set.add(SerializerConfigTest.TestSerializer1.class);
        assertThat(config.getRegisteredKryoTypes()).isEqualTo(set);
    }

    @Test
    void testNotOverridingRegisteredPojoTypesWithDefaultsFromConfiguration() {
        SerializerConfig config = new SerializerConfig();
        config.registerPojoType(SerializerConfigTest.class);
        config.registerPojoType(SerializerConfigTest.TestSerializer1.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashSet<Object> set = new LinkedHashSet<>();
        set.add(SerializerConfigTest.class);
        set.add(SerializerConfigTest.TestSerializer1.class);
        assertThat(config.getRegisteredPojoTypes()).isEqualTo(set);
    }

    @Test
    void testNotOverridingDefaultKryoSerializersFromConfiguration() {
        SerializerConfig config = new SerializerConfig();
        config.addDefaultKryoSerializer(
                SerializerConfigTest.class, SerializerConfigTest.TestSerializer1.class);
        config.addDefaultKryoSerializer(
                SerializerConfigTest.TestSerializer1.class,
                SerializerConfigTest.TestSerializer2.class);

        Configuration configuration = new Configuration();

        // mutate config according to configuration
        config.configure(configuration, Thread.currentThread().getContextClassLoader());

        LinkedHashMap<Class<?>, Class<? extends Serializer>> serializers = new LinkedHashMap<>();
        serializers.put(SerializerConfigTest.class, SerializerConfigTest.TestSerializer1.class);
        serializers.put(
                SerializerConfigTest.TestSerializer1.class,
                SerializerConfigTest.TestSerializer2.class);
        assertThat(config.getDefaultKryoSerializerClasses()).isEqualTo(serializers);
    }

    private static class TestSerializer1 extends Serializer<SerializerConfigTest>
            implements Serializable {
        @Override
        public void write(Kryo kryo, Output output, SerializerConfigTest object) {}

        @Override
        public SerializerConfigTest read(Kryo kryo, Input input, Class<SerializerConfigTest> type) {
            return null;
        }
    }

    private static class TestSerializer2 extends Serializer<SerializerConfigTest.TestSerializer1>
            implements Serializable {
        @Override
        public void write(Kryo kryo, Output output, SerializerConfigTest.TestSerializer1 object) {}

        @Override
        public SerializerConfigTest.TestSerializer1 read(
                Kryo kryo, Input input, Class<SerializerConfigTest.TestSerializer1> type) {
            return null;
        }
    }
}
