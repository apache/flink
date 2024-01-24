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

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testLoadingPojoTypesFromSerializationConfig() {
        String serializationConfigStr =
                "{org.apache.flink.api.common.serialization.SerializerConfigTest:"
                        + " {type: pojo},"
                        + " org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer1:"
                        + " {type: pojo}}";
        SerializerConfig serializerConfig = getConfiguredSerializerConfig(serializationConfigStr);

        assertThat(serializerConfig.getRegisteredPojoTypes())
                .containsExactly(SerializerConfigTest.class, TestSerializer1.class);
    }

    @Test
    void testLoadingKryoTypesFromSerializationConfig() {
        String serializationConfigStr =
                "{org.apache.flink.api.common.serialization.SerializerConfigTest:"
                        + " {type: kryo},"
                        + " org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer1:"
                        + " {type: kryo, kryo-type: default, class: org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer2},"
                        + " org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer2:"
                        + " {type: kryo, kryo-type: registered, class: org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer3}}";
        SerializerConfig serializerConfig = getConfiguredSerializerConfig(serializationConfigStr);

        assertThat(serializerConfig.getRegisteredKryoTypes())
                .containsExactly(SerializerConfigTest.class);
        assertThat(serializerConfig.getDefaultKryoSerializerClasses())
                .containsExactly(
                        new AbstractMap.SimpleEntry<>(
                                TestSerializer1.class, TestSerializer2.class));
        assertThat(serializerConfig.getRegisteredTypesWithKryoSerializerClasses())
                .containsExactly(
                        new AbstractMap.SimpleEntry<>(
                                TestSerializer2.class, TestSerializer3.class));
    }

    @Test
    void testLoadingTypeInfoFactoriesFromSerializationConfig() {
        String serializationConfigStr =
                "{org.apache.flink.api.common.serialization.SerializerConfigTest:"
                        + " {type: typeinfo, class: org.apache.flink.api.common.serialization.SerializerConfigTest$TestTypeInfoFactory}}";
        SerializerConfig serializerConfig = getConfiguredSerializerConfig(serializationConfigStr);

        assertThat(serializerConfig.getRegisteredTypeInfoFactories())
                .containsExactly(
                        new AbstractMap.SimpleEntry<>(
                                SerializerConfigTest.class, TestTypeInfoFactory.class));
    }

    @Test
    void testLoadingSerializationConfigWithLegacyParser() {
        GlobalConfiguration.setStandardYaml(false);
        String serializationConfigStr =
                "{org.apache.flink.api.common.serialization.SerializerConfigTest:"
                        + " {type: pojo},"
                        + " org.apache.flink.api.common.serialization.SerializerConfigTest$TestSerializer1:"
                        + " {type: pojo}}";
        assertThatThrownBy(() -> getConfiguredSerializerConfig(serializationConfigStr))
                .isInstanceOf(UnsupportedOperationException.class);

        // Clear the standard yaml flag to avoid impact to other cases.
        GlobalConfiguration.setStandardYaml(true);
    }

    @Test
    void testLoadingIllegalSerializationConfig() {
        String unsupportedTypeConfigStr =
                "{org.apache.flink.api.common.serialization.SerializerConfigTest:"
                        + " {type: random}}";
        assertThatThrownBy(() -> getConfiguredSerializerConfig(unsupportedTypeConfigStr))
                .isInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Unsupported type: random");
    }

    private SerializerConfig getConfiguredSerializerConfig(String serializationConfigStr) {
        Configuration configuration = new Configuration();
        configuration.setString(PipelineOptions.SERIALIZATION_CONFIG.key(), serializationConfigStr);

        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.configure(configuration, Thread.currentThread().getContextClassLoader());
        return serializerConfig;
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

    private static class TestSerializer3 extends Serializer<SerializerConfigTest.TestSerializer2>
            implements Serializable {
        @Override
        public void write(Kryo kryo, Output output, SerializerConfigTest.TestSerializer2 object) {}

        @Override
        public SerializerConfigTest.TestSerializer2 read(
                Kryo kryo, Input input, Class<SerializerConfigTest.TestSerializer2> type) {
            return null;
        }
    }

    private static class TestTypeInfoFactory extends TypeInfoFactory<SerializerConfigTest> {
        @Override
        public TypeInformation<SerializerConfigTest> createTypeInfo(
                Type t, Map<String, TypeInformation<?>> genericParameters) {
            return null;
        }
    }
}
