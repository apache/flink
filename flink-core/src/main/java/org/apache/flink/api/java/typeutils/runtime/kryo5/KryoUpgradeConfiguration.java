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

package org.apache.flink.api.java.typeutils.runtime.kryo5;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.AvroUtils;
import org.apache.flink.api.java.typeutils.runtime.Kryo5Registration;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;

import com.esotericsoftware.kryo.kryo5.Serializer;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Existing Kryo 2 serialized state contains Kryo 2 serializer information. The upgrade process
 * requires Kryo 5 analogs to the Kryo 2 serializers. This uses a JVM global variable to store this
 * information which isn't ideal. Ideally, this would be passed as a function argument, but that
 * would be a giant refactor. A second based approach would be to use something like ScopedValues
 * previewing in Java 21. Also, when support for Kryo 2 state is dropped, this can be removed
 * entirely.
 */
@PublicEvolving
public class KryoUpgradeConfiguration {
    @PublicEvolving
    public static KryoUpgradeConfiguration getActive() {
        return active;
    }

    @PublicEvolving
    public static void clearConfiguration() {
        active = EMPTY;
    }

    /**
     * Set a JVM global variable with Kryo 5 serializers required when deserializing Kryo 2 state.
     *
     * @param executionConfig The configuration containing the Kryo 5 serializers to use in the Kryo
     *     upgrade migration process.
     */
    @PublicEvolving
    public static void setConfigurationFromExecutionConfig(ExecutionConfig executionConfig) {
        setConfiguration(
                executionConfig.getDefaultKryo5Serializers(),
                executionConfig.getDefaultKryo5SerializerClasses(),
                buildKryoRegistrations(
                        executionConfig.getRegisteredKryo5Types(),
                        executionConfig.getRegisteredTypesWithKryo5SerializerClasses(),
                        executionConfig.getRegisteredTypesWithKryo5Serializers()));
    }

    @Internal
    @SuppressWarnings("deprecation")
    public LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
            upgradeDefaultSerializers(
                    LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
                            defaultKryo2Serializers) {
        LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>> upgraded =
                new LinkedHashMap<>();
        for (Class<?> c : defaultKryo2Serializers.keySet()) {
            ExecutionConfig.SerializableKryo5Serializer<?> upgradedSerializer =
                    this.defaultSerializers.get(c);
            if (upgradedSerializer == null) {
                throw new IllegalArgumentException(
                        String.format("Missing default serializer instance for %s", c.getName()));
            }
            upgraded.put(c, upgradedSerializer);
        }
        return upgraded;
    }

    @Internal
    public LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
            upgradeDefaultSerializerClasses(
                    LinkedHashMap<
                                    Class<?>,
                                    Class<? extends com.esotericsoftware.kryo.Serializer<?>>>
                            defaultKryo2SerializerClasses) {
        LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
                upgraded = new LinkedHashMap<>();
        for (Class<?> c : defaultKryo2SerializerClasses.keySet()) {
            Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>> upgradedSerializerClass =
                    this.defaultSerializerClasses.get(c);
            if (upgradedSerializerClass == null) {
                throw new IllegalArgumentException(
                        String.format("Missing default serializer class for %s", c.getName()));
            }
            upgraded.put(c, upgradedSerializerClass);
        }
        return upgraded;
    }

    @Internal
    public LinkedHashMap<String, Kryo5Registration> upgradeRegistrations(
            LinkedHashMap<String, KryoRegistration> kryo2Serializers) {
        LinkedHashMap<String, Kryo5Registration> upgraded = new LinkedHashMap<>();
        for (Map.Entry<String, KryoRegistration> entry : kryo2Serializers.entrySet()) {
            String className = entry.getKey();
            KryoRegistration existing = entry.getValue();

            Kryo5Registration upgradedRegistration = this.registrations.get(className);
            if (upgradedRegistration != null) {
                upgraded.put(className, upgradedRegistration);
            } else if (existing.getSerializerDefinitionType()
                    == KryoRegistration.SerializerDefinitionType.UNSPECIFIED) {
                Kryo5Registration unspecifiedRegistration =
                        new Kryo5Registration(existing.getRegisteredClass());
                upgraded.put(className, unspecifiedRegistration);
            } else if (className.equals("org.apache.avro.generic.GenericData$Array")) {
                AvroUtils.getAvroUtils().addAvroGenericDataArrayRegistration5(upgraded);
            } else {
                throw new IllegalArgumentException(
                        String.format("Missing registration serializer class for %s", className));
            }
        }
        return upgraded;
    }

    @Internal
    static void setConfiguration(
            LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                    defaultSerializers,
            LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
                    defaultSerializerClasses,
            LinkedHashMap<String, Kryo5Registration> registrations) {
        active =
                new KryoUpgradeConfiguration(
                        defaultSerializers, defaultSerializerClasses, registrations);
    }

    private static LinkedHashMap<String, Kryo5Registration> buildKryoRegistrations(
            LinkedHashSet<Class<?>> registeredTypes,
            LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
                    registeredTypesWithSerializerClasses,
            LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                    registeredTypesWithSerializers) {

        final LinkedHashMap<String, Kryo5Registration> kryoRegistrations = new LinkedHashMap<>();

        for (Class<?> registeredType : checkNotNull(registeredTypes)) {
            kryoRegistrations.put(registeredType.getName(), new Kryo5Registration(registeredType));
        }

        for (Map.Entry<Class<?>, Class<? extends Serializer<?>>>
                registeredTypeWithSerializerClassEntry :
                        checkNotNull(registeredTypesWithSerializerClasses).entrySet()) {

            kryoRegistrations.put(
                    registeredTypeWithSerializerClassEntry.getKey().getName(),
                    new Kryo5Registration(
                            registeredTypeWithSerializerClassEntry.getKey(),
                            registeredTypeWithSerializerClassEntry.getValue()));
        }

        for (Map.Entry<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                registeredTypeWithSerializerEntry :
                        checkNotNull(registeredTypesWithSerializers).entrySet()) {

            kryoRegistrations.put(
                    registeredTypeWithSerializerEntry.getKey().getName(),
                    new Kryo5Registration(
                            registeredTypeWithSerializerEntry.getKey(),
                            registeredTypeWithSerializerEntry.getValue()));
        }

        // add Avro support if flink-avro is available; a dummy otherwise
        AvroUtils.getAvroUtils().addAvroGenericDataArrayRegistration5(kryoRegistrations);

        return kryoRegistrations;
    }

    static final KryoUpgradeConfiguration EMPTY =
            new KryoUpgradeConfiguration(
                    new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedHashMap<>());
    static KryoUpgradeConfiguration active = EMPTY;

    KryoUpgradeConfiguration(
            LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                    defaultSerializers,
            LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
                    defaultSerializerClasses,
            LinkedHashMap<String, Kryo5Registration> registrations) {
        this.defaultSerializers = defaultSerializers;
        this.defaultSerializerClasses = defaultSerializerClasses;
        this.registrations = registrations;
    }

    final LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
            defaultSerializers;
    final LinkedHashMap<Class<?>, Class<? extends com.esotericsoftware.kryo.kryo5.Serializer<?>>>
            defaultSerializerClasses;
    final LinkedHashMap<String, Kryo5Registration> registrations;
}
