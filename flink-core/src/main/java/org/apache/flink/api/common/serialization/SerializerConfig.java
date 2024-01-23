/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A config to define the behavior for serializers in Flink job, it manages the registered types and
 * serializers. The config is created from job configuration and used by Flink to create serializers
 * for data types.
 */
@PublicEvolving
public final class SerializerConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Configuration configuration;

    // ------------------------------- User code values --------------------------------------------

    // Serializers and types registered with Kryo and the PojoSerializer
    // we store them in linked maps/sets to ensure they are registered in order in all kryo
    // instances.

    private LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
            registeredTypesWithKryoSerializers = new LinkedHashMap<>();

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            registeredTypesWithKryoSerializerClasses = new LinkedHashMap<>();

    private LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
            defaultKryoSerializers = new LinkedHashMap<>();

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses =
            new LinkedHashMap<>();

    private LinkedHashSet<Class<?>> registeredKryoTypes = new LinkedHashSet<>();

    private LinkedHashSet<Class<?>> registeredPojoTypes = new LinkedHashSet<>();

    private LinkedHashMap<Class<?>, Class<TypeInfoFactory<?>>> registeredTypeFactories =
            new LinkedHashMap<>();

    // --------------------------------------------------------------------------------------------

    public SerializerConfig() {
        this(new Configuration());
    }

    @Internal
    public SerializerConfig(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Adds a new Kryo default serializer to the Runtime.
     *
     * <p>Note that the serializer instance must be serializable (as defined by
     * java.io.Serializable), because it may be distributed to the worker nodes by java
     * serialization.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializer The serializer to use.
     */
    public <T extends Serializer<?> & Serializable> void addDefaultKryoSerializer(
            Class<?> type, T serializer) {
        if (type == null || serializer == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        defaultKryoSerializers.put(type, new ExecutionConfig.SerializableSerializer<>(serializer));
    }

    /**
     * Adds a new Kryo default serializer to the Runtime.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializerClass The class of the serializer to use.
     */
    public void addDefaultKryoSerializer(
            Class<?> type, Class<? extends Serializer<?>> serializerClass) {
        if (type == null || serializerClass == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }
        defaultKryoSerializerClasses.put(type, serializerClass);
    }

    /**
     * Registers the given type with a Kryo Serializer.
     *
     * <p>Note that the serializer instance must be serializable (as defined by
     * java.io.Serializable), because it may be distributed to the worker nodes by java
     * serialization.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializer The serializer to use.
     */
    public <T extends Serializer<?> & Serializable> void registerTypeWithKryoSerializer(
            Class<?> type, T serializer) {
        if (type == null || serializer == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        registeredTypesWithKryoSerializers.put(
                type, new ExecutionConfig.SerializableSerializer<>(serializer));
    }

    /**
     * Registers the given Serializer via its class as a serializer for the given type at the
     * KryoSerializer.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializerClass The class of the serializer to use.
     */
    @SuppressWarnings("rawtypes")
    public void registerTypeWithKryoSerializer(
            Class<?> type, Class<? extends Serializer> serializerClass) {
        if (type == null || serializerClass == null) {
            throw new NullPointerException("Cannot register null class or serializer.");
        }

        @SuppressWarnings("unchecked")
        Class<? extends Serializer<?>> castedSerializerClass =
                (Class<? extends Serializer<?>>) serializerClass;
        registeredTypesWithKryoSerializerClasses.put(type, castedSerializerClass);
    }

    /**
     * Registers the given type with the serialization stack. If the type is eventually serialized
     * as a POJO, then the type is registered with the POJO serializer. If the type ends up being
     * serialized with Kryo, then it will be registered at Kryo to make sure that only tags are
     * written.
     *
     * @param type The class of the type to register.
     */
    public void registerPojoType(Class<?> type) {
        if (type == null) {
            throw new NullPointerException("Cannot register null type class.");
        }
        registeredPojoTypes.add(type);
    }

    /**
     * Registers the given type with the serialization stack. If the type is eventually serialized
     * as a POJO, then the type is registered with the POJO serializer. If the type ends up being
     * serialized with Kryo, then it will be registered at Kryo to make sure that only tags are
     * written.
     *
     * @param type The class of the type to register.
     */
    public void registerKryoType(Class<?> type) {
        if (type == null) {
            throw new NullPointerException("Cannot register null type class.");
        }
        registeredKryoTypes.add(type);
    }

    /** Returns the registered types with Kryo Serializers. */
    public LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
            getRegisteredTypesWithKryoSerializers() {
        return registeredTypesWithKryoSerializers;
    }

    /** Returns the registered types with their Kryo Serializer classes. */
    public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            getRegisteredTypesWithKryoSerializerClasses() {
        return registeredTypesWithKryoSerializerClasses;
    }

    /** Returns the registered default Kryo Serializers. */
    public LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
            getDefaultKryoSerializers() {
        return defaultKryoSerializers;
    }

    /** Returns the registered default Kryo Serializer classes. */
    public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            getDefaultKryoSerializerClasses() {
        return defaultKryoSerializerClasses;
    }

    /** Returns the registered Kryo types. */
    public LinkedHashSet<Class<?>> getRegisteredKryoTypes() {
        if (isForceKryoEnabled()) {
            // if we force kryo, we must also return all the types that
            // were previously only registered as POJO
            LinkedHashSet<Class<?>> result = new LinkedHashSet<>(registeredKryoTypes);
            result.addAll(registeredPojoTypes);
            return result;
        } else {
            return registeredKryoTypes;
        }
    }

    /** Returns the registered POJO types. */
    public LinkedHashSet<Class<?>> getRegisteredPojoTypes() {
        return registeredPojoTypes;
    }

    /** Returns the registered type info factories. */
    public LinkedHashMap<Class<?>, Class<TypeInfoFactory<?>>> getRegisteredTypeFactories() {
        return registeredTypeFactories;
    }

    /**
     * Checks whether generic types are supported. Generic types are types that go through Kryo
     * during serialization.
     *
     * <p>Generic types are enabled by default.
     */
    public boolean hasGenericTypesDisabled() {
        return !configuration.get(PipelineOptions.GENERIC_TYPES);
    }

    public void setGenericTypes(boolean genericTypes) {
        configuration.set(PipelineOptions.GENERIC_TYPES, genericTypes);
    }

    /** Returns whether Kryo is the serializer for POJOs. */
    public boolean isForceKryoEnabled() {
        return configuration.get(PipelineOptions.FORCE_KRYO);
    }

    public void setForceKryo(boolean forceKryo) {
        configuration.set(PipelineOptions.FORCE_KRYO, forceKryo);
    }

    /** Returns whether the Apache Avro is the serializer for POJOs. */
    public boolean isForceAvroEnabled() {
        return configuration.get(PipelineOptions.FORCE_AVRO);
    }

    public void setForceAvro(boolean forceAvro) {
        configuration.set(PipelineOptions.FORCE_AVRO, forceAvro);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SerializerConfig) {
            SerializerConfig other = (SerializerConfig) obj;

            return Objects.equals(configuration, other.configuration)
                    && registeredTypesWithKryoSerializers.equals(
                            other.registeredTypesWithKryoSerializers)
                    && registeredTypesWithKryoSerializerClasses.equals(
                            other.registeredTypesWithKryoSerializerClasses)
                    && defaultKryoSerializers.equals(other.defaultKryoSerializers)
                    && defaultKryoSerializerClasses.equals(other.defaultKryoSerializerClasses)
                    && registeredKryoTypes.equals(other.registeredKryoTypes)
                    && registeredPojoTypes.equals(other.registeredPojoTypes)
                    && registeredTypeFactories.equals(other.registeredTypeFactories);

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                configuration,
                registeredTypesWithKryoSerializers,
                registeredTypesWithKryoSerializerClasses,
                defaultKryoSerializers,
                defaultKryoSerializerClasses,
                registeredKryoTypes,
                registeredPojoTypes,
                registeredTypeFactories);
    }

    @Override
    public String toString() {
        return "SerializerConfig{"
                + "configuration="
                + configuration
                + ", registeredTypesWithKryoSerializers="
                + registeredTypesWithKryoSerializers
                + ", registeredTypesWithKryoSerializerClasses="
                + registeredTypesWithKryoSerializerClasses
                + ", defaultKryoSerializers="
                + defaultKryoSerializers
                + ", defaultKryoSerializerClasses="
                + defaultKryoSerializerClasses
                + ", registeredKryoTypes="
                + registeredKryoTypes
                + ", registeredPojoTypes="
                + registeredPojoTypes
                + ", registeredTypeFactories="
                + registeredTypeFactories
                + '}';
    }

    // ------------------------------ Utilities  ----------------------------------

    /**
     * Sets all relevant options contained in the {@link ReadableConfig} such as e.g. {@link
     * PipelineOptions#FORCE_KRYO}.
     *
     * <p>It will change the value of a setting only if a corresponding option was set in the {@code
     * configuration}. If a key is not present, the current value of a field will remain untouched.
     *
     * @param configuration a configuration to read the values from
     * @param classLoader a class loader to use when loading classes
     */
    public void configure(ReadableConfig configuration, ClassLoader classLoader) {
        configuration.getOptional(PipelineOptions.GENERIC_TYPES).ifPresent(this::setGenericTypes);
        configuration.getOptional(PipelineOptions.FORCE_KRYO).ifPresent(this::setForceKryo);
        configuration.getOptional(PipelineOptions.FORCE_AVRO).ifPresent(this::setForceAvro);

        configuration
                .getOptional(PipelineOptions.KRYO_DEFAULT_SERIALIZERS)
                .map(s -> parseKryoSerializersWithExceptionHandling(classLoader, s))
                .ifPresent(s -> this.defaultKryoSerializerClasses = s);

        configuration
                .getOptional(PipelineOptions.POJO_REGISTERED_CLASSES)
                .map(c -> loadClasses(c, classLoader, "Could not load pojo type to be registered."))
                .ifPresent(c -> this.registeredPojoTypes = c);

        configuration
                .getOptional(PipelineOptions.KRYO_REGISTERED_CLASSES)
                .map(c -> loadClasses(c, classLoader, "Could not load kryo type to be registered."))
                .ifPresent(c -> this.registeredKryoTypes = c);
    }

    private LinkedHashSet<Class<?>> loadClasses(
            List<String> classNames, ClassLoader classLoader, String errorMessage) {
        return classNames.stream()
                .map(name -> this.<Class<?>>loadClass(name, classLoader, errorMessage))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            parseKryoSerializersWithExceptionHandling(
                    ClassLoader classLoader, List<String> kryoSerializers) {
        try {
            return parseKryoSerializers(classLoader, kryoSerializers);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not configure kryo serializers from %s. The expected format is:"
                                    + "'class:<fully qualified class name>,serializer:<fully qualified serializer name>;...",
                            kryoSerializers),
                    e);
        }
    }

    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> parseKryoSerializers(
            ClassLoader classLoader, List<String> kryoSerializers) {
        return kryoSerializers.stream()
                .map(ConfigurationUtils::parseStringToMap)
                .collect(
                        Collectors.toMap(
                                m ->
                                        loadClass(
                                                m.get("class"),
                                                classLoader,
                                                "Could not load class for kryo serialization"),
                                m ->
                                        loadClass(
                                                m.get("serializer"),
                                                classLoader,
                                                "Could not load serializer's class"),
                                (m1, m2) -> {
                                    throw new IllegalArgumentException(
                                            "Duplicated serializer for class: " + m1);
                                },
                                LinkedHashMap::new));
    }

    @SuppressWarnings("unchecked")
    private <T extends Class> T loadClass(
            String className, ClassLoader classLoader, String errorMessage) {
        try {
            return (T) Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(errorMessage, e);
        }
    }
}
