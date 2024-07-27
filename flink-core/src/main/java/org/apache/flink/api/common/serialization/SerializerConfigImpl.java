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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TernaryBoolean;

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** The default implement of {@link SerializerConfig}. */
@Internal
public final class SerializerConfigImpl implements SerializerConfig {

    private static final long serialVersionUID = 1L;

    private final Configuration configuration;

    /**
     * Note: This field is used only for compatibility while {@link
     * org.apache.flink.api.common.typeinfo.TypeInformation#createSerializer(ExecutionConfig)} is
     * deprecated; If it is removed, this field will also be removed.
     */
    private final ExecutionConfig executionConfig;

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

    // Order is not required as we will traverse the type hierarchy up to find the closest type
    // information factory when extracting the type information.
    private Map<Class<?>, Class<? extends TypeInfoFactory<?>>> registeredTypeInfoFactories =
            new HashMap<>();

    // --------------------------------------------------------------------------------------------

    public SerializerConfigImpl() {
        Configuration conf = new Configuration();
        this.configuration = conf;
        this.executionConfig = new ExecutionConfig(conf);
    }

    @Internal
    public SerializerConfigImpl(Configuration configuration, ExecutionConfig executionConfig) {
        this.configuration = configuration;
        this.executionConfig = executionConfig;
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
    public Map<Class<?>, Class<? extends TypeInfoFactory<?>>> getRegisteredTypeInfoFactories() {
        return registeredTypeInfoFactories;
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
    public void setForceKryoAvro(boolean forceKryoAvro) {
        configuration.set(PipelineOptions.FORCE_KRYO_AVRO, forceKryoAvro);
    }

    @Override
    public TernaryBoolean isForceKryoAvroEnabled() {
        return configuration
                .getOptional(PipelineOptions.FORCE_KRYO_AVRO)
                .map(TernaryBoolean::fromBoolean)
                .orElse(TernaryBoolean.UNDEFINED);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SerializerConfigImpl) {
            SerializerConfigImpl other = (SerializerConfigImpl) obj;

            return Objects.equals(configuration, other.configuration)
                    && registeredTypesWithKryoSerializers.equals(
                            other.registeredTypesWithKryoSerializers)
                    && registeredTypesWithKryoSerializerClasses.equals(
                            other.registeredTypesWithKryoSerializerClasses)
                    && defaultKryoSerializers.equals(other.defaultKryoSerializers)
                    && defaultKryoSerializerClasses.equals(other.defaultKryoSerializerClasses)
                    && registeredKryoTypes.equals(other.registeredKryoTypes)
                    && registeredPojoTypes.equals(other.registeredPojoTypes)
                    && registeredTypeInfoFactories.equals(other.registeredTypeInfoFactories);

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
                registeredTypeInfoFactories);
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
                + registeredTypeInfoFactories
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
                .getOptional(PipelineOptions.FORCE_KRYO_AVRO)
                .ifPresent(this::setForceKryoAvro);

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

        try {
            configuration
                    .getOptional(PipelineOptions.SERIALIZATION_CONFIG)
                    .ifPresent(c -> parseSerializationConfigWithExceptionHandling(classLoader, c));
        } catch (Exception e) {
            if (!GlobalConfiguration.isStandardYaml()) {
                throw new UnsupportedOperationException(
                        String.format(
                                "%s is only supported with the standard YAML config parser, please use \"config.yaml\" as the config file.",
                                PipelineOptions.SERIALIZATION_CONFIG.key()));
            }
            throw e;
        }
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

    private void parseSerializationConfigWithExceptionHandling(
            ClassLoader classLoader, List<String> serializationConfigs) {
        try {
            parseSerializationConfig(classLoader, serializationConfigs);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Could not configure serializers from %s.", serializationConfigs),
                    e);
        }
    }

    private void parseSerializationConfig(
            ClassLoader classLoader, List<String> serializationConfigs) {
        final LinkedHashMap<Class<?>, Map<String, String>> serializationConfigByClass =
                serializationConfigs.stream()
                        .map(ConfigurationUtils::parseStringToMap)
                        .flatMap(m -> m.entrySet().stream())
                        .collect(
                                Collectors.toMap(
                                        e ->
                                                loadClass(
                                                        e.getKey(),
                                                        classLoader,
                                                        "Could not load class for serialization config"),
                                        e -> ConfigurationUtils.parseStringToMap(e.getValue()),
                                        (v1, v2) -> {
                                            throw new IllegalArgumentException(
                                                    "Duplicated serializer for the same class.");
                                        },
                                        LinkedHashMap::new));
        for (Map.Entry<Class<?>, Map<String, String>> entry :
                serializationConfigByClass.entrySet()) {
            Class<?> type = entry.getKey();
            Map<String, String> config = entry.getValue();
            String configType = config.get("type");
            if (configType == null) {
                throw new IllegalArgumentException("Serializer type not specified for " + type);
            }
            switch (configType) {
                case "pojo":
                    registerPojoType(type);
                    break;
                case "kryo":
                    parseAndRegisterKryoType(classLoader, type, config);
                    break;
                case "typeinfo":
                    parseAndRegisterTypeFactory(classLoader, type, config);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unsupported serializer type %s for %s", configType, type));
            }
        }
    }

    private void parseAndRegisterKryoType(
            ClassLoader classLoader, Class<?> t, Map<String, String> m) {
        String kryoType = m.get("kryo-type");
        if (kryoType == null) {
            registerKryoType(t);
        } else {
            switch (kryoType) {
                case "default":
                    addDefaultKryoSerializer(
                            t,
                            loadClass(
                                    m.get("class"),
                                    classLoader,
                                    "Could not load serializer's class"));
                    break;
                case "registered":
                    registerTypeWithKryoSerializer(
                            t,
                            loadClass(
                                    m.get("class"),
                                    classLoader,
                                    "Could not load serializer's class"));
                    break;
                default:
                    break;
            }
        }
    }

    private void parseAndRegisterTypeFactory(
            ClassLoader classLoader, Class<?> t, Map<String, String> m) {
        Class<? extends TypeInfoFactory<?>> factoryClass =
                loadClass(m.get("class"), classLoader, "Could not load TypeInfoFactory's class");
        // Register in the global static factory map of TypeExtractor for now so that it can be
        // accessed from the static methods of TypeExtractor where SerializerConfig is currently
        // not accessible
        TypeExtractor.registerFactory(t, factoryClass);
        // Register inside SerializerConfig only for testing purpose for now
        registerTypeWithTypeInfoFactory(t, factoryClass);
    }

    private void registerTypeWithTypeInfoFactory(
            Class<?> t, Class<? extends TypeInfoFactory<?>> factory) {
        Preconditions.checkNotNull(t, "Type parameter must not be null.");
        Preconditions.checkNotNull(factory, "Factory parameter must not be null.");

        if (!TypeInfoFactory.class.isAssignableFrom(factory)) {
            throw new IllegalArgumentException("Class is not a TypeInfoFactory.");
        }
        if (registeredTypeInfoFactories.containsKey(t)) {
            throw new InvalidTypesException(
                    "A TypeInfoFactory for type '" + t + "' is already registered.");
        }
        registeredTypeInfoFactories.put(t, factory);
    }

    /**
     * Note: This method is used only for compatibility while {@link
     * org.apache.flink.api.common.typeinfo.TypeInformation#createSerializer(ExecutionConfig)} is
     * deprecated; If it is removed, this method will also be removed.
     */
    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public SerializerConfigImpl copy() {
        final SerializerConfigImpl newSerializerConfig = new SerializerConfigImpl();
        newSerializerConfig.configure(configuration, this.getClass().getClassLoader());

        getRegisteredTypesWithKryoSerializers()
                .forEach(
                        (c, s) ->
                                newSerializerConfig.registerTypeWithKryoSerializer(
                                        c, s.getSerializer()));
        getRegisteredTypesWithKryoSerializerClasses()
                .forEach(newSerializerConfig::registerTypeWithKryoSerializer);
        getDefaultKryoSerializers()
                .forEach(
                        (c, s) ->
                                newSerializerConfig.addDefaultKryoSerializer(c, s.getSerializer()));
        getDefaultKryoSerializerClasses().forEach(newSerializerConfig::addDefaultKryoSerializer);
        getRegisteredKryoTypes().forEach(newSerializerConfig::registerKryoType);
        getRegisteredPojoTypes().forEach(newSerializerConfig::registerPojoType);
        getRegisteredTypeInfoFactories()
                .forEach(newSerializerConfig::registerTypeWithTypeInfoFactory);

        return newSerializerConfig;
    }
}
