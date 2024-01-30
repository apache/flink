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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.TernaryBoolean;

import com.esotericsoftware.kryo.Serializer;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * A config to define the behavior for serializers in Flink job, it manages the registered types and
 * serializers. The config is created from job configuration and used by Flink to create serializers
 * for data types.
 */
@PublicEvolving
public interface SerializerConfig extends Serializable {
    /**
     * Adds a new Kryo default serializer to the Runtime.
     *
     * <p>Note that the serializer instance must be serializable (as defined by
     * java.io.Serializable), because it may be distributed to the worker nodes by java
     * serialization.
     *
     * <p>The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializer The serializer to use.
     */
    @Internal
    <T extends Serializer<?> & Serializable> void addDefaultKryoSerializer(
            Class<?> type, T serializer);

    /**
     * Adds a new Kryo default serializer to the Runtime.
     *
     * <p>The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializerClass The class of the serializer to use.
     */
    @Internal
    void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass);

    /**
     * Registers the given type with a Kryo Serializer.
     *
     * <p>Note that the serializer instance must be serializable (as defined by
     * java.io.Serializable), because it may be distributed to the worker nodes by java
     * serialization.
     *
     * <p>The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializer The serializer to use.
     */
    @Internal
    <T extends Serializer<?> & Serializable> void registerTypeWithKryoSerializer(
            Class<?> type, T serializer);

    /**
     * Registers the given Serializer via its class as a serializer for the given type at the
     * KryoSerializer.
     *
     * <p>The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     *
     * @param type The class of the types serialized with the given serializer.
     * @param serializerClass The class of the serializer to use.
     */
    @Internal
    @SuppressWarnings("rawtypes")
    void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer> serializerClass);

    /**
     * Registers the given type with the serialization stack. If the type is eventually serialized
     * as a POJO, then the type is registered with the POJO serializer. If the type ends up being
     * serialized with Kryo, then it will be registered at Kryo to make sure that only tags are
     * written.
     *
     * <p>The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     *
     * @param type The class of the type to register.
     */
    @Internal
    void registerPojoType(Class<?> type);

    /**
     * Registers the given type with the serialization stack. If the type is eventually serialized
     * as a POJO, then the type is registered with the POJO serializer. If the type ends up being
     * serialized with Kryo, then it will be registered at Kryo to make sure that only tags are
     * written.
     *
     * <p>The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     *
     * @param type The class of the type to register.
     */
    @Internal
    void registerKryoType(Class<?> type);

    /**
     * Returns the registered types with Kryo Serializers.
     *
     * @deprecated The method is deprecated because instance-type Kryo serializer definition based
     *     on {@link ExecutionConfig.SerializableSerializer} is deprecated. Use class-type Kryo
     *     serializers instead.
     */
    @Deprecated
    LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
            getRegisteredTypesWithKryoSerializers();

    /** Returns the registered types with their Kryo Serializer classes. */
    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            getRegisteredTypesWithKryoSerializerClasses();

    /**
     * Returns the registered default Kryo Serializers.
     *
     * @deprecated The method is deprecated because {@link ExecutionConfig.SerializableSerializer}
     *     is deprecated.
     */
    @Deprecated
    LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> getDefaultKryoSerializers();

    /** Returns the registered default Kryo Serializer classes. */
    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> getDefaultKryoSerializerClasses();

    /** Returns the registered Kryo types. */
    LinkedHashSet<Class<?>> getRegisteredKryoTypes();

    /** Returns the registered POJO types. */
    LinkedHashSet<Class<?>> getRegisteredPojoTypes();

    /** Returns the registered type info factories. */
    Map<Class<?>, Class<? extends TypeInfoFactory<?>>> getRegisteredTypeInfoFactories();

    /**
     * Checks whether generic types are supported. Generic types are types that go through Kryo
     * during serialization.
     *
     * <p>Generic types are enabled by default.
     */
    boolean hasGenericTypesDisabled();

    /**
     * The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     */
    @Internal
    void setGenericTypes(boolean genericTypes);

    /** Returns whether Kryo is the serializer for POJOs. */
    boolean isForceKryoEnabled();

    /**
     * The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     */
    @Internal
    void setForceKryo(boolean forceKryo);

    /** Returns whether the Apache Avro is the serializer for POJOs. */
    boolean isForceAvroEnabled();

    /**
     * The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     */
    @Internal
    void setForceAvro(boolean forceAvro);

    /**
     * The method will be converted to private in the next Flink major version after removing its
     * deprecated caller methods.
     */
    @Internal
    public void setForceKryoAvro(boolean forceKryoAvro);

    /** Returns whether forces Flink to register Apache Avro classes in Kryo serializer. */
    TernaryBoolean isForceKryoAvroEnabled();

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
    void configure(ReadableConfig configuration, ClassLoader classLoader);
}
