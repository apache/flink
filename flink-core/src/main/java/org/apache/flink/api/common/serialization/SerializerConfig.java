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

import org.apache.flink.annotation.PublicEvolving;
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
    /** Returns the registered types with their Kryo Serializer classes. */
    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            getRegisteredTypesWithKryoSerializerClasses();

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

    /** Returns whether Kryo is the serializer for POJOs. */
    boolean isForceKryoEnabled();

    /** Returns whether the Apache Avro is the serializer for POJOs. */
    boolean isForceAvroEnabled();

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

    SerializerConfig copy();
}
