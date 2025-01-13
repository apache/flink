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

package org.apache.flink.api.common.state.v2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link StateDescriptor} for {@link MapState}. This can be used to create partitioned map state
 * internally.
 *
 * @param <UK> The type of the user key for this map state.
 * @param <UV> The type of the values that the map state can hold.
 */
@Experimental
public class MapStateDescriptor<UK, UV> extends StateDescriptor<UV> {

    /** The serializer for the user key. */
    @Nonnull private final StateSerializerReference<UK> userKeySerializer;

    /**
     * Creates a new {@code MapStateDescriptor} with the given stateId and type.
     *
     * @param stateId The (unique) stateId for the state.
     * @param userKeyTypeInfo The type of the user keys in the state.
     * @param userValueTypeInfo The type of the values in the state.
     */
    public MapStateDescriptor(
            @Nonnull String stateId,
            @Nonnull TypeInformation<UK> userKeyTypeInfo,
            @Nonnull TypeInformation<UV> userValueTypeInfo) {
        super(stateId, userValueTypeInfo);
        this.userKeySerializer = new StateSerializerReference<>(userKeyTypeInfo);
    }

    /**
     * Create a new {@code MapStateDescriptor} with the given stateId and the given type serializer.
     *
     * @param stateId The (unique) stateId for the state.
     * @param userKeySerializer The serializer for the user keys in the state.
     * @param userValueSerializer The serializer for the user values in the state.
     */
    public MapStateDescriptor(
            @Nonnull String stateId,
            @Nonnull TypeSerializer<UK> userKeySerializer,
            @Nonnull TypeSerializer<UV> userValueSerializer) {
        super(stateId, userValueSerializer);
        this.userKeySerializer = new StateSerializerReference<>(userKeySerializer);
    }

    @Nonnull
    public TypeSerializer<UK> getUserKeySerializer() {
        TypeSerializer<UK> serializer = userKeySerializer.get();
        if (serializer != null) {
            return serializer.duplicate();
        } else {
            throw new IllegalStateException("Serializer not yet initialized.");
        }
    }

    @Internal
    @Nullable
    public TypeInformation<UK> getUserKeyTypeInformation() {
        return userKeySerializer.getTypeInformation();
    }

    /**
     * Checks whether the serializer has been initialized. Serializer initialization is lazy, to
     * allow parametrization of serializers with an {@link ExecutionConfig} via {@link
     * #initializeSerializerUnlessSet(ExecutionConfig)}.
     *
     * @return True if the serializers have been initialized, false otherwise.
     */
    @Override
    public boolean isSerializerInitialized() {
        return super.isSerializerInitialized() && userKeySerializer.isInitialized();
    }

    /**
     * Initializes the serializer, unless it has been initialized before.
     *
     * @param executionConfig The execution config to use when creating the serializer.
     */
    @Override
    public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
        super.initializeSerializerUnlessSet(executionConfig);
        userKeySerializer.initializeUnlessSet(executionConfig);
    }

    @Override
    public void initializeSerializerUnlessSet(SerializerFactory serializerFactory) {
        super.initializeSerializerUnlessSet(serializerFactory);
        userKeySerializer.initializeUnlessSet(serializerFactory);
    }

    @Override
    public Type getType() {
        return Type.MAP;
    }
}
