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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating partitioned
 * State in stateful operations internally.
 *
 * @param <T> The type of the value of the state object described by this state descriptor.
 */
@Experimental
public abstract class StateDescriptor<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** An enumeration of the types of supported states. */
    @Internal
    public enum Type {
        VALUE,
        LIST,
        REDUCING,
        AGGREGATING,
        MAP
    }

    /** ID that uniquely identifies state created from this StateDescriptor. */
    @Nonnull private final String stateId;

    /** The serializer for the type. */
    @Nonnull private final StateSerializerReference<T> typeSerializer;

    /** The configuration of state time-to-live(TTL), it is disabled by default. */
    @Nonnull private StateTtlConfig ttlConfig = StateTtlConfig.DISABLED;

    // ------------------------------------------------------------------------

    /**
     * Create a new {@code StateDescriptor} with the given stateId and the given type information.
     *
     * @param stateId The stateId of the {@code StateDescriptor}.
     * @param typeInfo The type information for the values in the state.
     */
    protected StateDescriptor(@Nonnull String stateId, @Nonnull TypeInformation<T> typeInfo) {
        this.stateId = checkNotNull(stateId, "state id must not be null");
        this.typeSerializer = new StateSerializerReference<>(typeInfo);
    }

    /**
     * Create a new {@code StateDescriptor} with the given stateId and the given type serializer.
     *
     * @param stateId The stateId of the {@code StateDescriptor}.
     * @param serializer The type serializer for the values in the state.
     */
    protected StateDescriptor(@Nonnull String stateId, TypeSerializer<T> serializer) {
        this.stateId = checkNotNull(stateId, "stateId must not be null");
        this.typeSerializer = new StateSerializerReference<>(serializer);
    }

    /**
     * Create a new {@code StateDescriptor} with the given name and the given type information.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #StateDescriptor(String, TypeInformation)} constructor.
     *
     * @param name The name of the {@code StateDescriptor}.
     * @param type The class of the type of values in the state.
     */
    protected StateDescriptor(String name, Class<T> type) {
        this.stateId = checkNotNull(name, "name must not be null");
        checkNotNull(type, "type class must not be null");
        this.typeSerializer = new StateSerializerReference<>(type);
    }

    // ------------------------------------------------------------------------

    /**
     * Configures optional activation of state time-to-live (TTL).
     *
     * <p>State user value will expire, become unavailable and be cleaned up in storage depending on
     * configured {@link StateTtlConfig}.
     *
     * @param ttlConfig configuration of state TTL
     */
    public void enableTimeToLive(StateTtlConfig ttlConfig) {
        this.ttlConfig = checkNotNull(ttlConfig);
    }

    @Nonnull
    public StateTtlConfig getTtlConfig() {
        return ttlConfig;
    }

    @Nonnull
    public String getStateId() {
        return stateId;
    }

    @Nonnull
    public TypeSerializer<T> getSerializer() {
        TypeSerializer<T> serializer = typeSerializer.get();
        if (serializer != null) {
            return serializer.duplicate();
        } else {
            throw new IllegalStateException("Serializer not yet initialized.");
        }
    }

    @Internal
    @Nullable
    public TypeInformation<T> getTypeInformation() {
        return typeSerializer.getTypeInformation();
    }

    // ------------------------------------------------------------------------

    /**
     * Checks whether the serializer has been initialized. Serializer initialization is lazy, to
     * allow parametrization of serializers with an {@link ExecutionConfig} via {@link
     * #initializeSerializerUnlessSet(ExecutionConfig)}.
     *
     * @return True if the serializers have been initialized, false otherwise.
     */
    public boolean isSerializerInitialized() {
        return typeSerializer.isInitialized();
    }

    /**
     * Initializes the serializer, unless it has been initialized before.
     *
     * @param executionConfig The execution config to use when creating the serializer.
     */
    public void initializeSerializerUnlessSet(ExecutionConfig executionConfig) {
        typeSerializer.initializeUnlessSet(executionConfig);
    }

    @Internal
    public void initializeSerializerUnlessSet(SerializerFactory serializerFactory) {
        typeSerializer.initializeUnlessSet(serializerFactory);
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return stateId.hashCode() + 31 * getClass().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o != null && o.getClass() == this.getClass()) {
            final StateDescriptor<?> that = (StateDescriptor<?>) o;
            return this.stateId.equals(that.stateId);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "{stateId="
                + stateId
                + ", typeSerializer="
                + typeSerializer
                + ", ttlConfig="
                + ttlConfig
                + '}';
    }

    /** Return the specific {@code Type} of described state. */
    @Internal
    public abstract Type getType();
}
