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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for state descriptors. A {@code StateDescriptor} is used for creating partitioned
 * State in stateful operations internally.
 *
 * @param <T> The type of the value of the state object described by this state descriptor.
 */
@Internal
public abstract class StateDescriptor<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** An enumeration of the types of supported states. */
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
    @Nonnull private final TypeSerializer<T> typeSerializer;

    /** The configuration of state time-to-live(TTL), it is disabled by default. */
    @Nonnull private StateTtlConfig ttlConfig = StateTtlConfig.DISABLED;

    // ------------------------------------------------------------------------

    /**
     * Create a new {@code StateDescriptor} with the given stateId and the given type information.
     *
     * @param stateId The stateId of the {@code StateDescriptor}.
     * @param typeInfo The type information for the values in the state.
     */
    protected StateDescriptor(@Nonnull String stateId, TypeInformation<T> typeInfo) {
        this(stateId, typeInfo, new SerializerConfigImpl());
    }

    /**
     * Create a new {@code StateDescriptor} with the given stateId and the given type information.
     *
     * @param stateId The stateId of the {@code StateDescriptor}.
     * @param typeInfo The type information for the values in the state.
     * @param serializerConfig The serializer related config used to generate {@code
     *     TypeSerializer}.
     */
    protected StateDescriptor(
            @Nonnull String stateId,
            @Nonnull TypeInformation<T> typeInfo,
            SerializerConfig serializerConfig) {
        this.stateId = checkNotNull(stateId, "stateId must not be null");
        checkNotNull(typeInfo, "type information must not be null");
        this.typeSerializer = typeInfo.createSerializer(serializerConfig);
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
        return typeSerializer.duplicate();
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
    public abstract Type getType();
}
