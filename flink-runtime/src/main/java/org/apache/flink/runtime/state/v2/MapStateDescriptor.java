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

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import javax.annotation.Nonnull;

/**
 * {@link StateDescriptor} for {@link MapState}. This can be used to create partitioned map state
 * internally.
 *
 * @param <UK> The type of the user key for this map state.
 * @param <UV> The type of the values that the map state can hold.
 */
public class MapStateDescriptor<UK, UV> extends StateDescriptor<UV> {

    /** The serializer for the user key. */
    @Nonnull private final TypeSerializer<UK> userKeySerializer;

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
        this(stateId, userKeyTypeInfo, userValueTypeInfo, new SerializerConfigImpl());
    }

    /**
     * Creates a new {@code MapStateDescriptor} with the given stateId and type.
     *
     * @param stateId The (unique) stateId for the state.
     * @param userKeyTypeInfo The type of the user keys in the state.
     * @param userValueTypeInfo The type of the values in the state.
     * @param serializerConfig The serializer related config used to generate {@code
     *     TypeSerializer}.
     */
    public MapStateDescriptor(
            @Nonnull String stateId,
            @Nonnull TypeInformation<UK> userKeyTypeInfo,
            @Nonnull TypeInformation<UV> userValueTypeInfo,
            SerializerConfig serializerConfig) {
        super(stateId, userValueTypeInfo, serializerConfig);
        this.userKeySerializer = userKeyTypeInfo.createSerializer(serializerConfig);
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
        this.userKeySerializer = userKeySerializer;
    }

    @Nonnull
    public TypeSerializer<UK> getUserKeySerializer() {
        return userKeySerializer.duplicate();
    }

    @Override
    public Type getType() {
        return Type.MAP;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final boolean equals(Object o) {
        return super.equals(o)
                && userKeySerializer.equals(((MapStateDescriptor<UK, UV>) o).userKeySerializer);
    }
}
