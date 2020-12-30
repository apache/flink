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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.util.Map;

/**
 * A {@link StateDescriptor} for {@link MapState}. This can be used to create state where the type
 * is a map that can be updated and iterated over.
 *
 * <p>Using {@code MapState} is typically more efficient than manually maintaining a map in a {@link
 * ValueState}, because the backing implementation can support efficient updates, rather then
 * replacing the full map on write.
 *
 * <p>To create keyed map state (on a KeyedStream), use {@link
 * org.apache.flink.api.common.functions.RuntimeContext#getMapState(MapStateDescriptor)}.
 *
 * <p>Note: The map state with TTL currently supports {@code null} user values only if the user
 * value serializer can handle {@code null} values. If the serializer does not support {@code null}
 * values, it can be wrapped with {@link
 * org.apache.flink.api.java.typeutils.runtime.NullableSerializer} at the cost of an extra byte in
 * the serialized form.
 *
 * @param <UK> The type of the keys that can be added to the map state.
 */
@PublicEvolving
public class MapStateDescriptor<UK, UV> extends StateDescriptor<MapState<UK, UV>, Map<UK, UV>> {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new {@code MapStateDescriptor} with the given name and the given type serializers.
     *
     * @param name The name of the {@code MapStateDescriptor}.
     * @param keySerializer The type serializer for the keys in the state.
     * @param valueSerializer The type serializer for the values in the state.
     */
    public MapStateDescriptor(
            String name, TypeSerializer<UK> keySerializer, TypeSerializer<UV> valueSerializer) {
        super(name, new MapSerializer<>(keySerializer, valueSerializer), null);
    }

    /**
     * Create a new {@code MapStateDescriptor} with the given name and the given type information.
     *
     * @param name The name of the {@code MapStateDescriptor}.
     * @param keyTypeInfo The type information for the keys in the state.
     * @param valueTypeInfo The type information for the values in the state.
     */
    public MapStateDescriptor(
            String name, TypeInformation<UK> keyTypeInfo, TypeInformation<UV> valueTypeInfo) {
        super(name, new MapTypeInfo<>(keyTypeInfo, valueTypeInfo), null);
    }

    /**
     * Create a new {@code MapStateDescriptor} with the given name and the given type information.
     *
     * <p>If this constructor fails (because it is not possible to describe the type via a class),
     * consider using the {@link #MapStateDescriptor(String, TypeInformation, TypeInformation)}
     * constructor.
     *
     * @param name The name of the {@code MapStateDescriptor}.
     * @param keyClass The class of the type of keys in the state.
     * @param valueClass The class of the type of values in the state.
     */
    public MapStateDescriptor(String name, Class<UK> keyClass, Class<UV> valueClass) {
        super(name, new MapTypeInfo<>(keyClass, valueClass), null);
    }

    @Override
    public Type getType() {
        return Type.MAP;
    }

    /**
     * Gets the serializer for the keys in the state.
     *
     * @return The serializer for the keys in the state.
     */
    public TypeSerializer<UK> getKeySerializer() {
        final TypeSerializer<Map<UK, UV>> rawSerializer = getSerializer();
        if (!(rawSerializer instanceof MapSerializer)) {
            throw new IllegalStateException("Unexpected serializer type.");
        }

        return ((MapSerializer<UK, UV>) rawSerializer).getKeySerializer();
    }

    /**
     * Gets the serializer for the values in the state.
     *
     * @return The serializer for the values in the state.
     */
    public TypeSerializer<UV> getValueSerializer() {
        final TypeSerializer<Map<UK, UV>> rawSerializer = getSerializer();
        if (!(rawSerializer instanceof MapSerializer)) {
            throw new IllegalStateException("Unexpected serializer type.");
        }

        return ((MapSerializer<UK, UV>) rawSerializer).getValueSerializer();
    }
}
