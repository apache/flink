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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A serializer for {@link Map}. The serializer relies on a key serializer and a value serializer
 * for the serialization of the map's key-value pairs.
 *
 * <p>The serialization format for the map is as follows: four bytes for the length of the map,
 * followed by the serialized representation of each key-value pair. To allow null values, each
 * value is prefixed by a null marker.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 */
@Internal
public final class MapSerializer<K, V> extends TypeSerializer<Map<K, V>> {

    private static final long serialVersionUID = -6885593032367050078L;

    /** The serializer for the keys in the map */
    private final TypeSerializer<K> keySerializer;

    /** The serializer for the values in the map */
    private final TypeSerializer<V> valueSerializer;

    /**
     * Creates a map serializer that uses the given serializers to serialize the key-value pairs in
     * the map.
     *
     * @param keySerializer The serializer for the keys in the map
     * @param valueSerializer The serializer for the values in the map
     */
    public MapSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
        this.keySerializer =
                Preconditions.checkNotNull(keySerializer, "The key serializer cannot be null");
        this.valueSerializer =
                Preconditions.checkNotNull(valueSerializer, "The value serializer cannot be null.");
    }

    // ------------------------------------------------------------------------
    //  MapSerializer specific properties
    // ------------------------------------------------------------------------

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    // ------------------------------------------------------------------------
    //  Type Serializer implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Map<K, V>> duplicate() {
        TypeSerializer<K> duplicateKeySerializer = keySerializer.duplicate();
        TypeSerializer<V> duplicateValueSerializer = valueSerializer.duplicate();

        return (duplicateKeySerializer == keySerializer)
                        && (duplicateValueSerializer == valueSerializer)
                ? this
                : new MapSerializer<>(duplicateKeySerializer, duplicateValueSerializer);
    }

    @Override
    public Map<K, V> createInstance() {
        return new HashMap<>();
    }

    @Override
    public Map<K, V> copy(Map<K, V> from) {
        Map<K, V> newMap = new HashMap<>(from.size());

        for (Map.Entry<K, V> entry : from.entrySet()) {
            K newKey = keySerializer.copy(entry.getKey());
            V newValue = entry.getValue() == null ? null : valueSerializer.copy(entry.getValue());

            newMap.put(newKey, newValue);
        }

        return newMap;
    }

    @Override
    public Map<K, V> copy(Map<K, V> from, Map<K, V> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    @Override
    public void serialize(Map<K, V> map, DataOutputView target) throws IOException {
        final int size = map.size();
        target.writeInt(size);

        for (Map.Entry<K, V> entry : map.entrySet()) {
            keySerializer.serialize(entry.getKey(), target);

            if (entry.getValue() == null) {
                target.writeBoolean(true);
            } else {
                target.writeBoolean(false);
                valueSerializer.serialize(entry.getValue(), target);
            }
        }
    }

    @Override
    public Map<K, V> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();

        final Map<K, V> map = new HashMap<>(size);
        for (int i = 0; i < size; ++i) {
            K key = keySerializer.deserialize(source);

            boolean isNull = source.readBoolean();
            V value = isNull ? null : valueSerializer.deserialize(source);

            map.put(key, value);
        }

        return map;
    }

    @Override
    public Map<K, V> deserialize(Map<K, V> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final int size = source.readInt();
        target.writeInt(size);

        for (int i = 0; i < size; ++i) {
            keySerializer.copy(source, target);

            boolean isNull = source.readBoolean();
            target.writeBoolean(isNull);

            if (!isNull) {
                valueSerializer.copy(source, target);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && keySerializer.equals(((MapSerializer<?, ?>) obj).getKeySerializer())
                        && valueSerializer.equals(
                                ((MapSerializer<?, ?>) obj).getValueSerializer()));
    }

    @Override
    public int hashCode() {
        return keySerializer.hashCode() * 31 + valueSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<Map<K, V>> snapshotConfiguration() {
        return new MapSerializerSnapshot<>(this);
    }
}
