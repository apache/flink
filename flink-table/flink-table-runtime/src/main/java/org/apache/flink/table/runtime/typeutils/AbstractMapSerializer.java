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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Map;

/**
 * Base serializer for {@link Map}. The serializer relies on a key serializer and a value serializer
 * for the serialization of the map's key-value pairs.
 *
 * <p>The serialization format for the map is as follows: four bytes for the length of the map,
 * followed by the serialized representation of each key-value pair. To allow null values, each
 * value is prefixed by a null flag.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 * @param <M> The type of the map.
 */
abstract class AbstractMapSerializer<K, V, M extends Map<K, V>> extends TypeSerializer<M> {

    private static final long serialVersionUID = 1L;

    /** The serializer for the keys in the map. */
    final TypeSerializer<K> keySerializer;

    /** The serializer for the values in the map. */
    final TypeSerializer<V> valueSerializer;

    /**
     * Creates a map serializer that uses the given serializers to serialize the key-value pairs in
     * the map.
     *
     * @param keySerializer The serializer for the keys in the map
     * @param valueSerializer The serializer for the values in the map
     */
    AbstractMapSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
        Preconditions.checkNotNull(keySerializer, "The key serializer must not be null");
        Preconditions.checkNotNull(valueSerializer, "The value serializer must not be null.");
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    // ------------------------------------------------------------------------

    /**
     * Returns the serializer for the keys in the map.
     *
     * @return The serializer for the keys in the map.
     */
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    /**
     * Returns the serializer for the values in the map.
     *
     * @return The serializer for the values in the map.
     */
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
    public M copy(M from) {
        M newMap = createInstance();

        for (Map.Entry<K, V> entry : from.entrySet()) {
            K newKey = entry.getKey() == null ? null : keySerializer.copy(entry.getKey());
            V newValue = entry.getValue() == null ? null : valueSerializer.copy(entry.getValue());

            newMap.put(newKey, newValue);
        }

        return newMap;
    }

    @Override
    public M copy(M from, M reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    @Override
    public void serialize(M map, DataOutputView target) throws IOException {
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
    public M deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();

        final M map = createInstance();
        for (int i = 0; i < size; ++i) {
            K key = keySerializer.deserialize(source);

            boolean isNull = source.readBoolean();
            V value = isNull ? null : valueSerializer.deserialize(source);

            map.put(key, value);
        }

        return map;
    }

    @Override
    public M deserialize(M reuse, DataInputView source) throws IOException {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractMapSerializer<?, ?, ?> that = (AbstractMapSerializer<?, ?, ?>) o;

        return keySerializer.equals(that.keySerializer)
                && valueSerializer.equals(that.valueSerializer);
    }

    @Override
    public int hashCode() {
        int result = keySerializer.hashCode();
        result = 31 * result + valueSerializer.hashCode();
        return result;
    }
}
