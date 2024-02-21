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

package org.apache.flink.table.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CollectionUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link NullAwareMapSerializer} is similar to MapSerializer, the only difference is that the
 * {@link NullAwareMapSerializer} can handle null keys.
 */
@Internal
@Deprecated
public class NullAwareMapSerializer<K, V> extends TypeSerializer<Map<K, V>> {
    private static final long serialVersionUID = 5363147328373166590L;

    /** The serializer for the keys in the map. */
    private final TypeSerializer<K> keySerializer;

    /** The serializer for the values in the map. */
    private final TypeSerializer<V> valueSerializer;

    public NullAwareMapSerializer(
            TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Map<K, V>> duplicate() {
        return new NullAwareMapSerializer<>(keySerializer.duplicate(), valueSerializer.duplicate());
    }

    @Override
    public Map<K, V> createInstance() {
        return new HashMap<>();
    }

    @Override
    public Map<K, V> copy(Map<K, V> from) {
        Map<K, V> newMap = CollectionUtil.newHashMapWithExpectedSize(from.size());

        for (Map.Entry<K, V> entry : from.entrySet()) {
            K newKey = entry.getKey() == null ? null : keySerializer.copy(entry.getKey());
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
        return -1;
    }

    @Override
    public void serialize(Map<K, V> map, DataOutputView target) throws IOException {
        final int size = map.size();
        target.writeInt(size);

        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (entry.getKey() == null) {
                target.writeBoolean(true);
            } else {
                target.writeBoolean(false);
                keySerializer.serialize(entry.getKey(), target);
            }

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

        final Map<K, V> map = CollectionUtil.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; ++i) {
            boolean keyIsNull = source.readBoolean();
            K key = keyIsNull ? null : keySerializer.deserialize(source);

            boolean valueIsNull = source.readBoolean();
            V value = valueIsNull ? null : valueSerializer.deserialize(source);

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
            boolean keyIsNull = source.readBoolean();
            target.writeBoolean(keyIsNull);
            if (!keyIsNull) {
                keySerializer.copy(source, target);
            }

            boolean valueIsNull = source.readBoolean();
            target.writeBoolean(valueIsNull);

            if (!valueIsNull) {
                valueSerializer.copy(source, target);
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && keySerializer.equals(
                                ((NullAwareMapSerializer<?, ?>) obj).getKeySerializer())
                        && valueSerializer.equals(
                                ((NullAwareMapSerializer<?, ?>) obj).getValueSerializer()));
    }

    @Override
    public int hashCode() {
        return keySerializer.hashCode() * 31 + valueSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Map<K, V>> snapshotConfiguration() {
        return new NullAwareMapSerializerSnapshot<>(this);
    }
}
