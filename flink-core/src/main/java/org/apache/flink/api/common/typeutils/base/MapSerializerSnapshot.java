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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;

import java.util.Map;

/** Snapshot class for the {@link MapSerializer}. */
public class MapSerializerSnapshot<K, V>
        extends CompositeTypeSerializerSnapshot<Map<K, V>, MapSerializer<K, V>> {

    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    public MapSerializerSnapshot() {}

    /** Constructor to create the snapshot for writing. */
    public MapSerializerSnapshot(MapSerializer<K, V> mapSerializer) {
        super(mapSerializer);
    }

    @Override
    public int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected MapSerializer<K, V> createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {
        @SuppressWarnings("unchecked")
        TypeSerializer<K> keySerializer = (TypeSerializer<K>) nestedSerializers[0];

        @SuppressWarnings("unchecked")
        TypeSerializer<V> valueSerializer = (TypeSerializer<V>) nestedSerializers[1];

        return new MapSerializer<>(keySerializer, valueSerializer);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(MapSerializer<K, V> outerSerializer) {
        return new TypeSerializer<?>[] {
            outerSerializer.getKeySerializer(), outerSerializer.getValueSerializer()
        };
    }

    @SuppressWarnings("unchecked")
    public TypeSerializerSnapshot<K> getKeySerializerSnapshot() {
        return (TypeSerializerSnapshot<K>) getNestedSerializerSnapshots()[0];
    }

    @SuppressWarnings("unchecked")
    public TypeSerializerSnapshot<V> getValueSerializerSnapshot() {
        return (TypeSerializerSnapshot<V>) getNestedSerializerSnapshots()[1];
    }
}
