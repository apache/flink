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
import org.apache.flink.api.common.typeutils.LegacySerializerSnapshotTransformer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.dataview.MapView;

import java.io.IOException;
import java.util.Map;

/**
 * A serializer for {@link MapView}. The serializer relies on a key serializer and a value
 * serializer for the serialization of the map's key-value pairs.
 *
 * <p>The serialization format for the map is as follows: four bytes for the length of the map,
 * followed by the serialized representation of each key-value pair. To allow null values, each
 * value is prefixed by a null marker.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 */
@Internal
@Deprecated
public class MapViewSerializer<K, V> extends TypeSerializer<MapView<K, V>>
        implements LegacySerializerSnapshotTransformer<MapView<K, V>> {

    private static final long serialVersionUID = -9007142882049098705L;

    private final TypeSerializer<Map<K, V>> mapSerializer;

    public MapViewSerializer(TypeSerializer<Map<K, V>> mapSerializer) {
        this.mapSerializer = mapSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<MapView<K, V>> duplicate() {
        return new MapViewSerializer<>(mapSerializer.duplicate());
    }

    @Override
    public MapView<K, V> createInstance() {
        return new MapView<>();
    }

    @Override
    public MapView<K, V> copy(MapView<K, V> from) {
        final MapView<K, V> view = new MapView<>();
        view.setMap(mapSerializer.copy(from.getMap()));
        return view;
    }

    @Override
    public MapView<K, V> copy(MapView<K, V> from, MapView<K, V> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(MapView<K, V> record, DataOutputView target) throws IOException {
        mapSerializer.serialize(record.getMap(), target);
    }

    @Override
    public MapView<K, V> deserialize(DataInputView source) throws IOException {
        final MapView<K, V> view = new MapView<>();
        view.setMap(mapSerializer.deserialize(source));
        return view;
    }

    @Override
    public MapView<K, V> deserialize(MapView<K, V> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        mapSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MapViewSerializer) {
            //noinspection unchecked
            MapViewSerializer<K, V> other = (MapViewSerializer<K, V>) obj;
            return mapSerializer.equals(other.mapSerializer);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return mapSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<MapView<K, V>> snapshotConfiguration() {
        return new MapViewSerializerSnapshot<>(this);
    }

    /**
     * We need to override this as a {@link LegacySerializerSnapshotTransformer} because in Flink
     * 1.6.x and below, this serializer was incorrectly returning directly the snapshot of the
     * nested map serializer as its own snapshot.
     *
     * <p>This method transforms the incorrect map serializer snapshot to be a proper {@link
     * MapViewSerializerSnapshot}.
     */
    @Override
    public <U> TypeSerializerSnapshot<MapView<K, V>> transformLegacySerializerSnapshot(
            TypeSerializerSnapshot<U> legacySnapshot) {
        if (legacySnapshot instanceof MapViewSerializerSnapshot) {
            return (TypeSerializerSnapshot<MapView<K, V>>) legacySnapshot;
        } else {
            throw new UnsupportedOperationException(
                    legacySnapshot.getClass().getCanonicalName() + " is not supported.");
        }
    }

    public TypeSerializer<Map<K, V>> getMapSerializer() {
        return mapSerializer;
    }
}
