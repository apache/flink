/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.v2.InternalSyncState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;

/**
 * Base class for {@link InternalSyncState} implementations that store state in a ForSt database.
 *
 * @param <K> Type of the key in the state.
 * @param <V> Type of the value in the state.
 */
public class AbstractForStState<K, V> implements InternalSyncState<K> {

    protected final RocksDB db;

    protected final ColumnFamilyHandle columnFamily;

    private final int maxParallelism;

    protected final WriteOptions writeOptions;

    protected final TypeSerializer<K> keySerializer;

    protected final TypeSerializer<V> valueSerializer;

    private final ThreadLocal<SerializedCompositeKeyBuilder<K>> serializedKeyBuilder;

    private final ThreadLocal<DataInputDeserializer> valueDataInputView;

    private final ThreadLocal<DataOutputSerializer> valueDataOutputView;

    protected AbstractForStState(
            RocksDB forstDB,
            WriteOptions writeOptions,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer,
            int maxParallelism) {
        this.db = forstDB;
        this.writeOptions = writeOptions;
        this.columnFamily = columnFamily;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.serializedKeyBuilder =
                ThreadLocal.withInitial(
                        () ->
                                new SerializedCompositeKeyBuilder<>(
                                        keySerializer,
                                        CompositeKeySerializationUtils
                                                .computeRequiredBytesInKeyGroupPrefix(
                                                        maxParallelism),
                                        32));
        this.valueDataInputView = ThreadLocal.withInitial(DataInputDeserializer::new);
        this.valueDataOutputView = ThreadLocal.withInitial(() -> new DataOutputSerializer(128));
        this.maxParallelism = maxParallelism;
    }

    @Override
    public void clear(K key) throws IOException {
        try {
            db.delete(columnFamily, writeOptions, serializeCurrentKeyAndKeyGroup(key));
        } catch (RocksDBException e) {
            throw new IOException("Error while removing entry from ForStDB", e);
        }
    }

    protected byte[] serializeCurrentKeyAndKeyGroup(K key) throws IOException {
        SerializedCompositeKeyBuilder<K> keyBuilder = serializedKeyBuilder.get();
        keyBuilder.setKeyAndKeyGroup(
                key, KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism));
        return keyBuilder.build();
    }

    protected byte[] serializeValue(V value) throws IOException {
        DataOutputSerializer outputView = valueDataOutputView.get();
        outputView.clear();
        valueSerializer.serialize(value, outputView);
        return outputView.getCopyOfBuffer();
    }

    protected V deserializeValue(byte[] valueBytes) throws IOException {
        DataInputDeserializer deserializeView = valueDataInputView.get();
        deserializeView.setBuffer(valueBytes);
        return valueSerializer.deserialize(deserializeView);
    }
}
