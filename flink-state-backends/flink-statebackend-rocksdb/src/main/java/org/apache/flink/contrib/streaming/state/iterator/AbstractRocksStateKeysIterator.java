/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.DataInputDeserializer;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Base class for iterators over RocksDB column families.
 *
 * @param <K> the type of the iterated objects, which are keys in RocksDB.
 */
@Internal
public abstract class AbstractRocksStateKeysIterator<K> implements AutoCloseable {

    @Nonnull protected final RocksIteratorWrapper iterator;

    @Nonnull protected final String state;

    @Nonnull protected final TypeSerializer<K> keySerializer;

    protected final boolean ambiguousKeyPossible;

    protected final int keyGroupPrefixBytes;

    protected final DataInputDeserializer byteArrayDataInputView;

    public AbstractRocksStateKeysIterator(
            @Nonnull RocksIteratorWrapper iterator,
            @Nonnull String state,
            @Nonnull TypeSerializer<K> keySerializer,
            int keyGroupPrefixBytes,
            boolean ambiguousKeyPossible) {
        this.iterator = iterator;
        this.state = state;
        this.keySerializer = keySerializer;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.ambiguousKeyPossible = ambiguousKeyPossible;
        this.byteArrayDataInputView = new DataInputDeserializer();
    }

    protected K deserializeKey(byte[] keyBytes, DataInputDeserializer readView) throws IOException {
        readView.setBuffer(keyBytes, keyGroupPrefixBytes, keyBytes.length - keyGroupPrefixBytes);
        return RocksDBKeySerializationUtils.readKey(
                keySerializer, byteArrayDataInputView, ambiguousKeyPossible);
    }

    @Override
    public void close() {
        iterator.close();
    }
}
