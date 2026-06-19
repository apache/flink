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

package org.apache.flink.state.rocksdb.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueStateSnapshot;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Iterator;

/** An iterator over heap timers that produces rocks compatible binary format. */
public final class RocksQueueIterator implements SingleStateIterator {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final DataOutputSerializer keyOut = new DataOutputSerializer(128);
    private final HeapPriorityQueueStateSnapshot<?> queueSnapshot;
    private final Iterator<Integer> keyGroupRangeIterator;
    private final int kvStateId;
    private final int keyGroupPrefixBytes;
    private final TypeSerializer<Object> elementSerializer;

    private Iterator<Object> elementsForKeyGroup;
    private int afterKeyMark = 0;

    private boolean isValid;
    private byte[] currentKey;

    public RocksQueueIterator(
            HeapPriorityQueueStateSnapshot<?> queuesSnapshot,
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            int kvStateId) {
        this.queueSnapshot = queuesSnapshot;
        this.elementSerializer = castToType(queuesSnapshot.getMetaInfo().getElementSerializer());
        this.keyGroupRangeIterator = keyGroupRange.iterator();
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.kvStateId = kvStateId;
        if (keyGroupRangeIterator.hasNext()) {
            try {
                if (moveToNextNonEmptyKeyGroup()) {
                    isValid = true;
                    next();
                } else {
                    isValid = false;
                }
            } catch (IOException e) {
                throw new FlinkRuntimeException(e);
            }
        }
    }

    @Override
    public void next() {
        try {
            if (!elementsForKeyGroup.hasNext()) {
                boolean hasElement = moveToNextNonEmptyKeyGroup();
                if (!hasElement) {
                    isValid = false;
                    return;
                }
            }
            keyOut.setPosition(afterKeyMark);
            elementSerializer.serialize(elementsForKeyGroup.next(), keyOut);
            this.currentKey = keyOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    private boolean moveToNextNonEmptyKeyGroup() throws IOException {
        while (keyGroupRangeIterator.hasNext()) {
            Integer keyGroupId = keyGroupRangeIterator.next();
            elementsForKeyGroup = castToType(queueSnapshot.getIteratorForKeyGroup(keyGroupId));
            if (elementsForKeyGroup.hasNext()) {
                writeKeyGroupId(keyGroupId);
                return true;
            }
        }
        return false;
    }

    private void writeKeyGroupId(Integer keyGroupId) throws IOException {
        keyOut.clear();
        CompositeKeySerializationUtils.writeKeyGroup(keyGroupId, keyGroupPrefixBytes, keyOut);
        afterKeyMark = keyOut.length();
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> castToType(TypeSerializer<?> typeSerializer) {
        return (TypeSerializer<T>) typeSerializer;
    }

    @SuppressWarnings("unchecked")
    private static <T> Iterator<T> castToType(Iterator<?> iterator) {
        return (Iterator<T>) iterator;
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getKvStateId() {
        return kvStateId;
    }

    @Override
    public void close() {}
}
