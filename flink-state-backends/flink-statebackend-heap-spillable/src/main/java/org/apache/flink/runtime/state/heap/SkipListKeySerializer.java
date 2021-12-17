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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentInputStreamWithPos;

import java.io.IOException;

/**
 * Serializer/deserializer used for conversion between key/namespace and skip list key. It is not
 * thread safe.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 */
class SkipListKeySerializer<K, N> {

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final ByteArrayOutputStreamWithPos outputStream;
    private final DataOutputViewStreamWrapper outputView;

    SkipListKeySerializer(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer) {
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.outputStream = new ByteArrayOutputStreamWithPos();
        this.outputView = new DataOutputViewStreamWrapper(outputStream);
    }

    /**
     * Serialize the key and namespace to bytes. The format is - int: length of serialized namespace
     * - byte[]: serialized namespace - int: length of serialized key - byte[]: serialized key
     */
    byte[] serialize(K key, N namespace) {
        // we know that the segment contains a byte[], because it is created
        // in the method below by wrapping a byte[]
        return serializeToSegment(key, namespace).getArray();
    }

    /**
     * Serialize the key and namespace to bytes. The format is - int: length of serialized namespace
     * - byte[]: serialized namespace - int: length of serialized key - byte[]: serialized key
     */
    MemorySegment serializeToSegment(K key, N namespace) {
        outputStream.reset();
        try {
            // serialize namespace
            outputStream.setPosition(Integer.BYTES);
            namespaceSerializer.serialize(namespace, outputView);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize namespace", e);
        }

        int keyStartPos = outputStream.getPosition();
        try {
            // serialize key
            outputStream.setPosition(keyStartPos + Integer.BYTES);
            keySerializer.serialize(key, outputView);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key", e);
        }

        final byte[] result = outputStream.toByteArray();
        final MemorySegment segment = MemorySegmentFactory.wrap(result);

        // set length of namespace and key
        segment.putInt(0, keyStartPos - Integer.BYTES);
        segment.putInt(keyStartPos, result.length - keyStartPos - Integer.BYTES);

        return segment;
    }

    /**
     * Deserialize the namespace from the byte buffer which stores skip list key.
     *
     * @param memorySegment the memory segment which stores the skip list key.
     * @param offset the start position of the skip list key in the byte buffer.
     * @param len length of the skip list key.
     */
    N deserializeNamespace(MemorySegment memorySegment, int offset, int len) {
        MemorySegmentInputStreamWithPos inputStream =
                new MemorySegmentInputStreamWithPos(memorySegment, offset, len);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
        inputStream.setPosition(offset + Integer.BYTES);
        try {
            return namespaceSerializer.deserialize(inputView);
        } catch (IOException e) {
            throw new RuntimeException("deserialize namespace failed", e);
        }
    }

    /**
     * Deserialize the partition key from the byte buffer which stores skip list key.
     *
     * @param memorySegment the memory segment which stores the skip list key.
     * @param offset the start position of the skip list key in the byte buffer.
     * @param len length of the skip list key.
     */
    K deserializeKey(MemorySegment memorySegment, int offset, int len) {
        MemorySegmentInputStreamWithPos inputStream =
                new MemorySegmentInputStreamWithPos(memorySegment, offset, len);
        DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
        int namespaceLen = memorySegment.getInt(offset);
        inputStream.setPosition(offset + Integer.BYTES + namespaceLen + Integer.BYTES);
        try {
            return keySerializer.deserialize(inputView);
        } catch (IOException e) {
            throw new RuntimeException("deserialize key failed", e);
        }
    }

    /**
     * Gets serialized key and namespace from the byte buffer.
     *
     * @param memorySegment the memory segment which stores the skip list key.
     * @param offset the start position of the skip list key in the byte buffer.
     * @return tuple of serialized key and namespace.
     */
    Tuple2<byte[], byte[]> getSerializedKeyAndNamespace(MemorySegment memorySegment, int offset) {
        // read namespace
        int namespaceLen = memorySegment.getInt(offset);
        MemorySegment namespaceSegment = MemorySegmentFactory.allocateUnpooledSegment(namespaceLen);
        memorySegment.copyTo(offset + Integer.BYTES, namespaceSegment, 0, namespaceLen);

        // read key
        int keyOffset = offset + Integer.BYTES + namespaceLen;
        int keyLen = memorySegment.getInt(keyOffset);
        MemorySegment keySegment = MemorySegmentFactory.allocateUnpooledSegment(keyLen);
        memorySegment.copyTo(keyOffset + Integer.BYTES, keySegment, 0, keyLen);

        return Tuple2.of(keySegment.getArray(), namespaceSegment.getArray());
    }

    /** Serialize the namespace to bytes. */
    byte[] serializeNamespace(N namespace) {
        outputStream.reset();
        try {
            namespaceSerializer.serialize(namespace, outputView);
        } catch (IOException e) {
            throw new RuntimeException("serialize namespace failed", e);
        }
        return outputStream.toByteArray();
    }

    MemorySegment serializeNamespaceToSegment(N namespace) {
        return MemorySegmentFactory.wrap(serializeNamespace(namespace));
    }
}
