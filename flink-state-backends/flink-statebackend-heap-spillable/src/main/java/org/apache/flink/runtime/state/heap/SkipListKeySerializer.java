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
import org.apache.flink.core.memory.ByteBufferInputStreamWithPos;
import org.apache.flink.core.memory.ByteBufferUtils;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serializer/deserializer used for conversion between key/namespace and skip list key.
 * It is not thread safe.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 */
class SkipListKeySerializer<K, N> {

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<N> namespaceSerializer;
	private final ByteArrayOutputStreamWithPos outputStream;
	private final DataOutputViewStreamWrapper outputView;

	SkipListKeySerializer(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer) {
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.outputStream = new ByteArrayOutputStreamWithPos();
		this.outputView = new DataOutputViewStreamWrapper(outputStream);
	}

	/**
	 * Serialize the key and namespace to bytes. The format is
	 * 	- int:    length of serialized namespace
	 * 	- byte[]: serialized namespace
	 * 	- int:    length of serialized key
	 * 	- byte[]: serialized key
	 */
	byte[] serialize(K key, N namespace) {
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

		byte[] result = outputStream.toByteArray();
		// set length of namespace and key
		int namespaceLen = keyStartPos - Integer.BYTES;
		int keyLen = result.length - keyStartPos - Integer.BYTES;
		ByteBuffer byteBuffer = ByteBuffer.wrap(result);
		ByteBufferUtils.putInt(byteBuffer, 0, namespaceLen);
		ByteBufferUtils.putInt(byteBuffer, keyStartPos, keyLen);

		return result;
	}

	/**
	 * Deserialize the namespace from the byte buffer which stores skip list key.
	 *
	 * @param byteBuffer the byte buffer which stores the skip list key.
	 * @param offset     the start position of the skip list key in the byte buffer.
	 * @param len        length of the skip list key.
	 */
	N deserializeNamespace(ByteBuffer byteBuffer, int offset, int len) {
		ByteBufferInputStreamWithPos inputStream = new ByteBufferInputStreamWithPos(byteBuffer, offset, len);
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
	 * @param byteBuffer the byte buffer which stores the skip list key.
	 * @param offset     the start position of the skip list key in the byte buffer.
	 * @param len        length of the skip list key.
	 */
	K deserializeKey(ByteBuffer byteBuffer, int offset, int len) {
		ByteBufferInputStreamWithPos inputStream = new ByteBufferInputStreamWithPos(byteBuffer, offset, len);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
		int namespaceLen = ByteBufferUtils.toInt(byteBuffer, offset);
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
	 * @param byteBuffer the byte buffer which stores the skip list key.
	 * @param offset     the start position of the skip list key in the byte buffer.
	 * @return tuple of serialized key and namespace.
	 */
	Tuple2<byte[], byte[]> getSerializedKeyAndNamespace(ByteBuffer byteBuffer, int offset) {
		// read namespace
		int namespaceLen = ByteBufferUtils.toInt(byteBuffer, offset);
		byte[] namespaceBytes = new byte[namespaceLen];
		ByteBufferUtils.copyFromBufferToArray(byteBuffer, offset + Integer.BYTES, namespaceBytes,
			0, namespaceLen);

		// read key
		int keyOffset = offset + Integer.BYTES + namespaceLen;
		int keyLen = ByteBufferUtils.toInt(byteBuffer, keyOffset);
		byte[] keyBytes = new byte[keyLen];
		ByteBufferUtils.copyFromBufferToArray(byteBuffer, keyOffset + Integer.BYTES, keyBytes,
			0, keyLen);

		return Tuple2.of(keyBytes, namespaceBytes);
	}

	/**
	 * Serialize the namespace to bytes.
	 */
	byte[] serializeNamespace(N namespace) {
		outputStream.reset();
		try {
			namespaceSerializer.serialize(namespace, outputView);
		} catch (IOException e) {
			throw new RuntimeException("serialize namespace failed", e);
		}
		return outputStream.toByteArray();
	}
}
