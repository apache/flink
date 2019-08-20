/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SkipListKeyComparator}.
 */
public class SkipListKeyComparatorTest {

	@Test
	public void testCompareSkipListKeyForPrimitive() {
		// The comparison of the serialized forms are identical to that of
		// the values only when the numbers to compare are both not negative.
		TypeSerializer<Long> keySerializer = LongSerializer.INSTANCE;
		TypeSerializer<Integer> namespaceSerializer = IntSerializer.INSTANCE;

		SkipListKeySerializer<Long, Integer> skipListKeySerializer =
			new SkipListKeySerializer<>(keySerializer, namespaceSerializer);

		// verify equal namespace and key
		verifySkipListKey(skipListKeySerializer, 0L, 0, 0L, 0, 0);

		// verify equal namespace and unequal key
		verifySkipListKey(skipListKeySerializer, 0L, 5, 1L, 5, -1);
		verifySkipListKey(skipListKeySerializer, 192L, 90, 87L, 90, 1);

		// verify unequal namespace and equal key
		verifySkipListKey(skipListKeySerializer, 8374L, 73, 8374L, 1212, -1);
		verifySkipListKey(skipListKeySerializer, 839L, 23231, 839L, 2, 1);

		// verify unequal namespace and unequal key
		verifySkipListKey(skipListKeySerializer, 1L, 2, 3L, 4, -1);
		verifySkipListKey(skipListKeySerializer, 1L, 4, 3L, 2, 1);
		verifySkipListKey(skipListKeySerializer, 3L, 2, 1L, 4, -1);
		verifySkipListKey(skipListKeySerializer, 3L, 4, 1L, 2, 1);
	}

	@Test
	public void testCompareSkipListKeyForByteArray() {
		TypeSerializer<byte[]> keySerializer = ByteArraySerializer.INSTANCE;
		TypeSerializer<byte[]> namespaceSerializer = ByteArraySerializer.INSTANCE;

		SkipListKeySerializer<byte[], byte[]> skipListKeySerializer =
			new SkipListKeySerializer<>(keySerializer, namespaceSerializer);

		// verify equal namespace and key
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25", "34", "25", 0);

		// verify larger namespace
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "27", "34", "25", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "27", "34", "25,34", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "27,28", "34", "25", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "27,28", "34", "25,34", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "27,28", "34", "27,3", 1);

		// verify smaller namespace
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25", "34", "27", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25", "34", "27,34", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25,28", "34", "27", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25,28", "34", "27,34", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25,28", "34", "25,34", -1);

		// verify larger key
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25", "30", "25", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34", "25", "30,38", "25", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34,22", "25", "30", "25", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34,22", "25", "30,38", "25", 1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "34,82", "25", "34,38", "25", 1);

		// verify smaller key
		verifySkipListKeyForByteArray(skipListKeySerializer, "30", "25", "34", "25", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "30,38", "25", "34", "25", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "30", "25", "34,22", "25", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "30,38", "25", "34,22", "25", -1);
		verifySkipListKeyForByteArray(skipListKeySerializer, "30,38", "25", "30,72", "25", -1);
	}

	@Test
	public void testCompareNamespace() {
		TypeSerializer<byte[]> keySerializer = ByteArraySerializer.INSTANCE;
		TypeSerializer<byte[]> namespaceSerializer = ByteArraySerializer.INSTANCE;

		SkipListKeySerializer<byte[], byte[]> skipListKeySerializer =
			new SkipListKeySerializer<>(keySerializer, namespaceSerializer);

		// test equal namespace
		verifyNamespaceCompare(skipListKeySerializer, "23", "34", "23", 0);

		// test smaller namespace
		verifyNamespaceCompare(skipListKeySerializer, "23", "34", "24", -1);
		verifyNamespaceCompare(skipListKeySerializer, "23", "34", "24,35", -1);
		verifyNamespaceCompare(skipListKeySerializer, "23,25", "34", "24", -1);
		verifyNamespaceCompare(skipListKeySerializer, "23,20", "34", "24,45", -1);
		verifyNamespaceCompare(skipListKeySerializer, "23,20", "34", "23,45", -1);

		// test larger namespace
		verifyNamespaceCompare(skipListKeySerializer, "26", "34", "14", 1);
		verifyNamespaceCompare(skipListKeySerializer, "26", "34", "14,73", 1);
		verifyNamespaceCompare(skipListKeySerializer, "26,25", "34", "14", 1);
		verifyNamespaceCompare(skipListKeySerializer, "26,20", "34", "14,45", 1);
		verifyNamespaceCompare(skipListKeySerializer, "26,90", "34", "26,45", 1);
	}

	private void verifySkipListKeyForByteArray(
		SkipListKeySerializer<byte[], byte[]> keySerializer,
		String key1, String namespace1, String key2, String namespace2, int result) {
		verifySkipListKey(keySerializer, convertByteString(key1), convertByteString(namespace1),
			convertByteString(key2), convertByteString(namespace2), result);
	}

	private <K, N> void verifySkipListKey(
		SkipListKeySerializer<K, N> keySerializer,
		K key1, N namespace1, K key2, N namespace2, int result) {
		ByteBuffer b1 = ByteBuffer.wrap(keySerializer.serialize(key1, namespace1));
		ByteBuffer b2 = ByteBuffer.wrap(keySerializer.serialize(key2, namespace2));
		int actual = SkipListKeyComparator.compareTo(b1, 0, b2, 0);
		assertTrue((result == 0 && actual == 0) || (result * actual > 0));
	}

	private void verifyNamespaceCompare(
		SkipListKeySerializer<byte[], byte[]> keySerializer,
		String namespace, String targetKey, String targetNamespace, int result) {
		byte[] n = keySerializer.serializeNamespace(convertByteString(namespace));
		byte[] k = keySerializer.serialize(convertByteString(targetKey), convertByteString(targetNamespace));
		int actual = SkipListKeyComparator.compareNamespaceAndNode(
			ByteBuffer.wrap(n), 0, n.length, ByteBuffer.wrap(k), 0);
		assertTrue((result == 0 && actual == 0) || (result * actual > 0));
	}

	private byte[] convertByteString(String str) {
		String[] subStr = str.split(",");
		byte[] value = new byte[subStr.length];
		for (int i = 0; i < subStr.length; i++) {
			int v = Integer.valueOf(subStr[i]);
			value[i] = (byte) v;
		}
		return value;
	}

	/**
	 * A serializer for byte array which does not support deserialization.
	 */
	private static class ByteArraySerializer extends TypeSerializerSingleton<byte[]> {

		private static final byte[] EMPTY = new byte[0];

		public static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public byte[] createInstance() {
			return EMPTY;
		}

		@Override
		public byte[] copy(byte[] from) {
			byte[] copy = new byte[from.length];
			System.arraycopy(from, 0, copy, 0, from.length);
			return copy;
		}

		@Override
		public byte[] copy(byte[] from, byte[] reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(byte[] record, DataOutputView target) throws IOException {
			if (record == null) {
				throw new IllegalArgumentException("The record must not be null.");
			}

			// do not write length of array, so deserialize is not supported
			target.write(record);
		}

		@Override
		public byte[] deserialize(DataInputView source) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public byte[] deserialize(byte[] reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) {
			throw new UnsupportedOperationException();
		}

		@Override
		public TypeSerializerSnapshot<byte[]> snapshotConfiguration() {
			throw new UnsupportedOperationException();
		}
	}
}
