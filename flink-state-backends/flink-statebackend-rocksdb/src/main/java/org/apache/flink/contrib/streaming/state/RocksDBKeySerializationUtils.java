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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Utils for RocksDB state serialization and deserialization.
 */
public class RocksDBKeySerializationUtils {

	static int readKeyGroup(int keyGroupPrefixBytes, DataInputView inputView) throws IOException {
		int keyGroup = 0;
		for (int i = 0; i < keyGroupPrefixBytes; ++i) {
			keyGroup <<= 8;
			keyGroup |= (inputView.readByte() & 0xFF);
		}
		return keyGroup;
	}

	public static <K> K readKey(
		TypeSerializer<K> keySerializer,
		DataInputDeserializer inputView,
		boolean ambiguousKeyPossible) throws IOException {
		int beforeRead = inputView.getPosition();
		K key = keySerializer.deserialize(inputView);
		if (ambiguousKeyPossible) {
			int length = inputView.getPosition() - beforeRead;
			readVariableIntBytes(inputView, length);
		}
		return key;
	}

	public static <N> N readNamespace(
		TypeSerializer<N> namespaceSerializer,
		DataInputDeserializer inputView,
		boolean ambiguousKeyPossible) throws IOException {
		int beforeRead = inputView.getPosition();
		N namespace = namespaceSerializer.deserialize(inputView);
		if (ambiguousKeyPossible) {
			int length = inputView.getPosition() - beforeRead;
			readVariableIntBytes(inputView, length);
		}
		return namespace;
	}

	public static <N> void writeNameSpace(
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		DataOutputSerializer keySerializationDataOutputView,
		boolean ambiguousKeyPossible) throws IOException {

		int beforeWrite = keySerializationDataOutputView.length();
		namespaceSerializer.serialize(namespace, keySerializationDataOutputView);

		if (ambiguousKeyPossible) {
			//write length of namespace
			writeLengthFrom(beforeWrite, keySerializationDataOutputView);
		}
	}

	public static boolean isSerializerTypeVariableSized(@Nonnull TypeSerializer<?> serializer) {
		return serializer.getLength() < 0;
	}

	public static boolean isAmbiguousKeyPossible(TypeSerializer keySerializer, TypeSerializer namespaceSerializer) {
		return (isSerializerTypeVariableSized(keySerializer) && isSerializerTypeVariableSized(namespaceSerializer));
	}

	public static void writeKeyGroup(
		int keyGroup,
		int keyGroupPrefixBytes,
		DataOutputView keySerializationDateDataOutputView) throws IOException {
		for (int i = keyGroupPrefixBytes; --i >= 0; ) {
			keySerializationDateDataOutputView.writeByte(extractByteAtPosition(keyGroup, i));
		}
	}

	public static <K> void writeKey(
		K key,
		TypeSerializer<K> keySerializer,
		DataOutputSerializer keySerializationDataOutputView,
		boolean ambiguousKeyPossible) throws IOException {
		//write key
		int beforeWrite = keySerializationDataOutputView.length();
		keySerializer.serialize(key, keySerializationDataOutputView);

		if (ambiguousKeyPossible) {
			//write size of key
			writeLengthFrom(beforeWrite, keySerializationDataOutputView);
		}
	}

	public static void readVariableIntBytes(DataInputView inputView, int value) throws IOException {
		do {
			inputView.readByte();
			value >>>= 8;
		} while (value != 0);
	}

	private static void writeLengthFrom(
		int fromPosition,
		DataOutputSerializer keySerializationDateDataOutputView) throws IOException {
		int length = keySerializationDateDataOutputView.length() - fromPosition;
		writeVariableIntBytes(length, keySerializationDateDataOutputView);
	}

	public static void writeVariableIntBytes(
		int value,
		DataOutputView keySerializationDateDataOutputView)
		throws IOException {
		do {
			keySerializationDateDataOutputView.writeByte(value);
			value >>>= 8;
		} while (value != 0);
	}

	public static void serializeKeyGroup(int keyGroup, byte[] startKeyGroupPrefixBytes) {
		final int keyGroupPrefixBytes = startKeyGroupPrefixBytes.length;
		for (int j = 0; j < keyGroupPrefixBytes; ++j) {
			startKeyGroupPrefixBytes[j] = extractByteAtPosition(keyGroup, keyGroupPrefixBytes - j - 1);
		}
	}

	private static byte extractByteAtPosition(int value, int byteIdx) {
		return (byte) ((value >>> (byteIdx << 3)));
	}

	public static int computeRequiredBytesInKeyGroupPrefix(int totalKeyGroupsInJob) {
		return totalKeyGroupsInJob > (Byte.MAX_VALUE + 1) ? 2 : 1;
	}
}
