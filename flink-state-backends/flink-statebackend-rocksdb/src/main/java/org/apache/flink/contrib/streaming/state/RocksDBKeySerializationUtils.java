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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Utils for RocksDB state serialization and deserialization.
 */
public class RocksDBKeySerializationUtils {

	public static int readKeyGroup(int keyGroupPrefixBytes, DataInputView inputView) throws IOException {
		int keyGroup = 0;
		for (int i = 0; i < keyGroupPrefixBytes; ++i) {
			keyGroup <<= 8;
			keyGroup |= (inputView.readByte() & 0xFF);
		}
		return keyGroup;
	}

	public static <K> K readKey(
		TypeSerializer<K> keySerializer,
		ByteArrayInputStreamWithPos inputStream,
		DataInputView inputView,
		boolean ambiguousKeyPossible) throws IOException {
		int beforeRead = inputStream.getPosition();
		K key = keySerializer.deserialize(inputView);
		if (ambiguousKeyPossible) {
			int length = inputStream.getPosition() - beforeRead;
			readVariableIntBytes(inputView, length);
		}
		return key;
	}

	public static <N> N readNamespace(
		TypeSerializer<N> namespaceSerializer,
		ByteArrayInputStreamWithPos inputStream,
		DataInputView inputView,
		boolean ambiguousKeyPossible) throws IOException {
		int beforeRead = inputStream.getPosition();
		N namespace = namespaceSerializer.deserialize(inputView);
		if (ambiguousKeyPossible) {
			int length = inputStream.getPosition() - beforeRead;
			readVariableIntBytes(inputView, length);
		}
		return namespace;
	}

	public static <N> void writeNameSpace(
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		ByteArrayOutputStreamWithPos keySerializationStream,
		DataOutputView keySerializationDataOutputView,
		boolean ambiguousKeyPossible) throws IOException {

		int beforeWrite = keySerializationStream.getPosition();
		namespaceSerializer.serialize(namespace, keySerializationDataOutputView);

		if (ambiguousKeyPossible) {
			//write length of namespace
			writeLengthFrom(beforeWrite, keySerializationStream,
				keySerializationDataOutputView);
		}
	}

	public static boolean isAmbiguousKeyPossible(TypeSerializer keySerializer, TypeSerializer namespaceSerializer) {
		return (keySerializer.getLength() < 0) && (namespaceSerializer.getLength() < 0);
	}

	public static void writeKeyGroup(
		int keyGroup,
		int keyGroupPrefixBytes,
		DataOutputView keySerializationDateDataOutputView) throws IOException {
		for (int i = keyGroupPrefixBytes; --i >= 0; ) {
			keySerializationDateDataOutputView.writeByte(keyGroup >>> (i << 3));
		}
	}

	public static <K> void writeKey(
		K key,
		TypeSerializer<K> keySerializer,
		ByteArrayOutputStreamWithPos keySerializationStream,
		DataOutputView keySerializationDataOutputView,
		boolean ambiguousKeyPossible) throws IOException {
		//write key
		int beforeWrite = keySerializationStream.getPosition();
		keySerializer.serialize(key, keySerializationDataOutputView);

		if (ambiguousKeyPossible) {
			//write size of key
			writeLengthFrom(beforeWrite, keySerializationStream,
				keySerializationDataOutputView);
		}
	}

	private static void readVariableIntBytes(DataInputView inputView, int value) throws IOException {
		do {
			inputView.readByte();
			value >>>= 8;
		} while (value != 0);
	}

	private static void writeLengthFrom(
		int fromPosition,
		ByteArrayOutputStreamWithPos keySerializationStream,
		DataOutputView keySerializationDateDataOutputView) throws IOException {
		int length = keySerializationStream.getPosition() - fromPosition;
		writeVariableIntBytes(length, keySerializationDateDataOutputView);
	}

	private static void writeVariableIntBytes(
		int value,
		DataOutputView keySerializationDateDataOutputView)
		throws IOException {
		do {
			keySerializationDateDataOutputView.writeByte(value);
			value >>>= 8;
		} while (value != 0);
	}
}
