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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.DefaultPair;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A helper class for de/serialize.
 */
public class StateSerializerUtil {
	private static final byte KEY_PREFIX_BYTE = 0x0f;
	public static final byte KEY_END_BYTE = 0x7f;
	private static final int KEY_PREFIX_BYTE_LENGTH = 1;
	public static final int GROUP_WRITE_BYTES = 2;
	public static final byte DELIMITER = ',';

	public static <K> byte[] getSerializedKeyForKeyedValueState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKey(outputView, key, keySerializer, group, stateNameByte);
		return outputStream.toByteArray();
	}

	public static <K> K getDeserializedKeyForKeyedValueState(
		byte[] serializedKey,
		TypeSerializer<K> keySerializer,
		int serializedStateNameLength) throws IOException {
		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedKey);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

		return getDeserializedSingleKey(inputView, keySerializer, serializedStateNameLength);
	}

	public static <K> byte[] getSerializedKeyForKeyedListState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKey(outputView, key, keySerializer, group, stateNameByte);
		return outputStream.toByteArray();
	}

	public static <K> K getDeserializedKeyForKeyedListState(
		byte[] serializedKey,
		TypeSerializer<K> keySerializer,
		int serializedStateNameLength) throws IOException {
		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedKey);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

		return getDeserializedSingleKey(inputView, keySerializer, serializedStateNameLength);
	}

	public static <K> byte[] getSerializedPrefixKeyForKeyedMapState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKey(outputView, key, keySerializer, group, stateNameByte);
		return outputStream.toByteArray();
	}

	public static <K, MK> byte[] getSerializedPrefixKeyEndForKeyedMapState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		MK mapKey,
		TypeSerializer<MK> mapKeySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		getSerializedKeyForKeyedValueState(outputStream, outputView, key, keySerializer, group, stateNameByte);
		if (mapKey != null) {
			outputView.write(KEY_PREFIX_BYTE);
			mapKeySerializer.serialize(mapKey, outputView);
		}
		outputView.write(KEY_END_BYTE);
		return outputStream.toByteArray();
	}

	public static <K, N, MK> byte[] getSerializedPrefixKeyEndForSubKeyedMapState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		MK mapKey,
		TypeSerializer<MK> mapKeySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKey(outputView, key, keySerializer, group, stateNameByte);

		serializeItemWithKeyPrefix(outputView, namespace, namespaceSerializer);

		if (mapKey != null) {
			serializeItemWithKeyPrefix(outputView, mapKey, mapKeySerializer);
		}

		outputView.write(KEY_END_BYTE);
		return outputStream.toByteArray();
	}

	public static <K, MK> byte[] getSerializedKeyForKeyedMapState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		MK mapKey,
		TypeSerializer<MK> mapKeySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeDoubleKey(outputView, key, keySerializer, mapKey, mapKeySerializer, group, stateNameByte);
		return outputStream.toByteArray();
	}

	public static <K> K getDeserializedKeyForKeyedMapState(
		byte[] serializedBytes,
		TypeSerializer<K> keySerializer,
		int serializedStateNameLength) throws IOException {
		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedBytes);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

		return getDeserializedSingleKey(inputView, keySerializer, serializedStateNameLength);
	}

	public static <K, MK> MK getDeserializedMapKeyForKeyedMapState(
		byte[] serializedBytes,
		TypeSerializer<K> keySerializer,
		TypeSerializer<MK> mapKeySerializer,
		int serializedStateNameLength) throws IOException {

		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedBytes);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
		return getDeserializedSecondKey(inputView, keySerializer, mapKeySerializer, serializedStateNameLength);
	}

	public static <K, N, MK> MK getDeserializedMapKeyForSubKeyedMapState(
		byte[] serializedBytes,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<MK> mapKeySerializer,
		int serializedStateNameLength) throws IOException {

		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedBytes);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
		getDeserializedSecondKey(inputView, keySerializer, namespaceSerializer, serializedStateNameLength);
		inputView.skipBytesToRead(KEY_PREFIX_BYTE_LENGTH);
		return mapKeySerializer.deserialize(inputView);
	}

	public static <K, N> N getDeserializedNamespcae(
		byte[] serializedBytes,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		int serializedStateNameLength) throws IOException {

		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedBytes);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
		return getDeserializedSecondKey(inputView, keySerializer, namespaceSerializer, serializedStateNameLength);
	}

	public static <K, N> Pair<K, N> getDeserializedKeyAndNamespace(
		byte[] serializedBytes,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		int serializedStateNameLenght) throws IOException {

		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedBytes);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

		K key = getDeserializedSingleKey(inputView, keySerializer, serializedStateNameLenght);

		inputView.skipBytesToRead(KEY_PREFIX_BYTE_LENGTH);
		N namespace = namespaceSerializer.deserialize(inputView);
		return new DefaultPair<>(key, namespace);
	}

	public static <K, N> byte[] getSerializedKeyForSubKeyedValueState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKeyWithNamespace(outputView, key, keySerializer, namespace, namespaceSerializer, group, stateNameByte);
		return outputStream.toByteArray();
	}

	public static <K, N> byte[] getSerializedKeyForSubKeyedListState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKeyWithNamespace(outputView, key, keySerializer, namespace, namespaceSerializer, group, stateNameByte);
		return outputStream.toByteArray();
	}

	public static <K, MK, N> byte[] getSerializedKeyForSubKeyedMapState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		MK mapKey,
		TypeSerializer<MK> mapKeySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeDoubleKeyWithNamespace(outputView, key, keySerializer, mapKey, mapKeySerializer, namespace, namespaceSerializer, group, stateNameByte);
		return outputStream.toByteArray();
	}

	public static <K, N> byte[] getSerializedPrefixKeyForSubKeyedState(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKey(outputView, key, keySerializer, group, stateNameByte);
		if (namespace != null) {
			serializeItemWithKeyPrefix(outputView, namespace, namespaceSerializer);
		}
		return outputStream.toByteArray();
	}

	public static <V> byte[] getSerializeSingleValue(
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		V value, TypeSerializer<V> valueSerializer) throws IOException {

		valueSerializer.serialize(value, outputView);

		return outputStream.toByteArray();
	}

	public static <V> V getDeserializeSingleValue(byte[] serializedValue, TypeSerializer<V> valueSerializer) throws IOException {
		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedValue);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

		return valueSerializer.deserialize(inputView);
	}

	public static <E> List<E> getDeserializeList(byte[] valueBytes, TypeSerializer<E> elementSerializer) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes);
		DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

		List<E> result = new ArrayList<>();
		while (in.available() > 0) {
			result.add(elementSerializer.deserialize(in));
			if (in.available() > 0) {
				in.readByte();
			}
		}
		return result;
	}

	public static <E> void getPreMergedList(
		DataOutputView outputView,
		Collection<? extends E> values,
		TypeSerializer<E> elementSerializer) throws IOException {

		boolean first = true;
		for (E value : values) {
			Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
			if (first) {
				first = false;
			} else {
				outputView.write(DELIMITER);
			}
			elementSerializer.serialize(value, outputView);
		}
	}

	private static <F, S> S getDeserializedSecondKey(
		DataInputViewStreamWrapper inputView,
		TypeSerializer<F> firstSerializer,
		TypeSerializer<S> secondeSerializer,
		int serializedStateNameLength) throws IOException {

		getDeserializedSingleKey(inputView, firstSerializer, serializedStateNameLength);

		// skip key prefix
		inputView.skipBytesToRead(KEY_PREFIX_BYTE_LENGTH);
		return secondeSerializer.deserialize(inputView);
	}

	private static <K> void serializeSingleKey(
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		writeGroup(outputView, group);
		if (stateNameByte != null) {
			outputView.write(stateNameByte);
		}
		serializeItemWithKeyPrefix(outputView, key, keySerializer);
	}

	private static <K, MK> void serializeDoubleKey(
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		MK mapKey,
		TypeSerializer<MK> mapKeySerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKey(outputView, key, keySerializer, group, stateNameByte);
		serializeItemWithKeyPrefix(outputView, mapKey, mapKeySerializer);
	}

	private static <K, N> void serializeSingleKeyWithNamespace(
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		int group,
		byte[] stateNameByte) throws IOException {
		serializeSingleKey(outputView, key, keySerializer, group, stateNameByte);
		serializeItemWithKeyPrefix(outputView, namespace, namespaceSerializer);
	}

	private static <K, N, MK> void serializeDoubleKeyWithNamespace(
		DataOutputView outputView,
		K key,
		TypeSerializer<K> keySerializer,
		MK mapKey,
		TypeSerializer<MK> mapKeySerializer,
		N namespace,
		TypeSerializer<N> namespaceSerializer,
		int group,
		byte[] stateNameByte) throws IOException {

		serializeSingleKeyWithNamespace(outputView, key, keySerializer, namespace, namespaceSerializer, group, stateNameByte);
		serializeItemWithKeyPrefix(outputView, mapKey, mapKeySerializer);
	}

	private static <K> K getDeserializedSingleKey(
		DataInputViewStreamWrapper inputView,
		TypeSerializer<K> keySerializer,
		int serializedStateNameLength
	) throws IOException {
		inputView.skipBytesToRead(GROUP_WRITE_BYTES + serializedStateNameLength + KEY_PREFIX_BYTE_LENGTH);
		return keySerializer.deserialize(inputView);
	}

	public static <K> void serializeItemWithKeyPrefix(
		DataOutputView outputView,
		K item,
		TypeSerializer<K> itemSerializer) throws IOException {
		outputView.write(KEY_PREFIX_BYTE);
		itemSerializer.serialize(item, outputView);
	}

	public static void serializeGroupPrefix(
		ByteArrayOutputStreamWithPos outputStream,
		int group,
		@Nullable byte[] stateNameBytes) throws IOException {

		writeGroup(outputStream, group);
		if (stateNameBytes != null) {
			outputStream.write(stateNameBytes);
		}
	}

	public static void writeGroup(ByteArrayOutputStreamWithPos outputStream, int group) {
		// because group always less than 32768, 2 bytes are ok.
		outputStream.write((group >>> 8) & 0xFF);
		outputStream.write(group & 0xFF);
	}

	public static void writeGroup(
		DataOutputView outputView,
		int group) throws IOException {
		// because group always less than 32768, 2 bytes are ok.
		outputView.write((group >>> 8) & 0xFF);
		outputView.write(group & 0xFF);
	}

	public static int getGroupFromSerializedKey(byte[] serializedBytes) throws IOException {
		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(serializedBytes);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

		return (inputView.readByte() << 8) + (inputView.readByte());
	}
}
