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

package org.apache.flink.queryablestate.client.state.serialization;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization and deserialization the different state types and namespaces.
 */
public final class KvStateSerializer {

	// ------------------------------------------------------------------------
	// Generic serialization utils
	// ------------------------------------------------------------------------

	/**
	 * Serializes the key and namespace into a {@link ByteBuffer}.
	 *
	 * <p>The serialized format matches the RocksDB state backend key format, i.e.
	 * the key and namespace don't have to be deserialized for RocksDB lookups.
	 *
	 * @param key                 Key to serialize
	 * @param keySerializer       Serializer for the key
	 * @param namespace           Namespace to serialize
	 * @param namespaceSerializer Serializer for the namespace
	 * @param <K>                 Key type
	 * @param <N>                 Namespace type
	 * @return Buffer holding the serialized key and namespace
	 * @throws IOException Serialization errors are forwarded
	 */
	public static <K, N> byte[] serializeKeyAndNamespace(
			K key,
			TypeSerializer<K> keySerializer,
			N namespace,
			TypeSerializer<N> namespaceSerializer) throws IOException {

		DataOutputSerializer dos = new DataOutputSerializer(32);

		keySerializer.serialize(key, dos);
		dos.writeByte(42);
		namespaceSerializer.serialize(namespace, dos);

		return dos.getCopyOfBuffer();
	}

	/**
	 * Deserializes the key and namespace into a {@link Tuple2}.
	 *
	 * @param serializedKeyAndNamespace Serialized key and namespace
	 * @param keySerializer             Serializer for the key
	 * @param namespaceSerializer       Serializer for the namespace
	 * @param <K>                       Key type
	 * @param <N>                       Namespace
	 * @return Tuple2 holding deserialized key and namespace
	 * @throws IOException              if the deserialization fails for any reason
	 */
	public static <K, N> Tuple2<K, N> deserializeKeyAndNamespace(
			byte[] serializedKeyAndNamespace,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) throws IOException {

		DataInputDeserializer dis = new DataInputDeserializer(
				serializedKeyAndNamespace,
				0,
				serializedKeyAndNamespace.length);

		try {
			K key = keySerializer.deserialize(dis);
			byte magicNumber = dis.readByte();
			if (magicNumber != 42) {
				throw new IOException("Unexpected magic number " + magicNumber + ".");
			}
			N namespace = namespaceSerializer.deserialize(dis);

			if (dis.available() > 0) {
				throw new IOException("Unconsumed bytes in the serialized key and namespace.");
			}

			return new Tuple2<>(key, namespace);
		} catch (IOException e) {
			throw new IOException("Unable to deserialize key " +
				"and namespace. This indicates a mismatch in the key/namespace " +
				"serializers used by the KvState instance and this access.", e);
		}
	}

	/**
	 * Serializes the value with the given serializer.
	 *
	 * @param value      Value of type T to serialize
	 * @param serializer Serializer for T
	 * @param <T>        Type of the value
	 * @return Serialized value or <code>null</code> if value <code>null</code>
	 * @throws IOException On failure during serialization
	 */
	public static <T> byte[] serializeValue(T value, TypeSerializer<T> serializer) throws IOException {
		if (value != null) {
			// Serialize
			DataOutputSerializer dos = new DataOutputSerializer(32);
			serializer.serialize(value, dos);
			return dos.getCopyOfBuffer();
		} else {
			return null;
		}
	}

	/**
	 * Deserializes the value with the given serializer.
	 *
	 * @param serializedValue Serialized value of type T
	 * @param serializer      Serializer for T
	 * @param <T>             Type of the value
	 * @return Deserialized value or <code>null</code> if the serialized value
	 * is <code>null</code>
	 * @throws IOException On failure during deserialization
	 */
	public static <T> T deserializeValue(byte[] serializedValue, TypeSerializer<T> serializer) throws IOException {
		if (serializedValue == null) {
			return null;
		} else {
			final DataInputDeserializer deser = new DataInputDeserializer(
				serializedValue, 0, serializedValue.length);
			final T value = serializer.deserialize(deser);
			if (deser.available() > 0) {
				throw new IOException(
					"Unconsumed bytes in the deserialized value. " +
						"This indicates a mismatch in the value serializers " +
						"used by the KvState instance and this access.");
			}
			return value;
		}
	}

	/**
	 * Deserializes all values with the given serializer.
	 *
	 * @param serializedValue Serialized value of type List&lt;T&gt;
	 * @param serializer      Serializer for T
	 * @param <T>             Type of the value
	 * @return Deserialized list or <code>null</code> if the serialized value
	 * is <code>null</code>
	 * @throws IOException On failure during deserialization
	 */
	public static <T> List<T> deserializeList(byte[] serializedValue, TypeSerializer<T> serializer) throws IOException {
		if (serializedValue != null) {
			final DataInputDeserializer in = new DataInputDeserializer(
				serializedValue, 0, serializedValue.length);

			try {
				final List<T> result = new ArrayList<>();
				while (in.available() > 0) {
					result.add(serializer.deserialize(in));

					// The expected binary format has a single byte separator. We
					// want a consistent binary format in order to not need any
					// special casing during deserialization. A "cleaner" format
					// would skip this extra byte, but would require a memory copy
					// for RocksDB, which stores the data serialized in this way
					// for lists.
					if (in.available() > 0) {
						in.readByte();
					}
				}

				return result;
			} catch (IOException e) {
				throw new IOException(
						"Unable to deserialize value. " +
							"This indicates a mismatch in the value serializers " +
							"used by the KvState instance and this access.", e);
			}
		} else {
			return null;
		}
	}

	/**
	 * Serializes all values of the Iterable with the given serializer.
	 *
	 * @param entries         Key-value pairs to serialize
	 * @param keySerializer   Serializer for UK
	 * @param valueSerializer Serializer for UV
	 * @param <UK>            Type of the keys
	 * @param <UV>            Type of the values
	 * @return Serialized values or <code>null</code> if values <code>null</code> or empty
	 * @throws IOException On failure during serialization
	 */
	public static <UK, UV> byte[] serializeMap(Iterable<Map.Entry<UK, UV>> entries, TypeSerializer<UK> keySerializer, TypeSerializer<UV> valueSerializer) throws IOException {
		if (entries != null) {
			// Serialize
			DataOutputSerializer dos = new DataOutputSerializer(32);

			for (Map.Entry<UK, UV> entry : entries) {
				keySerializer.serialize(entry.getKey(), dos);

				if (entry.getValue() == null) {
					dos.writeBoolean(true);
				} else {
					dos.writeBoolean(false);
					valueSerializer.serialize(entry.getValue(), dos);
				}
			}

			return dos.getCopyOfBuffer();
		} else {
			return null;
		}
	}

	/**
	 * Deserializes all kv pairs with the given serializer.
	 *
	 * @param serializedValue Serialized value of type Map&lt;UK, UV&gt;
	 * @param keySerializer   Serializer for UK
	 * @param valueSerializer Serializer for UV
	 * @param <UK>            Type of the key
	 * @param <UV>            Type of the value.
	 * @return Deserialized map or <code>null</code> if the serialized value
	 * is <code>null</code>
	 * @throws IOException On failure during deserialization
	 */
	public static <UK, UV> Map<UK, UV> deserializeMap(byte[] serializedValue, TypeSerializer<UK> keySerializer, TypeSerializer<UV> valueSerializer) throws IOException {
		if (serializedValue != null) {
			DataInputDeserializer in = new DataInputDeserializer(serializedValue, 0, serializedValue.length);

			Map<UK, UV> result = new HashMap<>();
			while (in.available() > 0) {
				UK key = keySerializer.deserialize(in);

				boolean isNull = in.readBoolean();
				UV value = isNull ? null : valueSerializer.deserialize(in);

				result.put(key, value);
			}

			return result;
		} else {
			return null;
		}
	}
}
