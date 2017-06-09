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

package org.apache.flink.runtime.query.netty.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.netty.KvStateClient;
import org.apache.flink.runtime.query.netty.KvStateServer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization and deserialization of messages exchanged between
 * {@link KvStateClient} and {@link KvStateServer}.
 *
 * <p>The binary messages have the following format:
 *
 * <pre>
 *                     <------ Frame ------------------------->
 *                    +----------------------------------------+
 *                    |        HEADER (8)      | PAYLOAD (VAR) |
 * +------------------+----------------------------------------+
 * | FRAME LENGTH (4) | VERSION (4) | TYPE (4) | CONTENT (VAR) |
 * +------------------+----------------------------------------+
 * </pre>
 *
 * <p>The concrete content of a message depends on the {@link KvStateRequestType}.
 */
public final class KvStateRequestSerializer {

	/** The serialization version ID. */
	private static final int VERSION = 0x79a1b710;

	/** Byte length of the header. */
	private static final int HEADER_LENGTH = 8;

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	/**
	 * Allocates a buffer and serializes the KvState request into it.
	 *
	 * @param alloc                     ByteBuf allocator for the buffer to
	 *                                  serialize message into
	 * @param requestId                 ID for this request
	 * @param kvStateId                 ID of the requested KvState instance
	 * @param serializedKeyAndNamespace Serialized key and namespace to request
	 *                                  from the KvState instance.
	 * @return Serialized KvState request message
	 */
	public static ByteBuf serializeKvStateRequest(
			ByteBufAllocator alloc,
			long requestId,
			KvStateID kvStateId,
			byte[] serializedKeyAndNamespace) {

		// Header + request ID + KvState ID + Serialized namespace
		int frameLength = HEADER_LENGTH + 8 + (8 + 8) + (4 + serializedKeyAndNamespace.length);
		ByteBuf buf = alloc.ioBuffer(frameLength + 4); // +4 for frame length

		buf.writeInt(frameLength);

		writeHeader(buf, KvStateRequestType.REQUEST);

		buf.writeLong(requestId);
		buf.writeLong(kvStateId.getLowerPart());
		buf.writeLong(kvStateId.getUpperPart());
		buf.writeInt(serializedKeyAndNamespace.length);
		buf.writeBytes(serializedKeyAndNamespace);

		return buf;
	}

	/**
	 * Allocates a buffer and serializes the KvState request result into it.
	 *
	 * @param alloc             ByteBuf allocator for the buffer to serialize message into
	 * @param requestId         ID for this request
	 * @param serializedResult  Serialized Result
	 * @return Serialized KvState request result message
	 */
	public static ByteBuf serializeKvStateRequestResult(
			ByteBufAllocator alloc,
			long requestId,
			byte[] serializedResult) {

		Preconditions.checkNotNull(serializedResult, "Serialized result");

		// Header + request ID + serialized result
		int frameLength = HEADER_LENGTH + 8 + 4 + serializedResult.length;

		ByteBuf buf = alloc.ioBuffer(frameLength);

		buf.writeInt(frameLength);
		writeHeader(buf, KvStateRequestType.REQUEST_RESULT);
		buf.writeLong(requestId);

		buf.writeInt(serializedResult.length);
		buf.writeBytes(serializedResult);

		return buf;
	}

	/**
	 * Allocates a buffer and serializes the KvState request failure into it.
	 *
	 * @param alloc ByteBuf allocator for the buffer to serialize message into
	 * @param requestId ID of the request responding to
	 * @param cause Failure cause
	 * @return Serialized KvState request failure message
	 * @throws IOException Serialization failures are forwarded
	 */
	public static ByteBuf serializeKvStateRequestFailure(
			ByteBufAllocator alloc,
			long requestId,
			Throwable cause) throws IOException {

		ByteBuf buf = alloc.ioBuffer();

		// Frame length is set at the end
		buf.writeInt(0);

		writeHeader(buf, KvStateRequestType.REQUEST_FAILURE);

		// Message
		buf.writeLong(requestId);

		try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
				ObjectOutputStream out = new ObjectOutputStream(bbos)) {

			out.writeObject(cause);
		}

		// Set frame length
		int frameLength = buf.readableBytes() - 4;
		buf.setInt(0, frameLength);

		return buf;
	}

	/**
	 * Allocates a buffer and serializes the server failure into it.
	 *
	 * <p>The cause must not be or contain any user types as causes.
	 *
	 * @param alloc ByteBuf allocator for the buffer to serialize message into
	 * @param cause Failure cause
	 * @return Serialized server failure message
	 * @throws IOException Serialization failures are forwarded
	 */
	public static ByteBuf serializeServerFailure(ByteBufAllocator alloc, Throwable cause) throws IOException {
		ByteBuf buf = alloc.ioBuffer();

		// Frame length is set at end
		buf.writeInt(0);

		writeHeader(buf, KvStateRequestType.SERVER_FAILURE);

		try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
				ObjectOutputStream out = new ObjectOutputStream(bbos)) {

			out.writeObject(cause);
		}

		// Set frame length
		int frameLength = buf.readableBytes() - 4;
		buf.setInt(0, frameLength);

		return buf;
	}

	// ------------------------------------------------------------------------
	// Deserialization
	// ------------------------------------------------------------------------

	/**
	 * Deserializes the header and returns the request type.
	 *
	 * @param buf Buffer to deserialize (expected to be at header position)
	 * @return Deserialzied request type
	 * @throws IllegalArgumentException If unexpected message version or message type
	 */
	public static KvStateRequestType deserializeHeader(ByteBuf buf) {
		// Check the version
		int version = buf.readInt();
		if (version != VERSION) {
			throw new IllegalArgumentException("Illegal message version " + version +
					". Expected: " + VERSION + ".");
		}

		// Get the message type
		int msgType = buf.readInt();
		KvStateRequestType[] values = KvStateRequestType.values();
		if (msgType >= 0 && msgType <= values.length) {
			return values[msgType];
		} else {
			throw new IllegalArgumentException("Illegal message type with index " + msgType);
		}
	}

	/**
	 * Deserializes the KvState request message.
	 *
	 * <p><strong>Important</strong>: the returned buffer is sliced from the
	 * incoming ByteBuf stream and retained. Therefore, it needs to be recycled
	 * by the consumer.
	 *
	 * @param buf Buffer to deserialize (expected to be positioned after header)
	 * @return Deserialized KvStateRequest
	 */
	public static KvStateRequest deserializeKvStateRequest(ByteBuf buf) {
		long requestId = buf.readLong();
		KvStateID kvStateId = new KvStateID(buf.readLong(), buf.readLong());

		// Serialized key and namespace
		int length = buf.readInt();

		if (length < 0) {
			throw new IllegalArgumentException("Negative length for serialized key and namespace. " +
					"This indicates a serialization error.");
		}

		// Copy the buffer in order to be able to safely recycle the ByteBuf
		byte[] serializedKeyAndNamespace = new byte[length];
		if (length > 0) {
			buf.readBytes(serializedKeyAndNamespace);
		}

		return new KvStateRequest(requestId, kvStateId, serializedKeyAndNamespace);
	}

	/**
	 * Deserializes the KvState request result.
	 *
	 * @param buf Buffer to deserialize (expected to be positioned after header)
	 * @return Deserialized KvStateRequestResult
	 */
	public static KvStateRequestResult deserializeKvStateRequestResult(ByteBuf buf) {
		long requestId = buf.readLong();

		// Serialized KvState
		int length = buf.readInt();

		if (length < 0) {
			throw new IllegalArgumentException("Negative length for serialized result. " +
					"This indicates a serialization error.");
		}

		byte[] serializedValue = new byte[length];

		if (length > 0) {
			buf.readBytes(serializedValue);
		}

		return new KvStateRequestResult(requestId, serializedValue);
	}

	/**
	 * Deserializes the KvState request failure.
	 *
	 * @param buf Buffer to deserialize (expected to be positioned after header)
	 * @return Deserialized KvStateRequestFailure
	 */
	public static KvStateRequestFailure deserializeKvStateRequestFailure(ByteBuf buf) throws IOException, ClassNotFoundException {
		long requestId = buf.readLong();

		Throwable cause;
		try (ByteBufInputStream bbis = new ByteBufInputStream(buf);
				ObjectInputStream in = new ObjectInputStream(bbis)) {

			cause = (Throwable) in.readObject();
		}

		return new KvStateRequestFailure(requestId, cause);
	}

	/**
	 * Deserializes the KvState request failure.
	 *
	 * @param buf Buffer to deserialize (expected to be positioned after header)
	 * @return Deserialized KvStateRequestFailure
	 * @throws IOException            Serialization failure are forwarded
	 * @throws ClassNotFoundException If Exception type can not be loaded
	 */
	public static Throwable deserializeServerFailure(ByteBuf buf) throws IOException, ClassNotFoundException {
		try (ByteBufInputStream bbis = new ByteBufInputStream(buf);
				ObjectInputStream in = new ObjectInputStream(bbis)) {

			return (Throwable) in.readObject();
		}
	}

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
	 * @param serializedValue Serialized value of type List<T>
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
	 * @param serializedValue Serialized value of type Map<UK, UV>
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

	// ------------------------------------------------------------------------

	/**
	 * Helper for writing the header.
	 *
	 * @param buf         Buffer to serialize header into
	 * @param requestType Result type to serialize
	 */
	private static void writeHeader(ByteBuf buf, KvStateRequestType requestType) {
		buf.writeInt(VERSION);
		buf.writeInt(requestType.ordinal());
	}
}
