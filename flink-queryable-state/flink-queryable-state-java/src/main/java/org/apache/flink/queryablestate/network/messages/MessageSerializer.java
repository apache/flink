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

package org.apache.flink.queryablestate.network.messages;

import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateRequestFailure;
import org.apache.flink.queryablestate.messages.KvStateRequestResult;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Serialization and deserialization of messages exchanged between
 * {@link org.apache.flink.queryablestate.client.KvStateClient client} and
 * {@link org.apache.flink.queryablestate.server.KvStateServerImpl server}.
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
 * <p>The concrete content of a message depends on the {@link MessageType}.
 */
public final class MessageSerializer {

	/** The serialization version ID. */
	private static final int VERSION = 0x79a1b710;

	/** Byte length of the header. */
	private static final int HEADER_LENGTH = 2 * Integer.BYTES;

	/** Byte length of the request id. */
	private static final int REQUEST_ID_SIZE = Long.BYTES;

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
		int frameLength = HEADER_LENGTH + REQUEST_ID_SIZE + AbstractID.SIZE + (Integer.BYTES + serializedKeyAndNamespace.length);
		ByteBuf buf = alloc.ioBuffer(frameLength + 4); // +4 for frame length

		buf.writeInt(frameLength);

		writeHeader(buf, MessageType.REQUEST);

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
		int frameLength = HEADER_LENGTH + REQUEST_ID_SIZE + 4 + serializedResult.length;

		// TODO: 10/5/17 there was a bug all this time?
		ByteBuf buf = alloc.ioBuffer(frameLength + 4);

		buf.writeInt(frameLength);
		writeHeader(buf, MessageType.REQUEST_RESULT);
		buf.writeLong(requestId);

		buf.writeInt(serializedResult.length);
		buf.writeBytes(serializedResult);

		return buf;
	}

	/**
	 * Serializes the exception containing the failure message sent to the
	 * {@link org.apache.flink.queryablestate.client.KvStateClient} in case of
	 * protocol related errors.
	 *
	 * @param alloc			The {@link ByteBufAllocator} used to allocate the buffer to serialize the message into.
	 * @param requestId		The id of the request to which the message refers to.
	 * @param cause			The exception thrown at the server.
	 * @return A {@link ByteBuf} containing the serialized message.
	 */
	public static ByteBuf serializeKvStateRequestFailure(
			final ByteBufAllocator alloc,
			final long requestId,
			final Throwable cause) throws IOException {

		final ByteBuf buf = alloc.ioBuffer();

		// Frame length is set at the end
		buf.writeInt(0);
		writeHeader(buf, MessageType.REQUEST_FAILURE);
		buf.writeLong(requestId);

		try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
				ObjectOutput out = new ObjectOutputStream(bbos)) {
			out.writeObject(cause);
		}

		// Set frame length
		int frameLength = buf.readableBytes() - Integer.BYTES;
		buf.setInt(0, frameLength);
		return buf;
	}

	/**
	 * Serializes the failure message sent to the
	 * {@link org.apache.flink.queryablestate.client.KvStateClient} in case of
	 * server related errors.
	 *
	 * @param alloc			The {@link ByteBufAllocator} used to allocate the buffer to serialize the message into.
	 * @param cause			The exception thrown at the server.
	 * @return		The failure message.
	 */
	public static ByteBuf serializeServerFailure(
			final ByteBufAllocator alloc,
			final Throwable cause) throws IOException {

		final ByteBuf buf = alloc.ioBuffer();

		// Frame length is set at end
		buf.writeInt(0);
		writeHeader(buf, MessageType.SERVER_FAILURE);

		try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
				ObjectOutput out = new ObjectOutputStream(bbos)) {
			out.writeObject(cause);
		}

		// Set frame length
		int frameLength = buf.readableBytes() - Integer.BYTES;
		buf.setInt(0, frameLength);
		return buf;
	}

	/**
	 * Helper for serializing the header.
	 *
	 * @param buf         The {@link ByteBuf} to serialize the header into.
	 * @param messageType The {@link MessageType} of the message this header refers to.
	 */
	private static void writeHeader(final ByteBuf buf, final MessageType messageType) {
		buf.writeInt(VERSION);
		buf.writeInt(messageType.ordinal());
	}

	// ------------------------------------------------------------------------
	// Deserialization
	// ------------------------------------------------------------------------

	/**
	 * De-serializes the header and returns the {@link MessageType}.
	 * <pre>
	 *  <b>The buffer is expected to be at the header position.</b>
	 * </pre>
	 * @param buf						The {@link ByteBuf} containing the serialized header.
	 * @return							The message type.
	 * @throws IllegalStateException	If unexpected message version or message type.
	 */
	public static MessageType deserializeHeader(final ByteBuf buf) {

		// checking the version
		int version = buf.readInt();
		Preconditions.checkState(version == VERSION,
				"Version Mismatch:  Found " + version + ", Expected: " + VERSION + '.');

		// fetching the message type
		int msgType = buf.readInt();
		MessageType[] values = MessageType.values();
		Preconditions.checkState(msgType >= 0 && msgType <= values.length,
				"Illegal message type with index " + msgType + '.');
		return values[msgType];
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
	 * De-serializes the {@link KvStateRequestFailure} sent to the
	 * {@link org.apache.flink.queryablestate.client.KvStateClient} in case of
	 * protocol related errors.
	 * <pre>
	 *  <b>The buffer is expected to be at the correct position.</b>
	 * </pre>
	 * @param buf	The {@link ByteBuf} containing the serialized failure message.
	 * @return		The failure message.
	 */
	public static KvStateRequestFailure deserializeKvStateRequestFailure(final ByteBuf buf) throws IOException, ClassNotFoundException {
		long requestId = buf.readLong();

		Throwable cause;
		try (ByteBufInputStream bis = new ByteBufInputStream(buf);
				ObjectInputStream in = new ObjectInputStream(bis)) {
			cause = (Throwable) in.readObject();
		}
		return new KvStateRequestFailure(requestId, cause);
	}

	/**
	 * De-serializes the failure message sent to the
	 * {@link org.apache.flink.queryablestate.client.KvStateClient} in case of
	 * server related errors.
	 * <pre>
	 *  <b>The buffer is expected to be at the correct position.</b>
	 * </pre>
	 * @param buf	The {@link ByteBuf} containing the serialized failure message.
	 * @return		The failure message.
	 */
	public static Throwable deserializeServerFailure(final ByteBuf buf) throws IOException, ClassNotFoundException {
		try (ByteBufInputStream bis = new ByteBufInputStream(buf);
				ObjectInputStream in = new ObjectInputStream(bis)) {
			return (Throwable) in.readObject();
		}
	}
}
