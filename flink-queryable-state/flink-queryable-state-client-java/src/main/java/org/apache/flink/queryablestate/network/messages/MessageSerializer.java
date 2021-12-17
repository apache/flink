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

import org.apache.flink.annotation.Internal;
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
 * Serialization and deserialization of messages exchanged between {@link
 * org.apache.flink.queryablestate.network.Client client} and {@link
 * org.apache.flink.queryablestate.network.AbstractServerBase server}.
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
 *
 * @param <REQ> Type of the requests of the protocol.
 * @param <RESP> Type of the responses of the protocol.
 */
@Internal
public final class MessageSerializer<REQ extends MessageBody, RESP extends MessageBody> {

    /** The serialization version ID. */
    private static final int VERSION = 0x79a1b710;

    /** Byte length of the header. */
    private static final int HEADER_LENGTH = 2 * Integer.BYTES;

    /** Byte length of the request id. */
    private static final int REQUEST_ID_SIZE = Long.BYTES;

    /** The constructor of the {@link MessageBody client requests}. Used for deserialization. */
    private final MessageDeserializer<REQ> requestDeserializer;

    /** The constructor of the {@link MessageBody server responses}. Used for deserialization. */
    private final MessageDeserializer<RESP> responseDeserializer;

    public MessageSerializer(
            MessageDeserializer<REQ> requestDeser, MessageDeserializer<RESP> responseDeser) {
        requestDeserializer = Preconditions.checkNotNull(requestDeser);
        responseDeserializer = Preconditions.checkNotNull(responseDeser);
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    /**
     * Serializes the request sent to the {@link
     * org.apache.flink.queryablestate.network.AbstractServerBase}.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param request The request to be serialized.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    public static <REQ extends MessageBody> ByteBuf serializeRequest(
            final ByteBufAllocator alloc, final long requestId, final REQ request) {
        Preconditions.checkNotNull(request);
        return writePayload(alloc, requestId, MessageType.REQUEST, request.serialize());
    }

    /**
     * Serializes the response sent to the {@link org.apache.flink.queryablestate.network.Client}.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param response The response to be serialized.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    public static <RESP extends MessageBody> ByteBuf serializeResponse(
            final ByteBufAllocator alloc, final long requestId, final RESP response) {
        Preconditions.checkNotNull(response);
        return writePayload(alloc, requestId, MessageType.REQUEST_RESULT, response.serialize());
    }

    /**
     * Serializes the exception containing the failure message sent to the {@link
     * org.apache.flink.queryablestate.network.Client} in case of protocol related errors.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param cause The exception thrown at the server.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    public static ByteBuf serializeRequestFailure(
            final ByteBufAllocator alloc, final long requestId, final Throwable cause)
            throws IOException {

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
     * Serializes the failure message sent to the {@link
     * org.apache.flink.queryablestate.network.Client} in case of server related errors.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param cause The exception thrown at the server.
     * @return The failure message.
     */
    public static ByteBuf serializeServerFailure(
            final ByteBufAllocator alloc, final Throwable cause) throws IOException {

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
     * @param buf The {@link ByteBuf} to serialize the header into.
     * @param messageType The {@link MessageType} of the message this header refers to.
     */
    private static void writeHeader(final ByteBuf buf, final MessageType messageType) {
        buf.writeInt(VERSION);
        buf.writeInt(messageType.ordinal());
    }

    /**
     * Helper for serializing the messages.
     *
     * @param alloc The {@link ByteBufAllocator} used to allocate the buffer to serialize the
     *     message into.
     * @param requestId The id of the request to which the message refers to.
     * @param messageType The {@link MessageType type of the message}.
     * @param payload The serialized version of the message.
     * @return A {@link ByteBuf} containing the serialized message.
     */
    private static ByteBuf writePayload(
            final ByteBufAllocator alloc,
            final long requestId,
            final MessageType messageType,
            final byte[] payload) {

        final int frameLength = HEADER_LENGTH + REQUEST_ID_SIZE + payload.length;
        final ByteBuf buf = alloc.ioBuffer(frameLength + Integer.BYTES);

        buf.writeInt(frameLength);
        writeHeader(buf, messageType);
        buf.writeLong(requestId);
        buf.writeBytes(payload);
        return buf;
    }

    // ------------------------------------------------------------------------
    // Deserialization
    // ------------------------------------------------------------------------

    /**
     * De-serializes the header and returns the {@link MessageType}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the header position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized header.
     * @return The message type.
     * @throws IllegalStateException If unexpected message version or message type.
     */
    public static MessageType deserializeHeader(final ByteBuf buf) {

        // checking the version
        int version = buf.readInt();
        Preconditions.checkState(
                version == VERSION,
                "Version Mismatch:  Found " + version + ", Expected: " + VERSION + '.');

        // fetching the message type
        int msgType = buf.readInt();
        MessageType[] values = MessageType.values();
        Preconditions.checkState(
                msgType >= 0 && msgType < values.length,
                "Illegal message type with index " + msgType + '.');
        return values[msgType];
    }

    /**
     * De-serializes the header and returns the {@link MessageType}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the request id position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized request id.
     * @return The request id.
     */
    public static long getRequestId(final ByteBuf buf) {
        return buf.readLong();
    }

    /**
     * De-serializes the request sent to the {@link
     * org.apache.flink.queryablestate.network.AbstractServerBase}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the request position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized request.
     * @return The request.
     */
    public REQ deserializeRequest(final ByteBuf buf) {
        Preconditions.checkNotNull(buf);
        return requestDeserializer.deserializeMessage(buf);
    }

    /**
     * De-serializes the response sent to the {@link
     * org.apache.flink.queryablestate.network.Client}.
     *
     * <pre>
     *  <b>The buffer is expected to be at the response position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized response.
     * @return The response.
     */
    public RESP deserializeResponse(final ByteBuf buf) {
        Preconditions.checkNotNull(buf);
        return responseDeserializer.deserializeMessage(buf);
    }

    /**
     * De-serializes the {@link RequestFailure} sent to the {@link
     * org.apache.flink.queryablestate.network.Client} in case of protocol related errors.
     *
     * <pre>
     *  <b>The buffer is expected to be at the correct position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized failure message.
     * @return The failure message.
     */
    public static RequestFailure deserializeRequestFailure(final ByteBuf buf)
            throws IOException, ClassNotFoundException {
        long requestId = buf.readLong();

        Throwable cause;
        try (ByteBufInputStream bis = new ByteBufInputStream(buf);
                ObjectInputStream in = new ObjectInputStream(bis)) {
            cause = (Throwable) in.readObject();
        }
        return new RequestFailure(requestId, cause);
    }

    /**
     * De-serializes the failure message sent to the {@link
     * org.apache.flink.queryablestate.network.Client} in case of server related errors.
     *
     * <pre>
     *  <b>The buffer is expected to be at the correct position.</b>
     * </pre>
     *
     * @param buf The {@link ByteBuf} containing the serialized failure message.
     * @return The failure message.
     */
    public static Throwable deserializeServerFailure(final ByteBuf buf)
            throws IOException, ClassNotFoundException {
        try (ByteBufInputStream bis = new ByteBufInputStream(buf);
                ObjectInputStream in = new ObjectInputStream(bis)) {
            return (Throwable) in.readObject();
        }
    }
}
