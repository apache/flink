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

package org.apache.flink.runtime.rest.messages.json;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link SerializedThrowableSerializer} and {@link SerializedThrowableDeserializer}. */
class SerializedThrowableSerializerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(
                SerializedThrowable.class, new SerializedThrowableDeserializer());
        simpleModule.addSerializer(SerializedThrowable.class, new SerializedThrowableSerializer());

        objectMapper = JacksonMapperFactory.createObjectMapper();
        objectMapper.registerModule(simpleModule);
    }

    @Test
    void testSerializationDeserialization() throws Exception {
        Exception cause = new Exception("cause");
        Exception root = new Exception("message", cause);
        Exception suppressed = new Exception("suppressed");
        root.addSuppressed(suppressed);

        final SerializedThrowable serializedThrowable = new SerializedThrowable(root);

        final String json = objectMapper.writeValueAsString(serializedThrowable);
        final SerializedThrowable deserializedSerializedThrowable =
                objectMapper.readValue(json, SerializedThrowable.class);

        assertThat(deserializedSerializedThrowable.getMessage())
                .isEqualTo("java.lang.Exception: message");
        assertThat(serializedThrowable.getFullStringifiedStackTrace())
                .isEqualTo(deserializedSerializedThrowable.getFullStringifiedStackTrace());
        assertThat(deserializedSerializedThrowable.getCause().getMessage())
                .isEqualTo("java.lang.Exception: cause");
        assertThat(deserializedSerializedThrowable.getCause())
                .isInstanceOf(SerializedThrowable.class);
        assertThat(deserializedSerializedThrowable.getSuppressed().length).isOne();
        assertThat(deserializedSerializedThrowable.getSuppressed()[0].getMessage())
                .isEqualTo("java.lang.Exception: suppressed");
        assertThat(deserializedSerializedThrowable.getSuppressed()[0])
                .isInstanceOf(SerializedThrowable.class);
    }

    @Test
    void testDeserializationNeverRunsObjectInputStreamOverSerializedThrowableBytes()
            throws Exception {
        // Not a valid Java serialization stream - if the deserializer ever passed this to
        // InstantiationUtil.deserializeObject()/ObjectInputStream.readObject() at parse time,
        // that call would throw. Parsing must succeed regardless of what's in this field.
        final String garbageBase64 =
                Base64.getEncoder().encodeToString("not-a-serialized-object".getBytes());
        final String json =
                "{\"class\":\"java.lang.Exception\",\"message\":\"boom\","
                        + "\"stack-trace\":\"java.lang.Exception: boom\","
                        + "\"serialized-throwable\":\""
                        + garbageBase64
                        + "\"}";

        final SerializedThrowable[] deserialized = new SerializedThrowable[1];
        assertThatCode(
                        () ->
                                deserialized[0] =
                                        objectMapper.readValue(json, SerializedThrowable.class))
                .doesNotThrowAnyException();

        assertThat(deserialized[0].getMessage()).isEqualTo("boom");
        assertThat(deserialized[0].getOriginalErrorClassName()).isEqualTo("java.lang.Exception");

        // Only an explicit deserializeError() call touches the bytes, and it degrades gracefully
        // (returns itself as a stand-in) instead of propagating a deserialization failure.
        assertThat(deserialized[0].deserializeError(ClassLoader.getSystemClassLoader()))
                .isSameAs(deserialized[0]);
    }

    /**
     * Proves backward compatibility: an old, pre-fix client - which only ever reads the {@code
     * serialized-throwable} field and runs it straight through {@code
     * InstantiationUtil.deserializeObject()} - must still fully recover message/cause/suppressed
     * when talking to this (patched) serializer. The helper below reproduces that old
     * deserializer's exact logic (the code this fix removed from {@link
     * SerializedThrowableDeserializer}), not an approximation of it, so this test fails if a future
     * change ever stops writing the full-wrapper blob into that field for compatibility.
     *
     * <p>See {@link #newDeserializerDegradesGracefullyAgainstOldShapedResponse} directly below for
     * the other rolling-upgrade direction.
     */
    @Test
    void oldClientLogicFullyRecoversCauseAndSuppressedFromNewSerializer() throws Exception {
        Exception cause = new Exception("cause");
        Exception root = new Exception("message", cause);
        Exception suppressed = new Exception("suppressed");
        root.addSuppressed(suppressed);

        final SerializedThrowable serializedThrowable = new SerializedThrowable(root);
        final String json = objectMapper.writeValueAsString(serializedThrowable);

        final SerializedThrowable oldClientResult =
                oldClientDeserialize(objectMapper.readTree(json));

        assertThat(oldClientResult.getMessage()).isEqualTo("java.lang.Exception: message");
        assertThat(oldClientResult.getCause()).isInstanceOf(SerializedThrowable.class);
        assertThat(oldClientResult.getCause().getMessage()).isEqualTo("java.lang.Exception: cause");
        assertThat(oldClientResult.getSuppressed()).hasSize(1);
        assertThat(oldClientResult.getSuppressed()[0].getMessage())
                .isEqualTo("java.lang.Exception: suppressed");
    }

    /** Byte-for-byte the old (pre-fix) {@code SerializedThrowableDeserializer.deserialize()}. */
    private static SerializedThrowable oldClientDeserialize(JsonNode root) throws Exception {
        final byte[] serializedException =
                root.get(SerializedThrowableSerializer.FIELD_NAME_SERIALIZED_THROWABLE)
                        .binaryValue();
        return InstantiationUtil.deserializeObject(
                serializedException, ClassLoader.getSystemClassLoader());
    }

    /**
     * The other rolling-upgrade direction from {@link
     * #oldClientLogicFullyRecoversCauseAndSuppressedFromNewSerializer}: this (patched) deserializer
     * talking to an old, pre-fix server's response, which never had {@code message}/{@code
     * cause}/{@code suppressed} fields - only {@code class}/{@code stack-trace}/{@code
     * serialized-throwable}. Parsing must still succeed; message/cause are simply unavailable until
     * an explicit {@link SerializedThrowable#deserializeError} call, since that data only exists
     * inside the (unparsed) blob on an old server.
     */
    @Test
    void newDeserializerDegradesGracefullyAgainstOldShapedResponse() throws Exception {
        final Exception cause = new Exception("cause");
        final Exception original = new Exception("message", cause);
        final SerializedThrowable serializedThrowable = new SerializedThrowable(original);
        final byte[] oldStyleBlob = InstantiationUtil.serializeObject(serializedThrowable);

        final String json =
                "{\"class\":\"java.lang.Exception\","
                        + "\"stack-trace\":\"java.lang.Exception: message\","
                        + "\"serialized-throwable\":\""
                        + Base64.getEncoder().encodeToString(oldStyleBlob)
                        + "\"}";

        final SerializedThrowable[] deserialized = new SerializedThrowable[1];
        assertThatCode(
                        () ->
                                deserialized[0] =
                                        objectMapper.readValue(json, SerializedThrowable.class))
                .doesNotThrowAnyException();

        // The known degradation: no message/cause field on the wire means none available yet.
        assertThat(deserialized[0].getMessage()).isNull();
        assertThat(deserialized[0].getCause()).isNull();
        assertThat(deserialized[0].getOriginalErrorClassName()).isEqualTo("java.lang.Exception");

        // The escape hatch still works: an explicit deserializeError() call recovers everything
        // an old client would have had, straight from the still-present binary blob.
        final Throwable recovered =
                deserialized[0].deserializeError(ClassLoader.getSystemClassLoader());
        assertThat(recovered.getMessage()).isEqualTo("java.lang.Exception: message");
        assertThat(recovered.getCause()).isInstanceOf(SerializedThrowable.class);
        assertThat(recovered.getCause().getMessage()).isEqualTo("java.lang.Exception: cause");
    }

    /**
     * The reconstructing constructor must not let {@code super(message)}'s default stack-trace
     * capture leak the deserializer's own call stack into {@link
     * SerializedThrowable#getStackTrace()} - the original exception's stack trace is only
     * meaningfully available as text, via {@link
     * SerializedThrowable#getFullStringifiedStackTrace()}.
     */
    @Test
    void deserializedThrowableDoesNotExposeDeserializersOwnStackTrace() throws Exception {
        final String json =
                "{\"class\":\"java.lang.Exception\",\"message\":\"boom\","
                        + "\"stack-trace\":\"java.lang.Exception: boom\"}";

        final SerializedThrowable deserialized =
                objectMapper.readValue(json, SerializedThrowable.class);

        assertThat(deserialized.getStackTrace()).isEmpty();
    }

    /**
     * A {@code cause} chain nested one level past {@code SerializedThrowableDeserializer}'s depth
     * cap must fail parsing with an {@link java.io.IOException}, not exhaust the parsing thread's
     * stack. Without an explicit cap, a response with an arbitrarily deep {@code cause} chain
     * would turn parsing itself into a source of instability (a stack overflow).
     */
    @Test
    void causeChainNestedPastTheDepthCapFailsParsingInsteadOfOverflowingTheStack() {
        final int nestingLevels = SerializedThrowableDeserializer.MAX_THROWABLE_DEPTH + 1;
        final StringBuilder json = new StringBuilder("{\"class\":\"java.lang.Exception\"");
        for (int i = 0; i < nestingLevels; i++) {
            json.append(",\"cause\":{\"class\":\"java.lang.Exception\"");
        }
        for (int i = 0; i < nestingLevels; i++) {
            json.append('}');
        }
        json.append('}');

        assertThatCode(() -> objectMapper.readValue(json.toString(), SerializedThrowable.class))
                .isInstanceOf(java.io.IOException.class);
    }
}
