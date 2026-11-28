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

import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

import static org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer.FIELD_NAME_CAUSE;
import static org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer.FIELD_NAME_CLASS;
import static org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer.FIELD_NAME_MESSAGE;
import static org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer.FIELD_NAME_SERIALIZED_THROWABLE;
import static org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer.FIELD_NAME_STACK_TRACE;
import static org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer.FIELD_NAME_SUPPRESSED;

/** JSON deserializer for {@link SerializedThrowable}. */
public class SerializedThrowableDeserializer extends StdDeserializer<SerializedThrowable> {

    private static final long serialVersionUID = 1L;

    public SerializedThrowableDeserializer() {
        super(SerializedThrowable.class);
    }

    /**
     * Caps how deep {@link #readThrowable} recurses into nested {@code cause}/{@code suppressed}
     * fields. Without this, an unexpectedly deeply-nested {@code cause} chain in a response could
     * exhaust the parsing thread's stack before Jackson's own tree-depth limits necessarily kick
     * in, turning parsing itself into a source of instability. 100 is far beyond any real
     * exception's cause/suppressed depth.
     */
    static final int MAX_THROWABLE_DEPTH = 100;

    @Override
    public SerializedThrowable deserialize(final JsonParser p, final DeserializationContext ctxt)
            throws IOException {
        return readThrowable(p.readValueAsTree(), 0);
    }

    /**
     * Recursively reconstructs a {@link SerializedThrowable} - and its cause and suppressed
     * exceptions - from the plain-string {@code class}/{@code message}/{@code stack-trace} fields
     * and the raw {@code serialized-throwable} bytes, without ever passing those bytes to {@code
     * InstantiationUtil.deserializeObject()}. Deserializing the original exception object only
     * happens lazily, on an explicit {@link SerializedThrowable#deserializeError} call, so that
     * parsing a response never runs an {@code ObjectInputStream.readObject()} over bytes this
     * process has not otherwise validated.
     */
    private static SerializedThrowable readThrowable(final JsonNode node, final int depth)
            throws IOException {
        if (node == null || node.isNull()) {
            return null;
        }
        if (depth >= MAX_THROWABLE_DEPTH) {
            throw new IOException(
                    "Refusing to deserialize a "
                            + SerializedThrowable.class.getCanonicalName()
                            + " nested more than "
                            + MAX_THROWABLE_DEPTH
                            + " levels deep");
        }

        final JsonNode classNode = node.get(FIELD_NAME_CLASS);
        final JsonNode messageNode = node.get(FIELD_NAME_MESSAGE);
        final JsonNode stackTraceNode = node.get(FIELD_NAME_STACK_TRACE);
        final JsonNode serializedNode = node.get(FIELD_NAME_SERIALIZED_THROWABLE);

        final SerializedThrowable throwable =
                new SerializedThrowable(
                        messageNode != null ? messageNode.asText() : null,
                        classNode != null ? classNode.asText() : null,
                        stackTraceNode != null ? stackTraceNode.asText() : null,
                        serializedNode != null ? serializedNode.binaryValue() : null);

        final SerializedThrowable cause = readThrowable(node.get(FIELD_NAME_CAUSE), depth + 1);
        if (cause != null) {
            throwable.initCause(cause);
        }

        final JsonNode suppressedNode = node.get(FIELD_NAME_SUPPRESSED);
        if (suppressedNode != null) {
            for (JsonNode s : suppressedNode) {
                final SerializedThrowable suppressed = readThrowable(s, depth + 1);
                if (suppressed != null) {
                    throwable.addSuppressed(suppressed);
                }
            }
        }

        return throwable;
    }
}
