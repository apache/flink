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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/** JSON serializer for {@link SerializedThrowable}. */
public class SerializedThrowableSerializer extends StdSerializer<SerializedThrowable> {

    private static final long serialVersionUID = 1L;

    static final String FIELD_NAME_CLASS = "class";

    static final String FIELD_NAME_MESSAGE = "message";

    static final String FIELD_NAME_STACK_TRACE = "stack-trace";

    public static final String FIELD_NAME_SERIALIZED_THROWABLE = "serialized-throwable";

    static final String FIELD_NAME_CAUSE = "cause";

    static final String FIELD_NAME_SUPPRESSED = "suppressed";

    public SerializedThrowableSerializer() {
        super(SerializedThrowable.class);
    }

    @Override
    public void serialize(
            final SerializedThrowable value,
            final JsonGenerator gen,
            final SerializerProvider provider)
            throws IOException {
        writeThrowable(value, gen);
    }

    /**
     * Writes a {@link SerializedThrowable} - and, recursively, its cause and suppressed exceptions
     * - using only fields that are safe to reconstruct without deserializing {@code
     * serialized-throwable} (see {@link SerializedThrowableDeserializer}). The binary blob is still
     * included for backward compatibility with old readers.
     */
    private static void writeThrowable(final SerializedThrowable value, final JsonGenerator gen)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_CLASS, value.getOriginalErrorClassName());
        if (value.getMessage() != null) {
            gen.writeStringField(FIELD_NAME_MESSAGE, value.getMessage());
        }
        gen.writeStringField(FIELD_NAME_STACK_TRACE, value.getFullStringifiedStackTrace());
        // Kept for backward compatibility with old readers: serializes the whole wrapper (not
        // just value.getSerializedException()), matching InstantiationUtil.deserializeObject()'s
        // old contract of returning a fully-populated SerializedThrowable.
        gen.writeBinaryField(
                FIELD_NAME_SERIALIZED_THROWABLE, InstantiationUtil.serializeObject(value));

        final Throwable cause = value.getCause();
        if (cause != null) {
            gen.writeFieldName(FIELD_NAME_CAUSE);
            writeThrowable(asSerializedThrowable(cause), gen);
        }

        final Throwable[] suppressed = value.getSuppressed();
        if (suppressed.length > 0) {
            gen.writeArrayFieldStart(FIELD_NAME_SUPPRESSED);
            for (Throwable s : suppressed) {
                writeThrowable(asSerializedThrowable(s), gen);
            }
            gen.writeEndArray();
        }

        gen.writeEndObject();
    }

    private static SerializedThrowable asSerializedThrowable(Throwable t) {
        return t instanceof SerializedThrowable
                ? (SerializedThrowable) t
                : new SerializedThrowable(t);
    }
}
