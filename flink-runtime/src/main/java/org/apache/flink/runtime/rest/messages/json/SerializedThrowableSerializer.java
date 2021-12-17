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

    static final String FIELD_NAME_STACK_TRACE = "stack-trace";

    static final String FIELD_NAME_SERIALIZED_THROWABLE = "serialized-throwable";

    public SerializedThrowableSerializer() {
        super(SerializedThrowable.class);
    }

    @Override
    public void serialize(
            final SerializedThrowable value,
            final JsonGenerator gen,
            final SerializerProvider provider)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_CLASS, value.getOriginalErrorClassName());
        gen.writeStringField(FIELD_NAME_STACK_TRACE, value.getFullStringifiedStackTrace());
        gen.writeBinaryField(
                FIELD_NAME_SERIALIZED_THROWABLE, InstantiationUtil.serializeObject(value));
        gen.writeEndObject();
    }
}
