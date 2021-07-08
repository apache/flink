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

import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;

/** JSON deserializer for {@link SerializedValue}. */
public class SerializedValueDeserializer extends StdDeserializer<SerializedValue<?>> {

    private static final long serialVersionUID = 1L;

    public SerializedValueDeserializer() {
        super(
                TypeFactory.defaultInstance()
                        .constructType(new TypeReference<SerializedValue<Object>>() {}));
    }

    public SerializedValueDeserializer(final JavaType valueType) {
        super(valueType);
    }

    @Override
    public SerializedValue<?> deserialize(final JsonParser p, final DeserializationContext ctxt)
            throws IOException {
        return SerializedValue.fromBytes(p.getBinaryValue());
    }
}
