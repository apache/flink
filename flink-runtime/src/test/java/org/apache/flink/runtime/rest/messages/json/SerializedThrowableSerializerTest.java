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
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
