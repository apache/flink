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
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SerializedValueSerializer} and {@link SerializedValueDeserializer}. */
public class SerializedValueSerializerTest extends TestLogger {

    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        objectMapper = new ObjectMapper();
        final SimpleModule simpleModule = new SimpleModule();
        final JavaType serializedValueWildcardType =
                objectMapper
                        .getTypeFactory()
                        .constructType(new TypeReference<SerializedValue<?>>() {});
        simpleModule.addSerializer(new SerializedValueSerializer(serializedValueWildcardType));
        simpleModule.addDeserializer(
                SerializedValue.class,
                new SerializedValueDeserializer(serializedValueWildcardType));
        objectMapper.registerModule(simpleModule);
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        final String json = objectMapper.writeValueAsString(new SerializedValue<>(new TestClass()));

        final SerializedValue<TestClass> serializedValue =
                objectMapper.readValue(json, new TypeReference<SerializedValue<TestClass>>() {});
        final TestClass deserializedValue =
                serializedValue.deserializeValue(ClassLoader.getSystemClassLoader());

        assertEquals("baz", deserializedValue.foo);
        assertEquals(1, deserializedValue.bar);
    }

    private static class TestClass implements Serializable {

        private static final long serialVersionUID = 1L;

        private String foo = "baz";

        private int bar = 1;
    }
}
