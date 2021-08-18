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

package org.apache.flink.api.common.serialization;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for the {@link SimpleStringSchema}. */
public class SimpleStringSchemaTest {

    private static SimpleStringSchema schemaDefault;
    private static SimpleStringSchema schemaUTF16BE;

    @BeforeClass
    public static void setup() {
        schemaDefault = new SimpleStringSchema();
        schemaUTF16BE = new SimpleStringSchema(StandardCharsets.UTF_16BE);
    }

    @Test
    public void testSerializability() throws Exception {
        final SimpleStringSchema copy = CommonTestUtils.createCopySerializable(schemaUTF16BE);

        assertEquals(schemaUTF16BE.getCharset(), copy.getCharset());
    }

    @Test
    public void testSerialization() {
        String original = "内容123456789qwertyuiop";
        byte[] serialized = schemaDefault.serialize(original);

        assertArrayEquals(original.getBytes(StandardCharsets.UTF_8), serialized);
    }

    @Test
    public void testSerializationWithAnotherCharset() {
        final Charset charset = StandardCharsets.UTF_16BE;
        final String string = "之掃描古籍版實乃姚鼐的";
        final byte[] bytes = string.getBytes(charset);

        assertArrayEquals(bytes, schemaUTF16BE.serialize(string));
        assertEquals(string, new SimpleStringSchema(charset).deserialize(bytes));
    }

    @Test
    public void testSerializeNullValue() {
        byte[] serialized = schemaDefault.serialize(null);

        assertEquals(serialized.length, 0);
    }

    @Test
    public void testDeserialization() {
        String original = "12345ABCDE";
        byte[] bytes = original.getBytes(StandardCharsets.UTF_8);
        String deserialized = schemaDefault.deserialize(bytes);

        assertEquals(deserialized, original);
    }

    @Test
    public void testDeserializeNullValue() {
        String deserializedString = schemaDefault.deserialize(null);

        assertNull(deserializedString);
    }
}
