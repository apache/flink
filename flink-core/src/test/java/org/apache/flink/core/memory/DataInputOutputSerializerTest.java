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

package org.apache.flink.core.memory;

import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the combination of {@link DataOutputSerializer} and {@link DataInputDeserializer}. */
class DataInputOutputSerializerTest {

    @Test
    void testWrapAsByteBuffer() throws IOException {
        SerializationTestType randomInt = Util.randomRecord(SerializationTestTypeFactory.INT);

        DataOutputSerializer serializer = new DataOutputSerializer(randomInt.length());
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(randomInt.length());

        // empty buffer, read buffer should be empty
        ByteBuffer wrapper = serializer.wrapAsByteBuffer();

        assertThat(wrapper.position()).isZero();
        assertThat(wrapper.limit()).isZero();

        // write to data output, read buffer should still be empty
        randomInt.write(serializer);

        assertThat(wrapper.position()).isZero();
        assertThat(wrapper.limit()).isZero();

        // get updated read buffer, read buffer should contain written data
        wrapper = serializer.wrapAsByteBuffer();

        assertThat(wrapper.position()).isZero();
        assertThat(wrapper.limit()).isEqualTo(randomInt.length());

        // clear data output, read buffer should still contain written data
        serializer.clear();

        assertThat(wrapper.position()).isZero();
        assertThat(wrapper.limit()).isEqualTo(randomInt.length());

        // get updated read buffer, should be empty
        wrapper = serializer.wrapAsByteBuffer();

        assertThat(wrapper.position()).isZero();
        assertThat(wrapper.limit()).isZero();

        // write to data output and read back to memory
        randomInt.write(serializer);
        wrapper = serializer.wrapAsByteBuffer();

        segment.put(0, wrapper, randomInt.length());

        assertThat(wrapper.position()).isEqualTo(randomInt.length());
        assertThat(wrapper.limit()).isEqualTo(randomInt.length());
    }

    @Test
    void testRandomValuesWriteRead()
            throws IOException, InstantiationException, IllegalAccessException {
        final int numElements = 100000;
        final ArrayDeque<SerializationTestType> reference = new ArrayDeque<>();

        DataOutputSerializer serializer = new DataOutputSerializer(1);

        for (SerializationTestType value : Util.randomRecords(numElements)) {
            reference.add(value);

            value.write(serializer);
        }

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.wrapAsByteBuffer());

        for (SerializationTestType expected : reference) {
            SerializationTestType actual = expected.getClass().newInstance();
            actual.read(deserializer);

            assertThat(actual).isEqualTo(expected);
        }

        reference.clear();
    }

    @Test
    void testLongUTFWriteRead() throws IOException {
        byte[] array = new byte[1000];
        new Random(1).nextBytes(array);
        String expected = new String(array, Charset.forName("UTF-8"));

        DataOutputSerializer serializer = new DataOutputSerializer(1);
        serializer.writeLongUTF(expected);
        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getSharedBuffer());

        String actual = deserializer.readLongUTF();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testUTFWriteRead() throws IOException {
        byte[] array = new byte[1000];
        new Random(1).nextBytes(array);
        String expected = new String(array, StandardCharsets.UTF_8);

        DataOutputSerializer serializer = new DataOutputSerializer(1);
        serializer.writeUTF(expected);
        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getSharedBuffer());

        String actual = deserializer.readUTF();
        assertThat(actual).isEqualTo(expected);
    }
}
