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
import java.util.ArrayDeque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the combination of {@link DataOutputSerializer} and {@link DataInputDeserializer}. */
class DataInputOutputSerializerTest {

    @Test
    void testWrapAsByteBuffer() {
        SerializationTestType randomInt = Util.randomRecord(SerializationTestTypeFactory.INT);

        DataOutputSerializer serializer = new DataOutputSerializer(randomInt.length());
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(randomInt.length());

        try {
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
        } catch (IOException e) {
            e.printStackTrace();
            fail("Test encountered an unexpected exception.");
        }
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
}
