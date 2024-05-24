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

package org.apache.flink.testutils.runtime;

import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link NoFetchingInput}. */
class NoFetchingInputTest {

    /**
     * Try deserializing an object whose serialization result is an empty byte array, and don't
     * expect any exceptions.
     */
    @Test
    void testDeserializeEmptyObject() {
        EmptyObjectSerializer emptyObjectSerializer = new EmptyObjectSerializer();

        Output output = new ByteBufferOutput(1000);
        emptyObjectSerializer.write(null, output, new Object());

        Input input = new NoFetchingInput(new ByteArrayInputStream(output.toBytes()));
        final Object deserialized = emptyObjectSerializer.read(null, input, Object.class);

        assertThat(deserialized).isExactlyInstanceOf(Object.class);
    }

    /** The serializer that serialize the empty object. */
    public static class EmptyObjectSerializer extends Serializer<Object> {

        @Override
        public void write(Kryo kryo, Output output, Object mes) {
            byte[] ser = new byte[0];
            output.writeInt(ser.length, true);
            output.writeBytes(ser);
        }

        @Override
        public Object read(Kryo kryo, Input input, Class<Object> pbClass) {
            try {
                int size = input.readInt(true);
                assertThat(size).isZero();

                // Try to read an empty byte array, and don't expect any exception.
                byte[] barr = new byte[size];
                input.readBytes(barr);

                return new Object();
            } catch (Exception e) {
                throw new RuntimeException("Could not create " + pbClass, e);
            }
        }
    }
}
