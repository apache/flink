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

import org.apache.flink.configuration.ConfigConstants;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ByteArrayOutputStreamWithPos}. */
class ByteArrayOutputStreamWithPosTest {

    private static final int BUFFER_SIZE = 32;

    private ByteArrayOutputStreamWithPos stream;

    @BeforeEach
    void setup() {
        stream = new ByteArrayOutputStreamWithPos(BUFFER_SIZE);
    }

    /** Test setting position which is exactly the same with the buffer size. */
    @Test
    void testSetPositionWhenBufferIsFull() throws Exception {
        stream.write(new byte[BUFFER_SIZE]);

        // check whether the buffer is filled fully
        assertThat(stream.getBuf()).hasSize(BUFFER_SIZE);

        // check current position is the end of the buffer
        assertThat(stream.getPosition()).isEqualTo(BUFFER_SIZE);

        stream.setPosition(BUFFER_SIZE);

        // confirm current position is at where we expect.
        assertThat(stream.getPosition()).isEqualTo(BUFFER_SIZE);
    }

    /** Test setting negative position. */
    @Test
    void testSetNegativePosition() {
        assertThatThrownBy(
                        () -> {
                            stream.write(new byte[BUFFER_SIZE]);
                            stream.setPosition(-1);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Position out of bounds");
    }

    /** Test setting position larger than buffer size. */
    @Test
    void testSetPositionLargerThanBufferSize() throws Exception {
        // fully fill the buffer
        stream.write(new byte[BUFFER_SIZE]);
        assertThat(stream.getBuf()).hasSize(BUFFER_SIZE);

        // expand the buffer by setting position beyond the buffer length
        stream.setPosition(BUFFER_SIZE + 1);
        assertThat(stream.getBuf()).hasSize(BUFFER_SIZE * 2);
        assertThat(stream.getPosition()).isEqualTo(BUFFER_SIZE + 1);
    }

    /** Test that toString returns a substring of the buffer with range(0, position). */
    @Test
    void testToString() throws IOException {
        byte[] data = "1234567890".getBytes(ConfigConstants.DEFAULT_CHARSET);

        try (ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos(data.length)) {

            stream.write(data);
            assertThat(stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET))
                    .containsExactly(data);

            for (int i = 0; i < data.length; i++) {
                stream.setPosition(i);
                assertThat(stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET))
                        .containsExactly(Arrays.copyOf(data, i));
            }

            // validate that the stored bytes are still tracked properly even when expanding array
            stream.setPosition(data.length + 1);
            assertThat(stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET))
                    .containsExactly(Arrays.copyOf(data, data.length + 1));
        }
    }
}
