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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/** Tests for {@link ByteArrayInputStreamWithPos}. */
class ByteArrayInputStreamWithPosTest {

    private final byte[] data = new byte[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private final ByteArrayInputStreamWithPos stream = new ByteArrayInputStreamWithPos(data);

    @Test
    void testGetWithNullArray() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> stream.read(null, 0, 1));
    }

    @Test
    void testGetWithNegativeLength() {
        int read = stream.read(new byte[0], 0, -1);
        assertThat(read).isZero();
    }

    @Test
    void testGetWithTargetArrayOverflow() {
        assertThatExceptionOfType(IndexOutOfBoundsException.class)
                .isThrownBy(() -> stream.read(new byte[0], 0, 2));
    }

    @Test
    void testGetWithEOF() {
        drainStream(stream);
        int read = stream.read(new byte[1], 0, 1);
        assertThat(read).isEqualTo(-1);
    }

    @Test
    void testGetMoreThanAvailable() {
        int read = stream.read(new byte[20], 0, 20);
        assertThat(read).isEqualTo(10);
        assertThat(stream.read()).isEqualTo(-1); // exhausted now
    }

    /** Test setting position on a {@link ByteArrayInputStreamWithPos}. */
    @Test
    void testSetPosition() throws Exception {
        assertThat(stream.available()).isEqualTo(data.length);
        assertThat(stream.read()).isEqualTo('0');

        stream.setPosition(1);
        assertThat(stream.available()).isEqualTo(data.length - 1);
        assertThat(stream.read()).isEqualTo('1');

        stream.setPosition(3);
        assertThat(stream.available()).isEqualTo(data.length - 3);
        assertThat(stream.read()).isEqualTo('3');

        stream.setPosition(data.length);
        assertThat(stream.available()).isZero();
        assertThat(stream.read()).isEqualTo(-1);
    }

    /** Test that the expected position exceeds the capacity of the byte array. */
    @Test
    void testSetTooLargePosition() throws Exception {
        assertThatThrownBy(() -> stream.setPosition(data.length + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Position out of bounds.");
    }

    /** Test setting a negative position. */
    @Test
    void testSetNegativePosition() throws Exception {
        assertThatThrownBy(() -> stream.setPosition(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Position out of bounds.");
    }

    @Test
    void testSetBuffer() {
        ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos();
        assertThat(in.read()).isEqualTo(-1);
        byte[] testData = new byte[] {0x42, 0x43, 0x44, 0x45};
        int off = 1;
        int len = 2;
        in.setBuffer(testData, off, len);
        for (int i = 0; i < len; ++i) {
            assertThat(in.read()).isEqualTo(testData[i + off]);
        }
        assertThat(in.read()).isEqualTo(-1);
    }

    private static int drainStream(ByteArrayInputStreamWithPos stream) {
        int skipped = 0;
        while (stream.read() != -1) {
            skipped++;
        }
        return skipped;
    }
}
