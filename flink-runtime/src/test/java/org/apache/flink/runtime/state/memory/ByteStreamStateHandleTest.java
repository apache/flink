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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.fs.FSDataInputStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ByteStreamStateHandle}. */
class ByteStreamStateHandleTest {

    @Test
    void testStreamSeekAndPos() throws IOException {
        final byte[] data = {34, 25, 22, 66, 88, 54};
        final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", data);

        // read backwards, one byte at a time
        for (int i = data.length; i >= 0; i--) {
            FSDataInputStream in = handle.openInputStream();
            in.seek(i);

            assertThat(in.getPos()).isEqualTo(i);

            if (i < data.length) {
                assertThat(in.read()).isEqualTo(data[i]);
                assertThat(in.getPos()).isEqualTo(i + 1);
            } else {
                assertThat(in.read()).isEqualTo(-1);
                assertThat(in.getPos()).isEqualTo(i);
            }
        }

        // reading past the end makes no difference
        FSDataInputStream in = handle.openInputStream();
        in.seek(data.length);

        // read multiple times, should not affect anything
        assertThat(in.read()).isEqualTo(-1);
        assertThat(in.read()).isEqualTo(-1);
        assertThat(in.read()).isEqualTo(-1);

        assertThat(in.getPos()).isEqualTo(data.length);
    }

    @Test
    void testStreamSeekOutOfBounds() {
        final int len = 10;
        final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", new byte[len]);

        // check negative offset
        assertThatThrownBy(() -> handle.openInputStream().seek(-2)).isInstanceOf(IOException.class);

        // check integer overflow
        assertThatThrownBy(() -> handle.openInputStream().seek(len + 1))
                .isInstanceOf(IOException.class);

        // check integer overflow
        assertThatThrownBy(() -> handle.openInputStream().seek(((long) Integer.MAX_VALUE) + 100L))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testBulkRead() throws IOException {
        final byte[] data = {34, 25, 22, 66};
        final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", data);
        final int targetLen = 8;

        for (int start = 0; start < data.length; start++) {
            for (int num = 0; num < targetLen; num++) {
                FSDataInputStream in = handle.openInputStream();
                in.seek(start);

                final byte[] target = new byte[targetLen];
                final int read = in.read(target, targetLen - num, num);

                assertThat(read).isEqualTo(Math.min(num, data.length - start));
                for (int i = 0; i < read; i++) {
                    assertThat(target[targetLen - num + i]).isEqualTo(data[start + i]);
                }

                int newPos = start + read;
                assertThat(in.getPos()).isEqualTo(newPos);
                assertThat(in.read()).isEqualTo(newPos < data.length ? data[newPos] : -1);
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testBulkReadINdexOutOfBounds() throws IOException {
        final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", new byte[10]);

        // check negative offset
        assertThatThrownBy(() -> handle.openInputStream().read(new byte[10], -1, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // check offset overflow
        assertThatThrownBy(() -> handle.openInputStream().read(new byte[10], 10, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // check negative length
        assertThatThrownBy(() -> handle.openInputStream().read(new byte[10], 0, -2))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // check length too large
        assertThatThrownBy(() -> handle.openInputStream().read(new byte[10], 5, 6))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // check length integer overflow
        assertThatThrownBy(() -> handle.openInputStream().read(new byte[10], 5, Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testStreamWithEmptyByteArray() throws IOException {
        final byte[] data = new byte[0];
        final ByteStreamStateHandle handle = new ByteStreamStateHandle("name", data);

        try (FSDataInputStream in = handle.openInputStream()) {
            byte[] dataGot = new byte[1];
            assertThat(in.read(dataGot, 0, 0)).isZero(); // got return 0 because len == 0.
            assertThat(in.read()).isEqualTo(-1); // got -1 because of EOF.
        }
    }
}
