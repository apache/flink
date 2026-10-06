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

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.fs.FSDataInputStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the stream pool of {@link ByteBufferReadableFSDataInputStream}. */
class ByteBufferReadableFSDataInputStreamTest {

    /**
     * A stream taken from (or created for) the pool that fails while reading is neither returned to
     * the pool nor closed by the caller, so {@code readFully} has to close it itself — otherwise
     * the underlying (remote) stream leaks.
     */
    @Test
    void testPooledStreamIsClosedWhenPositionedReadFails() throws Exception {
        List<FailingStream> created = new ArrayList<>();
        ByteBufferReadableFSDataInputStream stream =
                new ByteBufferReadableFSDataInputStream(
                        () -> {
                            // The first stream is the sequential "original" stream of the wrapper;
                            // every further one is created for the positioned-read pool.
                            FailingStream s = new FailingStream(!created.isEmpty());
                            created.add(s);
                            return s;
                        },
                        4,
                        1024);

        assertThatThrownBy(() -> stream.readFully(10, ByteBuffer.allocate(8)))
                .isInstanceOf(IOException.class)
                .hasMessage("read failed");

        assertThat(created).hasSize(2);
        assertThat(created.get(1).closed).as("failed pooled stream must be closed").isTrue();
        assertThat(created.get(0).closed).as("the original stream is untouched").isFalse();
        stream.close();
    }

    private static final class FailingStream extends FSDataInputStream {
        private final boolean failOnRead;
        private long pos;
        volatile boolean closed;

        FailingStream(boolean failOnRead) {
            this.failOnRead = failOnRead;
        }

        @Override
        public void seek(long desired) {
            pos = desired;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() throws IOException {
            if (failOnRead) {
                throw new IOException("read failed");
            }
            pos++;
            return 0;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
