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

package org.apache.flink.streaming.experimental;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.testutils.CheckedThread;

import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the SocketStreamIterator.
 *
 * <p>This experimental class is relocated from flink-streaming-contrib. Please see
 * package-info.java for more information.
 */
class SocketStreamIteratorTest {

    @Test
    void testIterator() throws Exception {

        final long seed = new Random().nextLong();
        final int numElements = 1000;

        final SocketStreamIterator<Long> iterator =
                new SocketStreamIterator<>(LongSerializer.INSTANCE);

        CheckedThread writer =
                new CheckedThread() {

                    @Override
                    public void go() throws Exception {
                        try (Socket sock =
                                        new Socket(iterator.getBindAddress(), iterator.getPort());
                                DataOutputViewStreamWrapper out =
                                        new DataOutputViewStreamWrapper(sock.getOutputStream())) {

                            final TypeSerializer<Long> serializer = LongSerializer.INSTANCE;
                            final Random rnd = new Random(seed);

                            for (int i = 0; i < numElements; i++) {
                                serializer.serialize(rnd.nextLong(), out);
                            }
                        }
                    }
                };

        writer.start();

        final Random validator = new Random(seed);
        for (int i = 0; i < numElements; i++) {
            assertThat(iterator).hasNext();
            assertThat(iterator).hasNext();
            assertThat(iterator.next()).isEqualTo(validator.nextLong());
        }

        assertThat(iterator).isExhausted();
        writer.sync();
        assertThat(iterator).isExhausted();
    }

    @Test
    void testIteratorWithException() throws Exception {

        final SocketStreamIterator<Long> iterator =
                new SocketStreamIterator<>(LongSerializer.INSTANCE);

        // asynchronously set an error
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
                iterator.notifyOfError(new Exception("test"));
            }
        }.start();

        assertThatThrownBy(iterator::hasNext)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("test");
    }
}
