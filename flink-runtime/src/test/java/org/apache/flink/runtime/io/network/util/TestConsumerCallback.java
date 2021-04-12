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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public interface TestConsumerCallback {

    void onBuffer(Buffer buffer);

    void onEvent(AbstractEvent event);

    public static class CountingCallback implements TestConsumerCallback {

        private final AtomicInteger numberOfReadBuffers = new AtomicInteger();

        private final AtomicInteger numberOfReadEvents = new AtomicInteger();

        @Override
        public void onBuffer(Buffer buffer) {
            numberOfReadBuffers.incrementAndGet();
        }

        @Override
        public void onEvent(AbstractEvent event) {
            numberOfReadEvents.incrementAndGet();
        }

        /** Returns the number of read buffers. */
        public int getNumberOfReadBuffers() {
            return numberOfReadBuffers.get();
        }

        /** Returns the number of read events; */
        public int getNumberOfReadEvents() {
            return numberOfReadEvents.get();
        }
    }

    public static class RecyclingCallback extends CountingCallback {

        @Override
        public void onBuffer(Buffer buffer) {
            super.onBuffer(buffer);

            buffer.recycleBuffer();
        }

        @Override
        public void onEvent(AbstractEvent event) {
            super.onEvent(event);
        }
    }

    public class VerifyAscendingCallback extends RecyclingCallback {

        @Override
        public void onBuffer(Buffer buffer) {
            final MemorySegment segment = buffer.getMemorySegment();

            int expected = getNumberOfReadBuffers() * (segment.size() / 4);

            for (int i = 0; i < segment.size(); i += 4) {
                assertEquals(expected, segment.getInt(i));

                expected++;
            }

            super.onBuffer(buffer);
        }

        @Override
        public void onEvent(AbstractEvent event) {
            super.onEvent(event);
        }
    }
}
