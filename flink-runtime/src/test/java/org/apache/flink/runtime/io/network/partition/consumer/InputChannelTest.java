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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link InputChannel}. */
class InputChannelTest {

    @Test
    void testExponentialBackoff() {
        InputChannel ch = createInputChannel(500, 4000);

        assertThat(ch.getCurrentBackoff()).isZero();

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(500);

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(1000);

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(2000);

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(4000);

        assertThat(ch.increaseBackoff()).isFalse();
        assertThat(ch.getCurrentBackoff()).isEqualTo(4000);
    }

    @Test
    void testExponentialBackoffCappedAtMax() {
        InputChannel ch = createInputChannel(500, 3000);

        assertThat(ch.getCurrentBackoff()).isZero();

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(500);

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(1000);

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(2000);

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(3000);

        assertThat(ch.increaseBackoff()).isFalse();
        assertThat(ch.getCurrentBackoff()).isEqualTo(3000);
    }

    @Test
    void testExponentialBackoffSingle() {
        InputChannel ch = createInputChannel(500, 500);

        assertThat(ch.getCurrentBackoff()).isZero();

        assertThat(ch.increaseBackoff()).isTrue();
        assertThat(ch.getCurrentBackoff()).isEqualTo(500);

        assertThat(ch.increaseBackoff()).isFalse();
        assertThat(ch.getCurrentBackoff()).isEqualTo(500);
    }

    @Test
    void testExponentialNoBackoff() {
        InputChannel ch = createInputChannel(0, 0);

        assertThat(ch.getCurrentBackoff()).isZero();

        assertThat(ch.increaseBackoff()).isFalse();
        assertThat(ch.getCurrentBackoff()).isZero();
    }

    private InputChannel createInputChannel(int initialBackoff, int maxBackoff) {
        return new MockInputChannel(
                mock(SingleInputGate.class),
                0,
                new ResultPartitionID(),
                initialBackoff,
                maxBackoff);
    }

    // ---------------------------------------------------------------------------------------------

    private static class MockInputChannel extends InputChannel {

        private MockInputChannel(
                SingleInputGate inputGate,
                int channelIndex,
                ResultPartitionID partitionId,
                int initialBackoff,
                int maxBackoff) {

            super(
                    inputGate,
                    channelIndex,
                    partitionId,
                    new ResultSubpartitionIndexSet(0),
                    initialBackoff,
                    maxBackoff,
                    new SimpleCounter(),
                    new SimpleCounter());
        }

        @Override
        public void resumeConsumption() {}

        @Override
        public void acknowledgeAllRecordsProcessed() throws IOException {}

        @Override
        void requestSubpartitions() throws IOException, InterruptedException {}

        @Override
        protected int peekNextBufferSubpartitionIdInternal() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<BufferAndAvailability> getNextBuffer()
                throws IOException, InterruptedException {
            return Optional.empty();
        }

        @Override
        void sendTaskEvent(TaskEvent event) throws IOException {}

        @Override
        boolean isReleased() {
            return false;
        }

        @Override
        void releaseAllResources() throws IOException {}

        @Override
        void announceBufferSize(int newBufferSize) {}

        @Override
        int getBuffersInUseCount() {
            return 0;
        }
    }
}
