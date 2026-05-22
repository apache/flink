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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ChannelIOExecutorDrainSubmissionTest {

    @TempDir Path tempDir;

    @Test
    void testFilterOnSubmitsDrainAfterConversion() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        FetchedChannelState state = writeRecords(cInfo, new byte[] {1, 2, 3});

        CapturingChannel chan = new CapturingChannel(cInfo);
        List<RecoverableInputChannel> all = new ArrayList<>();
        all.add(chan);
        FetchedChannelStateDrainer drainer =
                new FetchedChannelStateDrainer(state, CompletableFuture.completedFuture(all));

        ExecutorService channelIOExecutor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> done = new CompletableFuture<>();
            channelIOExecutor.execute(
                    () -> {
                        try {
                            drainer.drain();
                            done.complete(null);
                        } catch (Throwable t) {
                            done.completeExceptionally(t);
                        } finally {
                            try {
                                drainer.close();
                            } catch (IOException ignore) {
                            }
                        }
                    });

            done.get(5, TimeUnit.SECONDS);
            assertThat(chan.dataDeliveries).isGreaterThan(0);
            assertThat(chan.finishCalled).isTrue();
        } finally {
            channelIOExecutor.shutdownNow();
            assertThat(channelIOExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testDrainExceptionBubblesViaAsyncExceptionHandler() throws Exception {
        InputChannelInfo cInfo = new InputChannelInfo(0, 0);
        FetchedChannelState state = writeRecords(cInfo, new byte[] {1, 2, 3});

        RecoverableInputChannel chan =
                new RecoverableInputChannel() {
                    @Override
                    public InputChannelInfo getChannelInfo() {
                        return cInfo;
                    }

                    @Override
                    public void onRecoveredStateBuffer(Buffer buffer) {
                        throw new RuntimeException("boom");
                    }

                    @Override
                    public void finishRecoveredBufferDelivery() {}

                    @Override
                    public void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId) {
                        throw new RuntimeException("boom");
                    }

                    @Override
                    public Buffer requestRecoveryBufferBlocking() {
                        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(64);
                        return new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
                    }

                    @Override
                    public void onRecoveredStateConsumed() {}
                };

        List<RecoverableInputChannel> all = new ArrayList<>();
        all.add(chan);
        FetchedChannelStateDrainer drainer =
                new FetchedChannelStateDrainer(state, CompletableFuture.completedFuture(all));

        CountDownLatch handlerCalled = new CountDownLatch(1);
        AtomicReference<Throwable> captured = new AtomicReference<>();
        ExecutorService channelIOExecutor = Executors.newSingleThreadExecutor();
        try {
            channelIOExecutor.execute(
                    () -> {
                        try {
                            drainer.drain();
                        } catch (Throwable t) {
                            captured.set(t);
                            handlerCalled.countDown();
                        } finally {
                            try {
                                drainer.close();
                            } catch (IOException ignore) {
                            }
                        }
                    });

            assertThat(handlerCalled.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(captured.get()).isInstanceOf(RuntimeException.class);
            assertThat(captured.get().getMessage()).isEqualTo("boom");
        } finally {
            channelIOExecutor.shutdownNow();
            assertThat(channelIOExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private FetchedChannelState writeRecords(InputChannelInfo ch, byte[] payload)
            throws IOException {
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, payload, payload.length);
            return writer.getChannelState();
        }
    }

    private static final class CapturingChannel implements RecoverableInputChannel {
        private final InputChannelInfo channelInfo;
        int dataDeliveries = 0;
        boolean finishCalled = false;

        CapturingChannel(InputChannelInfo channelInfo) {
            this.channelInfo = channelInfo;
        }

        @Override
        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        @Override
        public void onRecoveredStateBuffer(Buffer buffer) {
            if (buffer.isBuffer()) {
                dataDeliveries++;
            }
        }

        @Override
        public void finishRecoveredBufferDelivery() {
            finishCalled = true;
        }

        @Override
        public void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId) {}

        @Override
        public Buffer requestRecoveryBufferBlocking() {
            MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(64);
            return new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
        }

        @Override
        public void onRecoveredStateConsumed() {}
    }
}
