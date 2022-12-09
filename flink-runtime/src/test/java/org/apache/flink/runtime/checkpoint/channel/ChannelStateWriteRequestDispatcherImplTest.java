/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.apache.flink.runtime.state.ChannelPersistenceITCase.getStreamFactoryFactory;
import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link ChannelStateWriteRequestDispatcherImpl} test. */
class ChannelStateWriteRequestDispatcherImplTest {

    @Test
    void testPartialInputChannelStateWrite() throws Exception {
        testBuffersRecycled(
                buffers ->
                        ChannelStateWriteRequest.write(
                                1L,
                                new InputChannelInfo(1, 2),
                                ofElements(Buffer::recycleBuffer, buffers)));
    }

    @Test
    void testPartialResultSubpartitionStateWrite() throws Exception {
        testBuffersRecycled(
                buffers ->
                        ChannelStateWriteRequest.write(
                                1L, new ResultSubpartitionInfo(1, 2), buffers));
    }

    @Test
    void testConcurrentUnalignedCheckpoint() throws Exception {
        ChannelStateWriteRequestDispatcher processor =
                new ChannelStateWriteRequestDispatcherImpl(
                        "dummy task",
                        0,
                        getStreamFactoryFactory(),
                        new ChannelStateSerializerImpl());
        ChannelStateWriteResult result = new ChannelStateWriteResult();
        processor.dispatch(
                ChannelStateWriteRequest.start(
                        1L, result, CheckpointStorageLocationReference.getDefault()));
        assertThat(result.isDone()).isFalse();

        processor.dispatch(
                ChannelStateWriteRequest.start(
                        2L,
                        new ChannelStateWriteResult(),
                        CheckpointStorageLocationReference.getDefault()));
        assertThat(result.isDone()).isTrue();
        assertThat(result.getInputChannelStateHandles()).isCompletedExceptionally();
        assertThat(result.getResultSubpartitionStateHandles()).isCompletedExceptionally();
    }

    private void testBuffersRecycled(
            Function<NetworkBuffer[], ChannelStateWriteRequest> requestBuilder) throws Exception {
        ChannelStateWriteRequestDispatcher dispatcher =
                new ChannelStateWriteRequestDispatcherImpl(
                        "dummy task",
                        0,
                        new MemoryBackendCheckpointStorageAccess(new JobID(), null, null, 1),
                        new ChannelStateSerializerImpl());
        ChannelStateWriteResult result = new ChannelStateWriteResult();
        dispatcher.dispatch(
                ChannelStateWriteRequest.start(
                        1L, result, CheckpointStorageLocationReference.getDefault()));

        result.getResultSubpartitionStateHandles().completeExceptionally(new TestException());
        result.getInputChannelStateHandles().completeExceptionally(new TestException());

        NetworkBuffer[] buffers = new NetworkBuffer[] {buffer(), buffer()};
        dispatcher.dispatch(requestBuilder.apply(buffers));
        assertThat(buffers).allMatch(NetworkBuffer::isRecycled);
    }

    private NetworkBuffer buffer() {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(10), FreeingBufferRecycler.INSTANCE);
    }
}
