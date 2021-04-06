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

import org.junit.Test;

import java.util.function.Function;

import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.junit.Assert.assertTrue;

/** {@link ChannelStateWriteRequestDispatcherImpl} test. */
public class ChannelStateWriteRequestDispatcherImplTest {

    @Test
    public void testPartialInputChannelStateWrite() throws Exception {
        testBuffersRecycled(
                buffers ->
                        ChannelStateWriteRequest.write(
                                1L,
                                new InputChannelInfo(1, 2),
                                ofElements(Buffer::recycleBuffer, buffers)));
    }

    @Test
    public void testPartialResultSubpartitionStateWrite() throws Exception {
        testBuffersRecycled(
                buffers ->
                        ChannelStateWriteRequest.write(
                                1L, new ResultSubpartitionInfo(1, 2), buffers));
    }

    private void testBuffersRecycled(
            Function<NetworkBuffer[], ChannelStateWriteRequest> requestBuilder) throws Exception {
        ChannelStateWriteRequestDispatcher dispatcher =
                new ChannelStateWriteRequestDispatcherImpl(
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
        for (NetworkBuffer buffer : buffers) {
            assertTrue(buffer.isRecycled());
        }
    }

    private NetworkBuffer buffer() {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(10), FreeingBufferRecycler.INSTANCE);
    }
}
