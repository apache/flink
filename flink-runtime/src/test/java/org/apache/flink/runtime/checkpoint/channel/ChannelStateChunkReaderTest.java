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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** {@link ChannelStateChunkReader} test. */
public class ChannelStateChunkReaderTest {

    @Test(expected = TestException.class)
    public void testBufferRecycledOnFailure() throws IOException, InterruptedException {
        FailingChannelStateSerializer serializer = new FailingChannelStateSerializer();
        TestRecoveredChannelStateHandler handler = new TestRecoveredChannelStateHandler();

        try (FSDataInputStream stream = getStream(serializer, 10)) {
            new ChannelStateChunkReader(serializer)
                    .readChunk(stream, serializer.getHeaderLength(), handler, "channelInfo");
        } finally {
            checkState(serializer.failed);
            checkState(!handler.requestedBuffers.isEmpty());
            assertTrue(
                    handler.requestedBuffers.stream()
                            .allMatch(TestChannelStateByteBuffer::isRecycled));
        }
    }

    @Test
    public void testBuffersNotRequestedForEmptyStream() throws IOException, InterruptedException {
        ChannelStateSerializer serializer = new ChannelStateSerializerImpl();
        TestRecoveredChannelStateHandler handler = new TestRecoveredChannelStateHandler();

        try (FSDataInputStream stream = getStream(serializer, 0)) {
            new ChannelStateChunkReader(serializer)
                    .readChunk(stream, serializer.getHeaderLength(), handler, "channelInfo");
        } finally {
            assertTrue(handler.requestedBuffers.isEmpty());
        }
    }

    @Test
    public void testNoSeekUnnecessarily() throws IOException, InterruptedException {
        final int offset = 123;
        final FSDataInputStream stream =
                new FSDataInputStream() {
                    @Override
                    public long getPos() {
                        return offset;
                    }

                    @Override
                    public void seek(long ignored) {
                        fail();
                    }

                    @Override
                    public int read() {
                        return 0;
                    }
                };

        new ChannelStateChunkReader(new ChannelStateSerializerImpl())
                .readChunk(stream, offset, new TestRecoveredChannelStateHandler(), "channelInfo");
    }

    private static class TestRecoveredChannelStateHandler
            implements RecoveredChannelStateHandler<Object, Object> {
        private final List<TestChannelStateByteBuffer> requestedBuffers = new ArrayList<>();

        @Override
        public BufferWithContext<Object> getBuffer(Object o) {
            TestChannelStateByteBuffer buffer = new TestChannelStateByteBuffer();
            requestedBuffers.add(buffer);
            return new BufferWithContext<>(buffer, null);
        }

        @Override
        public void recover(Object o, Object o2) {}

        @Override
        public void close() throws Exception {}
    }

    private static class FailingChannelStateSerializer extends ChannelStateSerializerImpl {
        private boolean failed;

        @Override
        public int readData(InputStream stream, ChannelStateByteBuffer buffer, int bytes) {
            failed = true;
            throw new TestException();
        }
    }

    private static FSDataInputStream getStream(ChannelStateSerializer serializer, int size)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DataOutputStream dataStream = new DataOutputStream(out);
            serializer.writeHeader(dataStream);
            serializer.writeData(
                    dataStream,
                    new NetworkBuffer(
                            MemorySegmentFactory.wrap(new byte[size]),
                            FreeingBufferRecycler.INSTANCE,
                            Buffer.DataType.DATA_BUFFER,
                            size));
            dataStream.flush();
            return new ByteStreamStateHandle("", out.toByteArray()).openInputStream();
        }
    }

    private static class TestChannelStateByteBuffer implements ChannelStateByteBuffer {
        private boolean recycled;

        @Override
        public boolean isWritable() {
            return true;
        }

        @Override
        public void recycle() {
            checkArgument(!recycled);
            recycled = true;
        }

        public boolean isRecycled() {
            return recycled;
        }

        @Override
        public int writeBytes(InputStream input, int bytesToRead) throws IOException {
            checkArgument(!recycled);
            input.skip(bytesToRead);
            return bytesToRead;
        }
    }
}
