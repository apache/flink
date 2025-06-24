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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link ChannelStateSerializerImpl} test. */
class ChannelStateSerializerImplTest {

    @Test
    void testReadWrite() throws IOException {
        byte[] data = generateData(123);
        ChannelStateSerializerImpl serializer = new ChannelStateSerializerImpl();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length)) {
            write(data, serializer, baos);
            readAndCheck(data, serializer, new ByteArrayInputStream(baos.toByteArray()));
        }
    }

    @Test
    void testReadWriteWithMultipleBuffers() throws IOException {
        int bufSize = 10;
        int[] numBuffersToWriteAtOnce = {0, 1, 2, 3};
        byte[] data = generateData(bufSize);
        ChannelStateSerializer s = new ChannelStateSerializerImpl();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        s.writeHeader(out);
        for (int count : numBuffersToWriteAtOnce) {
            Buffer[] buffers = new Buffer[count];
            Arrays.fill(buffers, getBuffer(data));
            s.writeData(out, buffers);
        }
        out.close();

        ChannelStateSerializer d = new ChannelStateSerializerImpl();
        ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
        d.readHeader(is);
        for (int count : numBuffersToWriteAtOnce) {
            int expected = bufSize * count;
            assertThat(d.readLength(is)).isEqualTo(expected);
            byte[] readBuf = new byte[expected];
            assertThat(d.readData(is, wrap(readBuf), Integer.MAX_VALUE)).isEqualTo(expected);
            for (int i = 0; i < count; i++) {
                assertThat(Arrays.copyOfRange(readBuf, i * bufSize, (i + 1) * bufSize))
                        .isEqualTo(data);
            }
        }
    }

    @Test
    void testReadToBufferBuilder() throws IOException {
        byte[] data = generateData(100);
        BufferBuilder bufferBuilder =
                new BufferBuilder(
                        MemorySegmentFactory.allocateUnpooledSegment(data.length, null),
                        FreeingBufferRecycler.INSTANCE);
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        new ChannelStateSerializerImpl()
                .readData(new ByteArrayInputStream(data), wrap(bufferBuilder), Integer.MAX_VALUE);

        assertThat(bufferBuilder.isFinished()).isFalse();

        bufferBuilder.finish();
        Buffer buffer = bufferConsumer.build();

        assertThat(buffer.readableBytes()).isEqualTo(data.length);
        byte[] actual = new byte[buffer.readableBytes()];
        buffer.asByteBuf().readBytes(actual);
        assertThat(actual).isEqualTo(data);
    }

    private NetworkBuffer getBuffer(byte[] data) {
        NetworkBuffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(data.length, null),
                        FreeingBufferRecycler.INSTANCE);
        buffer.writeBytes(data);
        return buffer;
    }

    private byte[] readBytes(NetworkBuffer buffer) {
        byte[] tmp = new byte[buffer.readableBytes()];
        buffer.readBytes(tmp);
        return tmp;
    }

    private void write(byte[] data, ChannelStateSerializerImpl serializer, OutputStream baos)
            throws IOException {
        DataOutputStream out = new DataOutputStream(baos);
        serializer.writeHeader(out);
        NetworkBuffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(data.length),
                        FreeingBufferRecycler.INSTANCE);
        try {
            buffer.writeBytes(data);
            serializer.writeData(out, buffer);
            out.flush();
        } finally {
            buffer.release();
        }
    }

    private void readAndCheck(
            byte[] data, ChannelStateSerializerImpl serializer, ByteArrayInputStream is)
            throws IOException {
        serializer.readHeader(is);
        int size = serializer.readLength(is);
        assertThat(data).hasSize(size);
        NetworkBuffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(data.length),
                        FreeingBufferRecycler.INSTANCE);
        try {
            int read = serializer.readData(is, wrap(buffer), size);
            assertThat(read).isEqualTo(size);
            assertThat(readBytes(buffer)).isEqualTo(data);
        } finally {
            buffer.release();
        }
    }

    static byte[] generateData(int len) {
        byte[] bytes = new byte[len];
        new Random().nextBytes(bytes);
        return bytes;
    }
}
