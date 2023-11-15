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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyConnectionWriter}. */
public class NettyConnectionWriterTest {

    private static final int SUBPARTITION_ID = 0;

    @Test
    void testWriteBuffer() {
        int bufferNumber = 10;
        NettyPayloadManager nettyPayloadManager = new NettyPayloadManager();
        NettyConnectionWriter nettyConnectionWriter =
                new NettyConnectionWriterImpl(nettyPayloadManager, () -> {});
        writeBufferToWriter(bufferNumber, nettyConnectionWriter);
        assertThat(nettyPayloadManager.getBacklog()).isEqualTo(bufferNumber);
        assertThat(nettyConnectionWriter.numQueuedPayloads()).isEqualTo(bufferNumber);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(bufferNumber);
    }

    @Test
    void testGetNettyConnectionId() {
        NettyConnectionWriter nettyConnectionWriter =
                new NettyConnectionWriterImpl(new NettyPayloadManager(), () -> {});
        assertThat(nettyConnectionWriter.getNettyConnectionId()).isNotNull();
    }

    @Test
    void testNotifyAvailable() {
        CompletableFuture<Void> notifier = new CompletableFuture<>();
        NettyConnectionWriter nettyConnectionWriter =
                new NettyConnectionWriterImpl(
                        new NettyPayloadManager(),
                        () -> {
                            notifier.complete(null);
                        });
        nettyConnectionWriter.notifyAvailable();
        assertThat(notifier).isDone();
    }

    @Test
    void testClose() {
        int bufferNumber = 10;
        NettyConnectionWriter nettyConnectionWriter =
                new NettyConnectionWriterImpl(new NettyPayloadManager(), () -> {});
        writeBufferToWriter(bufferNumber, nettyConnectionWriter);
        nettyConnectionWriter.close(null);
        assertThat(nettyConnectionWriter.numQueuedPayloads()).isZero();
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isZero();
        writeBufferToWriter(bufferNumber, nettyConnectionWriter);
        nettyConnectionWriter.close(new IOException());
        assertThat(nettyConnectionWriter.numQueuedPayloads()).isOne();
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isZero();
    }

    @Test
    void testGetNumQueuedBufferPayloads() {
        NettyPayloadManager nettyPayloadManager = new NettyPayloadManager();
        NettyConnectionWriter nettyConnectionWriter =
                new NettyConnectionWriterImpl(nettyPayloadManager, () -> {});
        nettyConnectionWriter.writeNettyPayload(NettyPayload.newSegment(0));
        writeBufferToWriter(3, nettyConnectionWriter);
        nettyConnectionWriter.writeNettyPayload(NettyPayload.newSegment(2));
        writeBufferToWriter(1, nettyConnectionWriter);
        nettyConnectionWriter.writeNettyPayload(NettyPayload.newSegment(3));
        writeBufferToWriter(1, nettyConnectionWriter);
        nettyConnectionWriter.writeNettyPayload(NettyPayload.newSegment(5));
        writeBufferToWriter(5, nettyConnectionWriter);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(3);
        clearNettyPayloadManager(1, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(3);
        clearNettyPayloadManager(2, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(1);
        clearNettyPayloadManager(1, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(2);
        clearNettyPayloadManager(1, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(2);
        clearNettyPayloadManager(1, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(1);
        clearNettyPayloadManager(1, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(1);
        clearNettyPayloadManager(1, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(5);
        clearNettyPayloadManager(1, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(5);
        clearNettyPayloadManager(2, nettyPayloadManager);
        assertThat(nettyConnectionWriter.numQueuedBufferPayloads()).isEqualTo(3);
    }

    private static void writeBufferToWriter(
            int bufferNumber, NettyConnectionWriter nettyConnectionWriter) {
        for (int index = 0; index < bufferNumber; ++index) {
            nettyConnectionWriter.writeNettyPayload(
                    NettyPayload.newBuffer(
                            BufferBuilderTestUtils.buildSomeBuffer(0), index, SUBPARTITION_ID));
        }
    }

    private static void clearNettyPayloadManager(
            int payloadNumber, NettyPayloadManager nettyPayloadManager) {
        for (int index = 0; index < payloadNumber; ++index) {
            nettyPayloadManager.poll();
        }
    }
}
