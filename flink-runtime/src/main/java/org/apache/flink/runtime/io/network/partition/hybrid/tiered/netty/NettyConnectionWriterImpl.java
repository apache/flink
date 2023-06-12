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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.util.Queue;

/** The default implementation of {@link NettyConnectionWriter}. */
public class NettyConnectionWriterImpl implements NettyConnectionWriter {

    private final Queue<NettyPayload> bufferQueue;

    private final NettyConnectionId connectionId;

    public NettyConnectionWriterImpl(Queue<NettyPayload> bufferQueue) {
        this.bufferQueue = bufferQueue;
        this.connectionId = NettyConnectionId.newId();
    }

    @Override
    public NettyConnectionId getNettyConnectionId() {
        return connectionId;
    }

    @Override
    public int numQueuedBuffers() {
        return bufferQueue.size();
    }

    @Override
    public void writeBuffer(NettyPayload nettyPayload) {
        bufferQueue.add(nettyPayload);
    }

    @Override
    public void close(@Nullable Throwable error) {
        NettyPayload nettyPayload;
        while ((nettyPayload = bufferQueue.poll()) != null) {
            nettyPayload.getBuffer().ifPresent(Buffer::recycleBuffer);
        }
        if (error != null) {
            writeBuffer(NettyPayload.newError(error));
        }
    }
}
