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

import javax.annotation.Nullable;

import java.util.function.Function;
import java.util.function.Supplier;

/** Test implementation for {@link NettyConnectionWriter}. */
public class TestingNettyConnectionWriter implements NettyConnectionWriter {

    private final Function<NettyPayload, Void> writeBufferFunction;

    private final Supplier<NettyConnectionId> nettyConnectionIdSupplier;

    private final Supplier<Integer> numQueuedBuffersSupplier;

    private final Runnable availableNotifier;

    private final Function<Throwable, Void> closeFunction;

    private TestingNettyConnectionWriter(
            Function<NettyPayload, Void> writeBufferFunction,
            Supplier<NettyConnectionId> nettyConnectionIdSupplier,
            Supplier<Integer> numQueuedBuffersSupplier,
            Function<Throwable, Void> closeFunction,
            Runnable availableNotifier) {
        this.writeBufferFunction = writeBufferFunction;
        this.nettyConnectionIdSupplier = nettyConnectionIdSupplier;
        this.numQueuedBuffersSupplier = numQueuedBuffersSupplier;
        this.closeFunction = closeFunction;
        this.availableNotifier = availableNotifier;
    }

    @Override
    public void writeBuffer(NettyPayload nettyPayload) {
        writeBufferFunction.apply(nettyPayload);
    }

    @Override
    public NettyConnectionId getNettyConnectionId() {
        return nettyConnectionIdSupplier.get();
    }

    @Override
    public int numQueuedBuffers() {
        return numQueuedBuffersSupplier.get();
    }

    @Override
    public void notifyAvailable() {
        availableNotifier.run();
    }

    @Override
    public void close(@Nullable Throwable error) {
        closeFunction.apply(error);
    }

    /** Builder for {@link TestingNettyConnectionWriter}. */
    public static class Builder {
        private Function<NettyPayload, Void> writeBufferFunction = buffer -> null;

        private Supplier<NettyConnectionId> nettyConnectionIdSupplier =
                () -> NettyConnectionId.newId();

        private Supplier<Integer> numQueuedBuffersSupplier = () -> 0;

        private Function<Throwable, Void> closeFunction = throwable -> null;

        private Runnable availableNotifier = () -> {};

        public TestingNettyConnectionWriter.Builder setWriteBufferFunction(
                Function<NettyPayload, Void> writeBufferFunction) {
            this.writeBufferFunction = writeBufferFunction;
            return this;
        }

        public TestingNettyConnectionWriter.Builder setNettyConnectionIdSupplier(
                Supplier<NettyConnectionId> nettyConnectionIdSupplier) {
            this.nettyConnectionIdSupplier = nettyConnectionIdSupplier;
            return this;
        }

        public TestingNettyConnectionWriter.Builder setNumQueuedBuffersSupplier(
                Supplier<Integer> numQueuedBuffersSupplier) {
            this.numQueuedBuffersSupplier = numQueuedBuffersSupplier;
            return this;
        }

        public TestingNettyConnectionWriter.Builder setCloseFunction(
                Function<Throwable, Void> closeFunction) {
            this.closeFunction = closeFunction;
            return this;
        }

        public TestingNettyConnectionWriter.Builder setAvailableNotifier(
                Runnable availableNotifier) {
            this.availableNotifier = availableNotifier;
            return this;
        }

        public TestingNettyConnectionWriter build() {
            return new TestingNettyConnectionWriter(
                    writeBufferFunction,
                    nettyConnectionIdSupplier,
                    numQueuedBuffersSupplier,
                    closeFunction,
                    availableNotifier);
        }
    }
}
