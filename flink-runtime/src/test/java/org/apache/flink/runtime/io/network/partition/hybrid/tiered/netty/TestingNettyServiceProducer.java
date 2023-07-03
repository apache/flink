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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Test implementation for {@link NettyServiceProducer}. */
public class TestingNettyServiceProducer implements NettyServiceProducer {

    private final BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter>
            connectionEstablishedConsumer;

    private final Consumer<NettyConnectionId> connectionBrokenConsumer;

    private TestingNettyServiceProducer(
            BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter>
                    connectionEstablishedConsumer,
            Consumer<NettyConnectionId> connectionBrokenConsumer) {
        this.connectionEstablishedConsumer = connectionEstablishedConsumer;
        this.connectionBrokenConsumer = connectionBrokenConsumer;
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        connectionEstablishedConsumer.accept(subpartitionId, nettyConnectionWriter);
    }

    @Override
    public void connectionBroken(NettyConnectionId connectionId) {
        connectionBrokenConsumer.accept(connectionId);
    }

    /** Builder for {@link TestingNettyServiceProducer}. */
    public static class Builder {

        private BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter>
                connectionEstablishConsumer = (subpartitionId, nettyConnectionWriter) -> {};

        private Consumer<NettyConnectionId> connectionBrokenConsumer = connectionId -> {};

        public Builder setConnectionEstablishConsumer(
                BiConsumer<TieredStorageSubpartitionId, NettyConnectionWriter>
                        connectionEstablishConsumer) {
            this.connectionEstablishConsumer = connectionEstablishConsumer;
            return this;
        }

        public Builder setConnectionBrokenConsumer(
                Consumer<NettyConnectionId> connectionBrokenConsumer) {
            this.connectionBrokenConsumer = connectionBrokenConsumer;
            return this;
        }

        public TestingNettyServiceProducer build() {
            return new TestingNettyServiceProducer(
                    connectionEstablishConsumer, connectionBrokenConsumer);
        }
    }
}
