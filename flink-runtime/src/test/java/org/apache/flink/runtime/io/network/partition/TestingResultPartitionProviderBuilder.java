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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.io.network.netty.NettyPartitionRequestNotifier;

import java.util.function.Consumer;
import java.util.function.Function;

/** Factory for {@link TestingResultPartitionProvider}. */
public class TestingResultPartitionProviderBuilder {
    private Function<Tuple3<ResultPartitionID, Integer, BufferAvailabilityListener>, ResultSubpartitionView> createSubpartitionViewFunction = tuple -> null;
    private Function<Tuple4<ResultPartitionID, Integer, BufferAvailabilityListener, PartitionRequestNotifier>, ResultSubpartitionView> createSubpartitionViewOrNotifyFunction = tuple -> null;
    private Consumer<NettyPartitionRequestNotifier> releasePartitionRequestNotifierConsumer = notifier -> {};

    public TestingResultPartitionProviderBuilder setCreateSubpartitionViewFunction(
            Function<Tuple3<ResultPartitionID, Integer, BufferAvailabilityListener>, ResultSubpartitionView> createSubpartitionViewFunction) {
        this.createSubpartitionViewFunction = createSubpartitionViewFunction;
        return this;
    }

    public TestingResultPartitionProviderBuilder setCreateSubpartitionViewOrNotifyFunction(
            Function<Tuple4<ResultPartitionID, Integer, BufferAvailabilityListener, PartitionRequestNotifier>, ResultSubpartitionView> createSubpartitionViewOrNotifyFunction) {
        this.createSubpartitionViewOrNotifyFunction = createSubpartitionViewOrNotifyFunction;
        return this;
    }

    public TestingResultPartitionProviderBuilder setReleasePartitionRequestNotifierConsumer(
            Consumer<NettyPartitionRequestNotifier> releasePartitionRequestNotifierConsumer) {
        this.releasePartitionRequestNotifierConsumer = releasePartitionRequestNotifierConsumer;
        return this;
    }

    public TestingResultPartitionProvider build() {
        return new TestingResultPartitionProvider(
                createSubpartitionViewFunction,
                createSubpartitionViewOrNotifyFunction,
                releasePartitionRequestNotifierConsumer);
    }
}
