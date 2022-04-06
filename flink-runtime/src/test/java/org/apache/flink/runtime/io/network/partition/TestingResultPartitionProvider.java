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

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

/** {@link ResultPartitionProvider} implementation for testing purposes. */
public class TestingResultPartitionProvider implements ResultPartitionProvider {
    private final Function<Tuple3<ResultPartitionID, Integer, BufferAvailabilityListener>, ResultSubpartitionView> createSubpartitionViewFunction;
    private final Function<Tuple4<ResultPartitionID, Integer, BufferAvailabilityListener, PartitionRequestNotifier>, ResultSubpartitionView> createSubpartitionViewOrNotifyFunction;
    private final Consumer<NettyPartitionRequestNotifier> releasePartitionRequestNotifierConsumer;

    public TestingResultPartitionProvider(
            Function<Tuple3<ResultPartitionID, Integer, BufferAvailabilityListener>, ResultSubpartitionView> createSubpartitionViewFunction,
            Function<Tuple4<ResultPartitionID, Integer, BufferAvailabilityListener, PartitionRequestNotifier>, ResultSubpartitionView> createSubpartitionViewOrNotifyFunction,
            Consumer<NettyPartitionRequestNotifier> releasePartitionRequestNotifierConsumer) {
        this.createSubpartitionViewFunction = createSubpartitionViewFunction;
        this.createSubpartitionViewOrNotifyFunction = createSubpartitionViewOrNotifyFunction;
        this.releasePartitionRequestNotifierConsumer = releasePartitionRequestNotifierConsumer;
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            int index,
            BufferAvailabilityListener availabilityListener) throws IOException {
        return createSubpartitionViewFunction.apply(Tuple3.of(partitionId, index, availabilityListener));
    }

    @Override
    public ResultSubpartitionView createSubpartitionViewOrNotify(
            ResultPartitionID partitionId,
            int index,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestNotifier notifier) throws IOException {
        return createSubpartitionViewOrNotifyFunction.apply(Tuple4.of(partitionId, index, availabilityListener, notifier));
    }

    @Override
    public void releasePartitionRequestNotifier(NettyPartitionRequestNotifier notifier) {
        releasePartitionRequestNotifierConsumer.accept(notifier);
    }

    public static TestingResultPartitionProviderBuilder newBuilder() {
        return new TestingResultPartitionProviderBuilder();
    }
}
