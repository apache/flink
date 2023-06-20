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

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link TieredStorageNettyService}. */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    // ------------------------------------
    //          For producer side
    // ------------------------------------

    private final Map<TieredStoragePartitionId, List<NettyServiceProducer>>
            registeredServiceProducers = new ConcurrentHashMap<>();

    private final Map<NettyConnectionId, BufferAvailabilityListener>
            registeredAvailabilityListeners = new ConcurrentHashMap<>();

    // ------------------------------------
    //          For consumer side
    // ------------------------------------

    /** The initialization in consumer side is thread-safe. */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, List<NettyConnectionReaderRegistration>>>
            nettyConnectionReaderRegistrations = new HashMap<>();

    @Override
    public void registerProducer(
            TieredStoragePartitionId partitionId, NettyServiceProducer serviceProducer) {
        registeredServiceProducers
                .computeIfAbsent(partitionId, ignore -> new ArrayList<>())
                .add(serviceProducer);
    }

    @Override
    public CompletableFuture<NettyConnectionReader> registerConsumer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {

        List<NettyConnectionReaderRegistration> registrations =
                getReaderRegistration(partitionId, subpartitionId);
        for (NettyConnectionReaderRegistration registration : registrations) {
            Optional<CompletableFuture<NettyConnectionReader>> futureOpt =
                    registration.trySetConsumer();
            if (futureOpt.isPresent()) {
                return futureOpt.get();
            }
        }

        NettyConnectionReaderRegistration registration = new NettyConnectionReaderRegistration();
        registrations.add(registration);
        return registration.trySetConsumer().get();
    }

    /**
     * Create a {@link ResultSubpartitionView} for the netty server.
     *
     * @param partitionId partition id indicates the unique id of {@link TieredResultPartition}.
     * @param subpartitionId subpartition id indicates the unique id of subpartition.
     * @param availabilityListener listener is used to listen the available status of data.
     * @return the {@link TieredStorageResultSubpartitionView}.
     */
    public ResultSubpartitionView createResultSubpartitionView(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            BufferAvailabilityListener availabilityListener) {
        List<NettyServiceProducer> serviceProducers = registeredServiceProducers.get(partitionId);
        if (serviceProducers == null) {
            return new TieredStorageResultSubpartitionView(
                    availabilityListener, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        List<Queue<NettyPayload>> queues = new ArrayList<>();
        List<NettyConnectionId> nettyConnectionIds = new ArrayList<>();
        for (NettyServiceProducer serviceProducer : serviceProducers) {
            LinkedBlockingQueue<NettyPayload> queue = new LinkedBlockingQueue<>();
            NettyConnectionWriterImpl writer = new NettyConnectionWriterImpl(queue);
            serviceProducer.connectionEstablished(subpartitionId, writer);
            nettyConnectionIds.add(writer.getNettyConnectionId());
            queues.add(queue);
            registeredAvailabilityListeners.put(
                    writer.getNettyConnectionId(), availabilityListener);
        }
        return new TieredStorageResultSubpartitionView(
                availabilityListener,
                queues,
                nettyConnectionIds,
                registeredServiceProducers.get(partitionId));
    }

    /**
     * Notify the {@link ResultSubpartitionView} to send buffer.
     *
     * @param connectionId connection id indicates the id of connection.
     */
    public void notifyResultSubpartitionViewSendBuffer(NettyConnectionId connectionId) {
        BufferAvailabilityListener listener = registeredAvailabilityListeners.get(connectionId);
        if (listener != null) {
            listener.notifyDataAvailable();
        }
    }

    /**
     * Set up input channels in {@link SingleInputGate}. The method will be invoked by the akka rpc
     * thread at first, and then the method {@link
     * TieredStorageNettyService#registerConsumer(TieredStoragePartitionId,
     * TieredStorageSubpartitionId)} will be invoked by the same thread sequentially, which ensures
     * thread safety.
     *
     * @param tieredStorageConsumerSpecs specs indicates {@link TieredResultPartition} and {@link
     *     TieredStorageSubpartitionId}.
     * @param inputChannelProviders it provides input channels for subpartitions.
     * @param helper it helps the reader notify the available and priority status.
     */
    public void setupInputChannels(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<Supplier<InputChannel>> inputChannelProviders,
            NettyConnectionReaderAvailabilityAndPriorityHelper helper) {
        checkState(tieredStorageConsumerSpecs.size() == inputChannelProviders.size());
        for (int index = 0; index < tieredStorageConsumerSpecs.size(); ++index) {
            setupInputChannel(
                    index,
                    tieredStorageConsumerSpecs.get(index).getPartitionId(),
                    tieredStorageConsumerSpecs.get(index).getSubpartitionId(),
                    inputChannelProviders.get(index),
                    helper);
        }
    }

    private void setupInputChannel(
            int index,
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            Supplier<InputChannel> inputChannelProvider,
            NettyConnectionReaderAvailabilityAndPriorityHelper helper) {
        List<NettyConnectionReaderRegistration> registrations =
                getReaderRegistration(partitionId, subpartitionId);
        for (NettyConnectionReaderRegistration registration : registrations) {
            if (registration.trySetChannel(index, inputChannelProvider, helper)) {
                return;
            }
        }

        NettyConnectionReaderRegistration registration = new NettyConnectionReaderRegistration();
        registration.trySetChannel(index, inputChannelProvider, helper);
        registrations.add(registration);
    }

    private List<NettyConnectionReaderRegistration> getReaderRegistration(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        return nettyConnectionReaderRegistrations
                .computeIfAbsent(partitionId, (ignore) -> new HashMap<>())
                .computeIfAbsent(subpartitionId, (ignore) -> new ArrayList<>());
    }

    /**
     * This class is used for pairing input channels with data consumers. Each registration consists
     * of exactly 1 input channel and exactly 1 data consumer. Upon both input channel and data
     * consumer are set, it creates a {@link NettyConnectionReader} through with the data consumer
     * reads data from the input channel.
     */
    private static class NettyConnectionReaderRegistration {

        /** This can be negative iff channel is not yet set. */
        private int channelIndex = -1;

        /** This can be null iff channel is not yet set. */
        @Nullable private Supplier<InputChannel> channelSupplier;

        /** This can be null iff channel is not yet set. */
        @Nullable private NettyConnectionReaderAvailabilityAndPriorityHelper helper;

        /** This can be null iff reader is not yet set. */
        @Nullable private CompletableFuture<NettyConnectionReader> readerFuture;

        /**
         * Try to set input channel.
         *
         * @param channelIndex the index of channel.
         * @param channelSupplier supplier to provide channel.
         * @param helper it helps the reader notify the available and priority status.
         * @return true if the channel is successfully set, or false if the registration already has
         *     an input channel.
         */
        public boolean trySetChannel(
                int channelIndex,
                Supplier<InputChannel> channelSupplier,
                NettyConnectionReaderAvailabilityAndPriorityHelper helper) {
            if (isChannelSet()) {
                return false;
            }

            checkArgument(channelIndex >= 0);
            this.channelIndex = channelIndex;
            this.channelSupplier = checkNotNull(channelSupplier);
            this.helper = checkNotNull(helper);

            tryCreateNettyConnectionReader();

            return true;
        }

        /**
         * Try to set data consumer.
         *
         * @return a future that provides the netty connection reader upon its created, or empty if
         *     the registration already has a consumer.
         */
        public Optional<CompletableFuture<NettyConnectionReader>> trySetConsumer() {
            if (!isReaderSet()) {
                this.readerFuture = new CompletableFuture<>();
                return Optional.of(readerFuture);
            }

            tryCreateNettyConnectionReader();

            return Optional.empty();
        }

        void tryCreateNettyConnectionReader() {
            if (isChannelSet() && isReaderSet()) {
                readerFuture.complete(
                        new NettyConnectionReaderImpl(channelIndex, channelSupplier, helper));
            }
        }

        private boolean isChannelSet() {
            return channelIndex >= 0;
        }

        private boolean isReaderSet() {
            return readerFuture != null;
        }
    }
}
