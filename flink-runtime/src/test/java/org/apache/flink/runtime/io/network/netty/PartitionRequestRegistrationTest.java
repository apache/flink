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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.TestingResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.testutils.TestingUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests {@link NettyMessage.PartitionRequest} before and after {@link ResultPartitionManager}
 * registers given {@link ResultPartition}.
 */
class PartitionRequestRegistrationTest {

    /**
     * Verifies that result partition manager registers partition before receive partition request.
     */
    @Test
    void testRegisterResultPartitionBeforeRequest() throws Exception {
        final TestPooledBufferProvider outboundBuffers = new TestPooledBufferProvider(16);
        final CountDownLatch sync = new CountDownLatch(1);
        final ResultSubpartitionView view =
                new CancelPartitionRequestTest.InfiniteSubpartitionView(outboundBuffers, sync);

        ResultPartitionManager partitionManager = new ResultPartitionManager();
        ResultPartition resultPartition =
                TestingResultPartition.newBuilder()
                        .setResultPartitionManager(partitionManager)
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        // Register result partition before request
        partitionManager.registerResultPartition(resultPartition);

        NettyTestUtil.NettyServerAndClient serverAndClient = null;
        try {

            NettyProtocol protocol =
                    new NettyProtocol(partitionManager, new NoOpTaskEventPublisher());

            serverAndClient = initServerAndClient(protocol);

            Channel ch = connect(serverAndClient);

            // Request for non-existing input channel => results in cancel request
            ch.writeAndFlush(
                            new NettyMessage.PartitionRequest(
                                    resultPartition.getPartitionId(),
                                    0,
                                    new InputChannelID(),
                                    Integer.MAX_VALUE))
                    .await();

            // Wait for the notification
            if (!sync.await(TestingUtils.TESTING_DURATION.toMillis(), TimeUnit.MILLISECONDS)) {
                fail(
                        "Timed out after waiting for "
                                + TestingUtils.TESTING_DURATION.toMillis()
                                + " ms to be notified about cancelled partition.");
            }
        } finally {
            shutdown(serverAndClient);
        }
    }

    /**
     * Verifies that result partition manager registers partition after receive partition request.
     */
    @Test
    void testRegisterResultPartitionAfterRequest() throws Exception {
        final TestPooledBufferProvider outboundBuffers = new TestPooledBufferProvider(16);
        final CountDownLatch sync = new CountDownLatch(1);
        final ResultSubpartitionView view =
                new CancelPartitionRequestTest.InfiniteSubpartitionView(outboundBuffers, sync);

        ResultPartitionManager partitionManager = new ResultPartitionManager();
        ResultPartition resultPartition =
                TestingResultPartition.newBuilder()
                        .setResultPartitionManager(partitionManager)
                        .setCreateSubpartitionViewFunction((index, listener) -> view)
                        .build();

        NettyTestUtil.NettyServerAndClient serverAndClient = null;
        try {

            NettyProtocol protocol =
                    new NettyProtocol(partitionManager, new NoOpTaskEventPublisher());

            serverAndClient = initServerAndClient(protocol);

            Channel ch = connect(serverAndClient);

            // Request for non-existing input channel => results in cancel request
            ch.writeAndFlush(
                            new NettyMessage.PartitionRequest(
                                    resultPartition.getPartitionId(),
                                    0,
                                    new InputChannelID(),
                                    Integer.MAX_VALUE))
                    .await();

            // Register result partition after partition request
            partitionManager.registerResultPartition(resultPartition);

            // Wait for the notification
            if (!sync.await(TestingUtils.TESTING_DURATION.toMillis(), TimeUnit.MILLISECONDS)) {
                fail(
                        "Timed out after waiting for "
                                + TestingUtils.TESTING_DURATION.toMillis()
                                + " ms to be notified about cancelled partition.");
            }
        } finally {
            shutdown(serverAndClient);
        }
    }

    /** Verifies that result partition manager notifier timeout. */
    @Test
    void testPartitionRequestNotifierTimeout() throws Exception {
        final ResultPartitionID pid = new ResultPartitionID();
        final CountDownLatch sync = new CountDownLatch(1);

        NettyTestUtil.NettyServerAndClient serverAndClient = null;
        try {
            ResultPartitionProvider partitions =
                    new ResultPartitionProvider() {
                        @Override
                        public ResultSubpartitionView createSubpartitionView(
                                ResultPartitionID partitionId,
                                int index,
                                BufferAvailabilityListener availabilityListener) {
                            return null;
                        }

                        @Override
                        public Optional<ResultSubpartitionView>
                                createSubpartitionViewOrRegisterListener(
                                        ResultPartitionID partitionId,
                                        int index,
                                        BufferAvailabilityListener availabilityListener,
                                        PartitionRequestListener partitionRequestListener) {
                            partitionRequestListener.notifyPartitionCreatedTimeout();
                            return Optional.empty();
                        }

                        @Override
                        public void releasePartitionRequestListener(
                                PartitionRequestListener listener) {}
                    };

            NettyProtocol protocol = new NettyProtocol(partitions, new NoOpTaskEventPublisher());

            serverAndClient = initServerAndClient(protocol);

            Channel ch = connect(serverAndClient);

            NetworkClientHandler clientHandler = ch.pipeline().get(NetworkClientHandler.class);
            RemoteInputChannel remoteInputChannel =
                    new TestRemoteInputChannelForPartitionNotFound(sync);
            clientHandler.addInputChannel(remoteInputChannel);

            // Request for non-existing input channel => results in cancel request
            ch.writeAndFlush(
                            new NettyMessage.PartitionRequest(
                                    pid,
                                    0,
                                    remoteInputChannel.getInputChannelId(),
                                    Integer.MAX_VALUE))
                    .await();

            // Wait for the notification
            if (!sync.await(TestingUtils.TESTING_DURATION.toMillis(), TimeUnit.MILLISECONDS)) {
                fail(
                        "Timed out after waiting for "
                                + TestingUtils.TESTING_DURATION.toMillis()
                                + " ms to be notified about cancelled partition.");
            }
        } finally {
            shutdown(serverAndClient);
        }
    }

    /**
     * The test remote input channel to count down the latch when it receives partition not found
     * exception.
     */
    private static class TestRemoteInputChannelForPartitionNotFound extends RemoteInputChannel {
        private final CountDownLatch latch;

        TestRemoteInputChannelForPartitionNotFound(CountDownLatch latch) {
            super(
                    new SingleInputGateBuilder().setNumberOfChannels(1).build(),
                    0,
                    new ResultPartitionID(),
                    0,
                    InputChannelBuilder.STUB_CONNECTION_ID,
                    new TestingConnectionManager(),
                    0,
                    100,
                    10000,
                    2,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    ChannelStateWriter.NO_OP);
            this.latch = latch;
        }

        @Override
        public void onFailedPartitionRequest() {
            latch.countDown();
        }
    }

    /** A testing implementation of {@link TaskEventPublisher} without operation. */
    private static class NoOpTaskEventPublisher implements TaskEventPublisher {
        @Override
        public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
            return true;
        }
    }
}
