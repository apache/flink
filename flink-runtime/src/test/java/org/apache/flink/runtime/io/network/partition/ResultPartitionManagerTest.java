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

import org.apache.flink.runtime.io.network.netty.NettyPartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.verifyCreateSubpartitionViewThrowsException;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ResultPartitionManager}. */
class ResultPartitionManagerTest extends TestLogger {

    /**
     * Tests that {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} would throw {@link PartitionNotFoundException} if this partition
     * was not registered before.
     */
    @Test
    void testThrowPartitionNotFoundException() {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        verifyCreateSubpartitionViewThrowsException(partitionManager, partition.getPartitionId());
    }

    /**
     * Tests {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} successful if this partition was already registered before.
     */
    @Test
    void testCreateViewForRegisteredPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        partitionManager.registerResultPartition(partition);
        partitionManager.createSubpartitionView(
                partition.getPartitionId(), 0, new NoOpBufferAvailablityListener());
    }

    /**
     * {@link ResultPartitionManager} creates subpartition view reader after the given partition is
     * registered.
     */
    @Test
    void testCreateSubpartitionViewAfterRegisteredPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        assertThat(partitionManager.getListenerManagers().isEmpty()).isTrue();

        partitionManager.registerResultPartition(partition);
        PartitionRequestListener partitionRequestListener =
                TestingPartitionRequestListener.newBuilder().build();
        assertThat(
                        partitionManager.createSubpartitionViewOrRegisterListener(
                                partition.getPartitionId(),
                                0,
                                new NoOpBufferAvailablityListener(),
                                partitionRequestListener))
                .isPresent();
        assertThat(partitionManager.getListenerManagers().isEmpty()).isTrue();
    }

    /**
     * The {@link ResultPartitionManager} registers {@link PartitionRequestListener} before specify
     * {@link ResultPartition} is registered. When the {@link ResultPartition} is registered, the
     * {@link ResultPartitionManager} will find the listener and create partition view reader. an
     */
    @Test
    void testRegisterPartitionListenerBeforeRegisteredPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        assertThat(partitionManager.getListenerManagers().isEmpty()).isTrue();

        final CompletableFuture<ResultPartition> notifySubpartitionCreatedFuture =
                new CompletableFuture<>();
        PartitionRequestListener partitionRequestListener =
                TestingPartitionRequestListener.newBuilder()
                        .setResultPartitionId(partition.getPartitionId())
                        .setNetworkSequenceViewReader(
                                TestingSubpartitionCreatedViewReader.newBuilder()
                                        .setNotifySubpartitionCreatedConsumer(
                                                tuple ->
                                                        notifySubpartitionCreatedFuture.complete(
                                                                tuple.f0))
                                        .build())
                        .build();
        assertThat(
                        partitionManager.createSubpartitionViewOrRegisterListener(
                                partition.getPartitionId(),
                                0,
                                new NoOpBufferAvailablityListener(),
                                partitionRequestListener))
                .isNotPresent();
        assertThat(partitionManager.getListenerManagers()).hasSize(1);

        // Check if the partition request listener is registered.
        PartitionRequestListenerManager listenerManager =
                partitionManager.getListenerManagers().get(partition.getPartitionId());
        assertThat(listenerManager).isNotNull();
        assertThat(listenerManager.isEmpty()).isFalse();
        assertThat(listenerManager.getPartitionRequestListeners()).hasSize(1);
        PartitionRequestListener listener =
                listenerManager.getPartitionRequestListeners().iterator().next();
        assertThat(listener.getResultPartitionId()).isEqualTo(partition.getPartitionId());
        assertThat(notifySubpartitionCreatedFuture).isNotDone();

        partitionManager.registerResultPartition(partition);

        // Check if the listener is notified.
        ResultPartition notifyPartition =
                notifySubpartitionCreatedFuture.get(10, TimeUnit.MILLISECONDS);
        assertThat(partition.getPartitionId()).isEqualTo(notifyPartition.getPartitionId());
        assertThat(partitionManager.getListenerManagers().isEmpty()).isTrue();
    }

    /**
     * Tests {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} would throw a {@link PartitionNotFoundException} if this
     * partition was already released before.
     */
    @Test
    void testCreateViewForReleasedPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        partitionManager.registerResultPartition(partition);
        partitionManager.releasePartition(partition.getPartitionId(), null);

        verifyCreateSubpartitionViewThrowsException(partitionManager, partition.getPartitionId());
    }

    /** Test notifier timeout in {@link ResultPartitionManager}. */
    @Test
    void testCreateViewReaderForNotifierTimeout() throws Exception {
        ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final ResultPartitionManager partitionManager =
                new ResultPartitionManager(1000000, scheduledExecutor);
        final ResultPartition partition1 = createPartition();
        final ResultPartition partition2 = createPartition();

        CompletableFuture<PartitionRequestListener> timeoutFuture1 = new CompletableFuture<>();
        CompletableFuture<PartitionRequestListener> timeoutFuture2 = new CompletableFuture<>();
        partitionManager.createSubpartitionViewOrRegisterListener(
                partition1.getPartitionId(),
                0,
                new NoOpBufferAvailablityListener(),
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .setPartitionRequestListenerTimeoutConsumer(
                                        timeoutFuture1::complete)
                                .build(),
                        0,
                        partition1.getPartitionId(),
                        0L));
        partitionManager.createSubpartitionViewOrRegisterListener(
                partition2.getPartitionId(),
                0,
                new NoOpBufferAvailablityListener(),
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .setPartitionRequestListenerTimeoutConsumer(
                                        timeoutFuture2::complete)
                                .build(),
                        0,
                        partition2.getPartitionId()));
        scheduledExecutor.triggerScheduledTasks();

        assertThat(timeoutFuture1.isDone()).isTrue();
        assertThat(partition1.getPartitionId())
                .isEqualTo(timeoutFuture1.get().getResultPartitionId());
        assertThat(timeoutFuture2.isDone()).isFalse();
        assertThat(partitionManager.getListenerManagers().get(partition1.getPartitionId()))
                .isNull();
        assertThat(partitionManager.getListenerManagers().get(partition2.getPartitionId()))
                .isNotNull();
    }
}
