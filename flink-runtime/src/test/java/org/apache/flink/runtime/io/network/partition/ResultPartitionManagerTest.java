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

import org.apache.flink.runtime.io.network.netty.NettyPartitionRequestNotifier;
import org.apache.flink.runtime.io.network.netty.PartitionRequestNotifierTimeout;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.util.TestLogger;

import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.verifyCreateSubpartitionViewThrowsException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link ResultPartitionManager}. */
public class ResultPartitionManagerTest extends TestLogger {

    /**
     * Tests that {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} would throw {@link PartitionNotFoundException} if this partition
     * was not registered before.
     */
    @Test
    public void testThrowPartitionNotFoundException() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        verifyCreateSubpartitionViewThrowsException(partitionManager, partition.getPartitionId());
    }

    /**
     * Tests {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} successful if this partition was already registered before.
     */
    @Test
    public void testCreateViewForRegisteredPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        partitionManager.registerResultPartition(partition);
        partitionManager.createSubpartitionView(
                partition.getPartitionId(), 0, new NoOpBufferAvailablityListener());
    }

    @Test
    public void testCreateViewNotifierAfterRegisteredPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        partitionManager.registerResultPartition(partition);
        PartitionRequestNotifier partitionRequestNotifier = new TestingPartitionRequestNotifier();
        assertNotNull(partitionManager.createSubpartitionViewOrNotify(
                partition.getPartitionId(), 0, new NoOpBufferAvailablityListener(), partitionRequestNotifier));
    }

    @Test
    public void testCreateViewNotifierBeforeRegisteredPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        PartitionRequestNotifier partitionRequestNotifier = new TestingPartitionRequestNotifier();
        assertNull(partitionManager.createSubpartitionViewOrNotify(
                partition.getPartitionId(), 0, new NoOpBufferAvailablityListener(), partitionRequestNotifier));

        partitionManager.registerResultPartition(partition);
    }

    /**
     * Tests {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} would throw a {@link PartitionNotFoundException} if this
     * partition was already released before.
     */
    @Test
    public void testCreateViewForReleasedPartition() throws Exception {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition partition = createPartition();

        partitionManager.registerResultPartition(partition);
        partitionManager.releasePartition(partition.getPartitionId(), null);

        verifyCreateSubpartitionViewThrowsException(partitionManager, partition.getPartitionId());
    }

    /**
     * Test notifier timeout in {@link ResultPartitionManager}.
     */
    @Test
    public void testCreateViewReaderForNotifierTimeout() throws Exception {
        ManuallyTriggeredScheduledExecutor scheduledExecutor = new ManuallyTriggeredScheduledExecutor();
        final ResultPartitionManager partitionManager = new ResultPartitionManager(Duration.ofSeconds(100L), scheduledExecutor);
        final ResultPartition partition1 = createPartition();
        final ResultPartition partition2 = createPartition();

        CompletableFuture<PartitionRequestNotifierTimeout> timeoutFuture1 = new CompletableFuture<>();
        CompletableFuture<PartitionRequestNotifierTimeout> timeoutFuture2 = new CompletableFuture<>();
        partitionManager.createSubpartitionViewOrNotify(
                partition1.getPartitionId(),
                0,
                new NoOpBufferAvailablityListener(),
                new NettyPartitionRequestNotifier(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestNotifierViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .setPartitionRequestNotifierTimeout(timeoutFuture1::complete)
                                .build(),
                        0,
                        partition1.getPartitionId(),
                        0L));
        partitionManager.createSubpartitionViewOrNotify(
                partition2.getPartitionId(),
                0,
                new NoOpBufferAvailablityListener(),
                new NettyPartitionRequestNotifier(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestNotifierViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .setPartitionRequestNotifierTimeout(timeoutFuture2::complete)
                                .build(),
                        0,
                        partition2.getPartitionId()));
        scheduledExecutor.triggerScheduledTasks();

        assertTrue(timeoutFuture1.isDone());
        assertEquals(partition1.getPartitionId(), timeoutFuture1.get().getPartitionRequestNotifier().getResultPartitionId());
        assertFalse(timeoutFuture2.isDone());
    }
}
