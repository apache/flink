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
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test case for {@link InputRequestNotifierManager}.
 */
public class InputRequestNotifierManagerTest extends TestLogger {
    /**
     * Test add notifier to {@link InputRequestNotifierManager}.
     */
    @Test
    public void testAddNotifier() {
        InputRequestNotifierManager inputRequestNotifierManager = new InputRequestNotifierManager();
        assertTrue(inputRequestNotifierManager.isEmpty());

        List<NettyPartitionRequestNotifier> notifierList = new ArrayList<>();
        NettyPartitionRequestNotifier notifier1 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                0,
                new ResultPartitionID());
        inputRequestNotifierManager.addNotifier(notifier1);
        notifierList.add(notifier1);

        NettyPartitionRequestNotifier notifier2 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                1,
                new ResultPartitionID());
        inputRequestNotifierManager.addNotifier(notifier2);
        notifierList.add(notifier2);

        NettyPartitionRequestNotifier notifier3 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                2,
                new ResultPartitionID());
        inputRequestNotifierManager.addNotifier(notifier3);
        notifierList.add(notifier3);

        assertEquals(notifierList.size(), inputRequestNotifierManager.getPartitionRequestNotifiers().size());
        assertTrue(notifierList.containsAll(inputRequestNotifierManager.getPartitionRequestNotifiers()));
    }

    /**
     * Test remove notifier from {@link InputRequestNotifierManager} by {@link InputChannelID}.
     */
    @Test
    public void testRemoveNotifier() {
        InputRequestNotifierManager inputRequestNotifierManager = new InputRequestNotifierManager();
        assertTrue(inputRequestNotifierManager.isEmpty());

        List<NettyPartitionRequestNotifier> notifierList = new ArrayList<>();
        NettyPartitionRequestNotifier notifier1 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                0,
                new ResultPartitionID());
        inputRequestNotifierManager.addNotifier(notifier1);

        NettyPartitionRequestNotifier notifier2 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                1,
                new ResultPartitionID());
        inputRequestNotifierManager.addNotifier(notifier2);
        notifierList.add(notifier2);

        NettyPartitionRequestNotifier notifier3 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                2,
                new ResultPartitionID());
        inputRequestNotifierManager.addNotifier(notifier3);
        notifierList.add(notifier3);

        inputRequestNotifierManager.remove(notifier1.getReceiverId());
        assertEquals(notifierList.size(), inputRequestNotifierManager.getPartitionRequestNotifiers().size());
        assertTrue(notifierList.containsAll(inputRequestNotifierManager.getPartitionRequestNotifiers()));
    }

    /**
     * Test remove expire notifiers from {@link InputRequestNotifierManager}.
     */
    @Test
    public void testRemoveExpiration() {
        InputRequestNotifierManager inputRequestNotifierManager = new InputRequestNotifierManager();
        assertTrue(inputRequestNotifierManager.isEmpty());

        List<NettyPartitionRequestNotifier> notifierList = new ArrayList<>();
        List<NettyPartitionRequestNotifier> expireNotifierList = new ArrayList<>();
        NettyPartitionRequestNotifier notifier1 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                0,
                new ResultPartitionID(),
                0L);
        inputRequestNotifierManager.addNotifier(notifier1);
        expireNotifierList.add(notifier1);

        NettyPartitionRequestNotifier notifier2 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                1,
                new ResultPartitionID(),
                0L);
        inputRequestNotifierManager.addNotifier(notifier2);
        expireNotifierList.add(notifier2);

        long currentTimestamp = System.currentTimeMillis();
        NettyPartitionRequestNotifier notifier3 = new NettyPartitionRequestNotifier(
                TestingResultPartitionProvider.newBuilder().build(),
                TestNotifierViewReader.newBuilder()
                        .setReceiverId(new InputChannelID())
                        .build(),
                2,
                new ResultPartitionID(),
                currentTimestamp);
        inputRequestNotifierManager.addNotifier(notifier3);
        notifierList.add(notifier3);

        List<PartitionRequestNotifier> removeExpireNotifierList = new ArrayList<>();
        inputRequestNotifierManager.removeExpiration(currentTimestamp, 1L, removeExpireNotifierList);

        assertEquals(notifierList.size(), inputRequestNotifierManager.getPartitionRequestNotifiers().size());
        assertTrue(notifierList.containsAll(inputRequestNotifierManager.getPartitionRequestNotifiers()));

        assertEquals(expireNotifierList.size(), removeExpireNotifierList.size());
        assertTrue(expireNotifierList.containsAll(removeExpireNotifierList));
    }
}
