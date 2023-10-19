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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link PartitionRequestListenerManager}. */
class PartitionRequestListenerManagerTest extends TestLogger {
    /** Test add listener to {@link PartitionRequestListenerManager}. */
    @Test
    void testAddListener() {
        PartitionRequestListenerManager partitionRequestListenerManager =
                new PartitionRequestListenerManager();
        assertThat(partitionRequestListenerManager.isEmpty()).isTrue();

        List<PartitionRequestListener> listenerList = new ArrayList<>();
        NettyPartitionRequestListener listener1 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        0,
                        new ResultPartitionID());
        partitionRequestListenerManager.registerListener(listener1);
        listenerList.add(listener1);

        NettyPartitionRequestListener listener2 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        1,
                        new ResultPartitionID());
        partitionRequestListenerManager.registerListener(listener2);
        listenerList.add(listener2);

        NettyPartitionRequestListener listener3 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        2,
                        new ResultPartitionID());
        partitionRequestListenerManager.registerListener(listener3);
        listenerList.add(listener3);

        assertThat(partitionRequestListenerManager.getPartitionRequestListeners())
                .hasSize(listenerList.size());
        assertThat(listenerList)
                .containsAll(partitionRequestListenerManager.getPartitionRequestListeners());
    }

    /**
     * Test remove listener from {@link PartitionRequestListenerManager} by {@link InputChannelID}.
     */
    @Test
    void testRemoveListener() {
        PartitionRequestListenerManager partitionRequestListenerManager =
                new PartitionRequestListenerManager();
        assertThat(partitionRequestListenerManager.isEmpty()).isTrue();

        List<PartitionRequestListener> listenerList = new ArrayList<>();
        NettyPartitionRequestListener listener1 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        0,
                        new ResultPartitionID());
        partitionRequestListenerManager.registerListener(listener1);

        NettyPartitionRequestListener listener2 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        1,
                        new ResultPartitionID());
        partitionRequestListenerManager.registerListener(listener2);
        listenerList.add(listener2);

        NettyPartitionRequestListener listener3 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        2,
                        new ResultPartitionID());
        partitionRequestListenerManager.registerListener(listener3);
        listenerList.add(listener3);

        partitionRequestListenerManager.remove(listener1.getReceiverId());
        assertThat(partitionRequestListenerManager.getPartitionRequestListeners())
                .hasSize(listenerList.size());
        assertThat(listenerList)
                .containsAll(partitionRequestListenerManager.getPartitionRequestListeners());
    }

    /** Test remove expire listeners from {@link PartitionRequestListenerManager}. */
    @Test
    void testRemoveExpiration() {
        PartitionRequestListenerManager partitionRequestListenerManager =
                new PartitionRequestListenerManager();
        assertThat(partitionRequestListenerManager.isEmpty()).isTrue();

        List<PartitionRequestListener> listenerList = new ArrayList<>();
        List<PartitionRequestListener> expireListenerList = new ArrayList<>();
        NettyPartitionRequestListener listener1 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        0,
                        new ResultPartitionID(),
                        0L);
        partitionRequestListenerManager.registerListener(listener1);
        expireListenerList.add(listener1);

        NettyPartitionRequestListener listener2 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        1,
                        new ResultPartitionID(),
                        0L);
        partitionRequestListenerManager.registerListener(listener2);
        expireListenerList.add(listener2);

        long currentTimestamp = System.currentTimeMillis();
        NettyPartitionRequestListener listener3 =
                new NettyPartitionRequestListener(
                        TestingResultPartitionProvider.newBuilder().build(),
                        TestingSubpartitionCreatedViewReader.newBuilder()
                                .setReceiverId(new InputChannelID())
                                .build(),
                        2,
                        new ResultPartitionID(),
                        currentTimestamp);
        partitionRequestListenerManager.registerListener(listener3);
        listenerList.add(listener3);

        List<PartitionRequestListener> removeExpireListenerList = new ArrayList<>();
        partitionRequestListenerManager.removeExpiration(
                currentTimestamp, 1L, removeExpireListenerList);

        assertThat(partitionRequestListenerManager.getPartitionRequestListeners())
                .hasSize(listenerList.size());
        assertThat(listenerList)
                .containsAll(partitionRequestListenerManager.getPartitionRequestListeners());

        assertThat(removeExpireListenerList).hasSize(expireListenerList.size());
        assertThat(expireListenerList).containsAll(removeExpireListenerList);
    }
}
