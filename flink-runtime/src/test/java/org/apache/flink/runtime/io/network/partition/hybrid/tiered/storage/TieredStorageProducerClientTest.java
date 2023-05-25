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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.TieredStorageTestUtils.generateRandomData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for {@link TieredStorageProducerClient}. */
@ExtendWith(ParameterizedTestExtension.class)
public class TieredStorageProducerClientTest {

    private static final int NUM_TOTAL_BUFFERS = 1000;

    private static final int NETWORK_BUFFER_SIZE = 1024;

    @Parameter public boolean isBroadcast;

    private NetworkBufferPool globalPool;

    @Parameters(name = "isBroadcast={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @BeforeEach
    void before() {
        globalPool = new NetworkBufferPool(NUM_TOTAL_BUFFERS, NETWORK_BUFFER_SIZE);
    }

    @AfterEach
    void after() {
        globalPool.destroy();
    }

    @TestTemplate
    void testWriteRecordsToEmptyStorageTiers() {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        Random random = new Random();

        TieredStorageProducerClient tieredStorageProducerClient =
                createTieredStorageProducerClient(numSubpartitions, Collections.emptyList());
        assertThatThrownBy(
                        () ->
                                tieredStorageProducerClient.write(
                                        generateRandomData(bufferSize, random),
                                        new TieredStorageSubpartitionId(0),
                                        Buffer.DataType.DATA_BUFFER,
                                        isBroadcast))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to choose a storage tier");
    }

    @TestTemplate
    void testWriteRecords() throws IOException {
        int numSubpartitions = 10;
        int numToWriteRecords = 20;
        int bufferSize = 1024;
        Random random = new Random();

        AtomicInteger numReceivedBuffers = new AtomicInteger(0);
        AtomicInteger numReceivedBytes = new AtomicInteger(0);
        AtomicInteger numReceivedBuffersInTier1 = new AtomicInteger(0);
        AtomicInteger numReceivedBuffersInTier2 = new AtomicInteger(0);

        TestingTierProducerAgent tierProducerAgent1 =
                new TestingTierProducerAgent.Builder()
                        .setTryStartSegmentSupplier(
                                ((subpartitionId, integer) -> numReceivedBuffersInTier1.get() < 1))
                        .setTryWriterFunction(
                                ((subpartitionId, buffer) -> {
                                    boolean isSuccess = numReceivedBuffersInTier1.get() % 2 == 0;
                                    if (isSuccess) {
                                        numReceivedBuffers.incrementAndGet();
                                        numReceivedBuffersInTier1.incrementAndGet();
                                        numReceivedBytes.set(
                                                numReceivedBytes.get() + buffer.readableBytes());
                                    }
                                    return isSuccess;
                                }))
                        .build();
        TestingTierProducerAgent tierProducerAgent2 =
                new TestingTierProducerAgent.Builder()
                        .setTryWriterFunction(
                                ((subpartitionId, buffer) -> {
                                    numReceivedBuffers.incrementAndGet();
                                    numReceivedBuffersInTier2.incrementAndGet();
                                    numReceivedBytes.set(
                                            numReceivedBytes.get() + buffer.readableBytes());
                                    return true;
                                }))
                        .build();
        List<TierProducerAgent> tierProducerAgents = new ArrayList<>();
        tierProducerAgents.add(tierProducerAgent1);
        tierProducerAgents.add(tierProducerAgent2);

        TieredStorageProducerClient tieredStorageProducerClient =
                createTieredStorageProducerClient(numSubpartitions, tierProducerAgents);
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);

        for (int i = 0; i < numToWriteRecords; i++) {
            tieredStorageProducerClient.write(
                    generateRandomData(bufferSize, random),
                    subpartitionId,
                    Buffer.DataType.DATA_BUFFER,
                    isBroadcast);
        }

        int numExpectedBytes =
                isBroadcast
                        ? numSubpartitions * numToWriteRecords * bufferSize
                        : numToWriteRecords * bufferSize;
        assertThat(numReceivedBuffersInTier1.get()).isEqualTo(1);
        assertThat(numReceivedBuffers.get())
                .isEqualTo(numReceivedBuffersInTier1.get() + numReceivedBuffersInTier2.get());
        assertThat(numReceivedBytes.get()).isEqualTo(numExpectedBytes);
    }

    @TestTemplate
    void testTierCanNotStartNewSegment() {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        Random random = new Random();

        TestingTierProducerAgent tierProducerAgent =
                new TestingTierProducerAgent.Builder()
                        .setTryStartSegmentSupplier(((subpartitionId, integer) -> false))
                        .build();
        TieredStorageProducerClient tieredStorageProducerClient =
                createTieredStorageProducerClient(
                        numSubpartitions, Collections.singletonList(tierProducerAgent));

        assertThatThrownBy(
                        () ->
                                tieredStorageProducerClient.write(
                                        generateRandomData(bufferSize, random),
                                        new TieredStorageSubpartitionId(0),
                                        Buffer.DataType.DATA_BUFFER,
                                        isBroadcast))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to choose a storage tier");
    }

    @TestTemplate
    void testUpdateMetrics() throws IOException {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        Random random = new Random();

        TestingTierProducerAgent tierProducerAgent = new TestingTierProducerAgent.Builder().build();
        TieredStorageProducerClient tieredStorageProducerClient =
                new TieredStorageProducerClient(
                        numSubpartitions,
                        false,
                        new TestingBufferAccumulator(),
                        null,
                        Collections.singletonList(tierProducerAgent));

        AtomicInteger numWriteBuffers = new AtomicInteger(0);
        AtomicInteger numWriteBytes = new AtomicInteger(0);
        tieredStorageProducerClient.setMetricStatisticsUpdater(
                metricStatistics -> {
                    numWriteBuffers.set(
                            numWriteBuffers.get() + metricStatistics.numWriteBuffersDelta());
                    numWriteBytes.set(numWriteBytes.get() + metricStatistics.numWriteBytesDelta());
                });

        tieredStorageProducerClient.write(
                generateRandomData(bufferSize, random),
                new TieredStorageSubpartitionId(0),
                Buffer.DataType.DATA_BUFFER,
                isBroadcast);

        int numExpectedBuffers = isBroadcast ? numSubpartitions : 1;
        int numExpectedBytes = isBroadcast ? bufferSize * numSubpartitions : bufferSize;
        assertThat(numWriteBuffers.get()).isEqualTo(numExpectedBuffers);
        assertThat(numWriteBytes.get()).isEqualTo(numExpectedBytes);
    }

    @TestTemplate
    void testClose() {
        int numSubpartitions = 10;

        AtomicBoolean isClosed = new AtomicBoolean(false);
        TestingTierProducerAgent tierProducerAgent =
                new TestingTierProducerAgent.Builder()
                        .setCloseRunnable(() -> isClosed.set(true))
                        .build();

        TieredStorageProducerClient tieredStorageProducerClient =
                createTieredStorageProducerClient(
                        numSubpartitions, Collections.singletonList(tierProducerAgent));

        assertThat(isClosed.get()).isFalse();
        tieredStorageProducerClient.close();
        assertThat(isClosed.get()).isTrue();
    }

    private static TieredStorageProducerClient createTieredStorageProducerClient(
            int numSubpartitions, List<TierProducerAgent> tierProducerAgents) {
        TieredStorageProducerClient tieredStorageProducerClient =
                new TieredStorageProducerClient(
                        numSubpartitions,
                        false,
                        new TestingBufferAccumulator(),
                        null,
                        tierProducerAgents);
        tieredStorageProducerClient.setMetricStatisticsUpdater(metricStatistics -> {});
        return tieredStorageProducerClient;
    }
}
