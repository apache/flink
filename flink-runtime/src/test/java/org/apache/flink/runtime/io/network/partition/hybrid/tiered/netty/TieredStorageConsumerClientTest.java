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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TestingTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierConsumerAgent;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageConsumerClient}. */
class TieredStorageConsumerClientTest {

    private final TieredStoragePartitionId partitionId =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());

    private final TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);

    @Test
    void testGetNextBuffer() {
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(0);
        TestingTieredStorageNettyService nettyService =
                new TestingTieredStorageNettyService.Builder()
                        .setRegisterConsumerFunction(
                                (partitionId, subpartitionId) ->
                                        CompletableFuture.completedFuture(
                                                new TestingNettyConnectionReader.Builder()
                                                        .setReadBufferFunction(segmentId -> buffer)
                                                        .build()))
                        .build();
        TieredStorageConsumerClient tieredStorageConsumerClient =
                new TieredStorageConsumerClient(
                        Collections.singletonList(
                                new TestingTierFactory.Builder()
                                        .setTierConsumerAgentSupplier(MemoryTierConsumerAgent::new)
                                        .build()),
                        Collections.singletonList(
                                new TieredStorageConsumerSpec(partitionId, subpartitionId)),
                        nettyService);
        Optional<Buffer> nextBuffer =
                tieredStorageConsumerClient.getNextBuffer(partitionId, subpartitionId);
        assertThat(nextBuffer).hasValue(buffer);
    }
}
