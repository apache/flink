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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Test of different implementation of {@link ResultSubpartitionRecoveredStateHandler}. */
class ResultSubpartitionRecoveredStateHandlerTest extends RecoveredChannelStateHandlerTest {
    private static final int preAllocatedSegments = 3;
    private NetworkBufferPool networkBufferPool;
    private ResultPartition partition;
    private ResultSubpartitionRecoveredStateHandler rstHandler;
    private ResultSubpartitionInfo channelInfo;

    @BeforeEach
    void setUp() throws IOException {
        // given: Segment provider with defined number of allocated segments.
        channelInfo = new ResultSubpartitionInfo(0, 0);

        networkBufferPool = new NetworkBufferPool(preAllocatedSegments, 1024);
        partition = new ResultPartitionBuilder().setNetworkBufferPool(networkBufferPool).build();
        partition.setup();

        rstHandler = buildResultStateHandler(partition);
    }

    private ResultSubpartitionRecoveredStateHandler buildResultStateHandler(
            ResultPartition partition) {
        return new ResultSubpartitionRecoveredStateHandler(
                new ResultPartitionWriter[] {partition},
                false,
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.IDENTITY)
                        }));
    }

    @Test
    void testRecycleBufferBeforeRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<BufferBuilder> bufferWithContext =
                rstHandler.getBuffer(new ResultSubpartitionInfo(0, 0));

        // and: Recycle buffer outside.
        bufferWithContext.buffer.close();

        // Close the partition for flushing the cached recycled buffers to the segment provider.
        partition.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }

    @Test
    void testRecycleBufferAfterRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<BufferBuilder> bufferWithContext =
                rstHandler.getBuffer(channelInfo);

        // and: Pass the buffer to recovery.
        rstHandler.recover(channelInfo, 0, bufferWithContext);

        // Close the partition for flushing the cached recycled buffers to the segment provider.
        partition.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }
}
