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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;

import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * This class should consolidate all mocking logic for ResultPartitions. While using Mockito
 * internally (for now), the use of Mockito should not leak out of this class.
 */
public enum PartitionTestUtils {
    ;

    public static ResultPartition createPartition() {
        return createPartition(ResultPartitionType.PIPELINED_BOUNDED);
    }

    public static ResultPartition createPartition(ResultPartitionType type) {
        return new ResultPartitionBuilder().setResultPartitionType(type).build();
    }

    public static ResultPartition createPartition(
            ResultPartitionType type, FileChannelManager channelManager) {
        return new ResultPartitionBuilder()
                .setResultPartitionType(type)
                .setFileChannelManager(channelManager)
                .build();
    }

    public static ResultPartition createPartition(
            ResultPartitionType type,
            FileChannelManager channelManager,
            boolean compressionEnabled,
            int networkBufferSize) {
        return new ResultPartitionBuilder()
                .setResultPartitionType(type)
                .setFileChannelManager(channelManager)
                .setBlockingShuffleCompressionEnabled(compressionEnabled)
                .setNetworkBufferSize(networkBufferSize)
                .build();
    }

    public static ResultPartition createPartition(
            NettyShuffleEnvironment environment,
            ResultPartitionType partitionType,
            int numChannels) {
        return new ResultPartitionBuilder()
                .setResultPartitionManager(environment.getResultPartitionManager())
                .setupBufferPoolFactoryFromNettyShuffleEnvironment(environment)
                .setResultPartitionType(partitionType)
                .setNumberOfSubpartitions(numChannels)
                .build();
    }

    public static ResultPartition createPartition(
            NettyShuffleEnvironment environment,
            FileChannelManager channelManager,
            ResultPartitionType partitionType,
            int numChannels) {
        return new ResultPartitionBuilder()
                .setResultPartitionManager(environment.getResultPartitionManager())
                .setupBufferPoolFactoryFromNettyShuffleEnvironment(environment)
                .setFileChannelManager(channelManager)
                .setResultPartitionType(partitionType)
                .setNumberOfSubpartitions(numChannels)
                .build();
    }

    public static ResultSubpartitionView createView(
            ResultSubpartition subpartition, BufferAvailabilityListener listener)
            throws IOException {
        final ResultPartitionManager partitionManager = new ResultPartitionManager();
        final ResultPartition parent = subpartition.parent;
        partitionManager.registerResultPartition(parent);
        return partitionManager.createSubpartitionView(parent.partitionId, 0, listener);
    }

    static void verifyCreateSubpartitionViewThrowsException(
            ResultPartitionProvider partitionManager, ResultPartitionID partitionId)
            throws IOException {
        try {
            partitionManager.createSubpartitionView(
                    partitionId, 0, new NoOpBufferAvailablityListener());

            fail("Should throw a PartitionNotFoundException.");
        } catch (PartitionNotFoundException notFound) {
            assertThat(partitionId, Matchers.is(notFound.getPartitionId()));
        }
    }

    public static ResultPartitionDeploymentDescriptor createPartitionDeploymentDescriptor(
            ResultPartitionType partitionType) {
        ShuffleDescriptor shuffleDescriptor =
                NettyShuffleDescriptorBuilder.newBuilder().buildLocal();
        PartitionDescriptor partitionDescriptor =
                PartitionDescriptorBuilder.newBuilder()
                        .setPartitionId(shuffleDescriptor.getResultPartitionID().getPartitionId())
                        .setPartitionType(partitionType)
                        .build();
        return new ResultPartitionDeploymentDescriptor(
                partitionDescriptor, shuffleDescriptor, 1, true);
    }

    public static PartitionedFile createPartitionedFile(
            String basePath,
            int numSubpartitions,
            int numBuffersPerSubpartition,
            int bufferSize,
            byte[] dataBytes)
            throws Exception {
        List<BufferWithChannel> buffers = new ArrayList<>();
        for (int i = 0; i < numSubpartitions; ++i) {
            for (int j = 0; j < numBuffersPerSubpartition; ++j) {
                Buffer.DataType dataType =
                        j == numBuffersPerSubpartition - 1
                                ? Buffer.DataType.EVENT_BUFFER
                                : Buffer.DataType.DATA_BUFFER;

                MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
                segment.put(0, dataBytes);
                Buffer buffer =
                        new NetworkBuffer(
                                segment, FreeingBufferRecycler.INSTANCE, dataType, bufferSize);
                buffers.add(new BufferWithChannel(buffer, i));
            }
        }

        PartitionedFileWriter fileWriter =
                new PartitionedFileWriter(numSubpartitions, 1024, basePath);
        fileWriter.startNewRegion(false);
        fileWriter.writeBuffers(buffers);
        return fileWriter.finish();
    }
}
