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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ProcessorArchitecture;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

/** Factory for {@link ResultPartition} to use in {@link NettyShuffleEnvironment}. */
public class ResultPartitionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionFactory.class);

    private final ResultPartitionManager partitionManager;

    private final FileChannelManager channelManager;

    private final BufferPoolFactory bufferPoolFactory;

    private final BatchShuffleReadBufferPool batchShuffleReadBufferPool;

    private final ExecutorService batchShuffleReadIOExecutor;

    private final BoundedBlockingSubpartitionType blockingSubpartitionType;

    private final int networkBuffersPerChannel;

    private final int floatingNetworkBuffersPerGate;

    private final int networkBufferSize;

    private final boolean blockingShuffleCompressionEnabled;

    private final String compressionCodec;

    private final int maxBuffersPerChannel;

    private final int sortShuffleMinBuffers;

    private final int sortShuffleMinParallelism;

    private final boolean sslEnabled;

    public ResultPartitionFactory(
            ResultPartitionManager partitionManager,
            FileChannelManager channelManager,
            BufferPoolFactory bufferPoolFactory,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ExecutorService batchShuffleReadIOExecutor,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            int networkBuffersPerChannel,
            int floatingNetworkBuffersPerGate,
            int networkBufferSize,
            boolean blockingShuffleCompressionEnabled,
            String compressionCodec,
            int maxBuffersPerChannel,
            int sortShuffleMinBuffers,
            int sortShuffleMinParallelism,
            boolean sslEnabled) {

        this.partitionManager = partitionManager;
        this.channelManager = channelManager;
        this.networkBuffersPerChannel = networkBuffersPerChannel;
        this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
        this.bufferPoolFactory = bufferPoolFactory;
        this.batchShuffleReadBufferPool = batchShuffleReadBufferPool;
        this.batchShuffleReadIOExecutor = batchShuffleReadIOExecutor;
        this.blockingSubpartitionType = blockingSubpartitionType;
        this.networkBufferSize = networkBufferSize;
        this.blockingShuffleCompressionEnabled = blockingShuffleCompressionEnabled;
        this.compressionCodec = compressionCodec;
        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.sortShuffleMinBuffers = sortShuffleMinBuffers;
        this.sortShuffleMinParallelism = sortShuffleMinParallelism;
        this.sslEnabled = sslEnabled;
    }

    public ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionDeploymentDescriptor desc) {
        return create(
                taskNameWithSubtaskAndId,
                partitionIndex,
                desc.getShuffleDescriptor().getResultPartitionID(),
                desc.getPartitionType(),
                desc.getNumberOfSubpartitions(),
                desc.getMaxParallelism(),
                createBufferPoolFactory(desc.getNumberOfSubpartitions(), desc.getPartitionType()));
    }

    @VisibleForTesting
    public ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionID id,
            ResultPartitionType type,
            int numberOfSubpartitions,
            int maxParallelism,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {
        BufferCompressor bufferCompressor = null;
        if (type.isBlocking() && blockingShuffleCompressionEnabled) {
            bufferCompressor = new BufferCompressor(networkBufferSize, compressionCodec);
        }

        ResultSubpartition[] subpartitions = new ResultSubpartition[numberOfSubpartitions];

        final ResultPartition partition;
        if (type == ResultPartitionType.PIPELINED
                || type == ResultPartitionType.PIPELINED_BOUNDED
                || type == ResultPartitionType.PIPELINED_APPROXIMATE) {
            final PipelinedResultPartition pipelinedPartition =
                    new PipelinedResultPartition(
                            taskNameWithSubtaskAndId,
                            partitionIndex,
                            id,
                            type,
                            subpartitions,
                            maxParallelism,
                            partitionManager,
                            bufferCompressor,
                            bufferPoolFactory);

            BiFunction<Integer, PipelinedResultPartition, PipelinedSubpartition> factory;
            if (type == ResultPartitionType.PIPELINED_APPROXIMATE) {
                factory = PipelinedApproximateSubpartition::new;
            } else {
                factory = PipelinedSubpartition::new;
            }

            for (int i = 0; i < subpartitions.length; i++) {
                subpartitions[i] = factory.apply(i, pipelinedPartition);
            }

            partition = pipelinedPartition;
        } else if (type == ResultPartitionType.BLOCKING
                || type == ResultPartitionType.BLOCKING_PERSISTENT) {
            if (numberOfSubpartitions >= sortShuffleMinParallelism) {
                partition =
                        new SortMergeResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions.length,
                                maxParallelism,
                                batchShuffleReadBufferPool,
                                batchShuffleReadIOExecutor,
                                partitionManager,
                                channelManager.createChannel().getPath(),
                                bufferCompressor,
                                bufferPoolFactory);
            } else {
                final BoundedBlockingResultPartition blockingPartition =
                        new BoundedBlockingResultPartition(
                                taskNameWithSubtaskAndId,
                                partitionIndex,
                                id,
                                type,
                                subpartitions,
                                maxParallelism,
                                partitionManager,
                                bufferCompressor,
                                bufferPoolFactory);

                initializeBoundedBlockingPartitions(
                        subpartitions,
                        blockingPartition,
                        blockingSubpartitionType,
                        networkBufferSize,
                        channelManager,
                        sslEnabled);

                partition = blockingPartition;
            }
        } else {
            throw new IllegalArgumentException("Unrecognized ResultPartitionType: " + type);
        }

        LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);

        return partition;
    }

    private static void initializeBoundedBlockingPartitions(
            ResultSubpartition[] subpartitions,
            BoundedBlockingResultPartition parent,
            BoundedBlockingSubpartitionType blockingSubpartitionType,
            int networkBufferSize,
            FileChannelManager channelManager,
            boolean sslEnabled) {
        int i = 0;
        try {
            for (i = 0; i < subpartitions.length; i++) {
                final File spillFile = channelManager.createChannel().getPathFile();
                subpartitions[i] =
                        blockingSubpartitionType.create(
                                i, parent, spillFile, networkBufferSize, sslEnabled);
            }
        } catch (IOException e) {
            // undo all the work so that a failed constructor does not leave any resources
            // in need of disposal
            releasePartitionsQuietly(subpartitions, i);

            // this is not good, we should not be forced to wrap this in a runtime exception.
            // the fact that the ResultPartition and Task constructor (which calls this) do not
            // tolerate any exceptions
            // is incompatible with eager initialization of resources (RAII).
            throw new FlinkRuntimeException(e);
        }
    }

    private static void releasePartitionsQuietly(ResultSubpartition[] partitions, int until) {
        for (int i = 0; i < until; i++) {
            final ResultSubpartition subpartition = partitions[i];
            ExceptionUtils.suppressExceptions(subpartition::release);
        }
    }

    /**
     * The minimum pool size should be <code>numberOfSubpartitions + 1</code> for two
     * considerations:
     *
     * <p>1. StreamTask can only process input if there is at-least one available buffer on output
     * side, so it might cause stuck problem if the minimum pool size is exactly equal to the number
     * of subpartitions, because every subpartition might maintain a partial unfilled buffer.
     *
     * <p>2. Increases one more buffer for every output LocalBufferPool to avoid performance
     * regression if processing input is based on at-least one buffer available on output side.
     */
    @VisibleForTesting
    SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            int numberOfSubpartitions, ResultPartitionType type) {
        return () -> {
            int maxNumberOfMemorySegments =
                    type.isBounded()
                            ? numberOfSubpartitions * networkBuffersPerChannel
                                    + floatingNetworkBuffersPerGate
                            : Integer.MAX_VALUE;
            int numRequiredBuffers =
                    !type.isPipelined() && numberOfSubpartitions >= sortShuffleMinParallelism
                            ? sortShuffleMinBuffers
                            : numberOfSubpartitions + 1;

            // If the partition type is back pressure-free, we register with the buffer pool for
            // callbacks to release memory.
            return bufferPoolFactory.createBufferPool(
                    numRequiredBuffers,
                    maxNumberOfMemorySegments,
                    numberOfSubpartitions,
                    maxBuffersPerChannel);
        };
    }

    static BoundedBlockingSubpartitionType getBoundedBlockingType() {
        switch (ProcessorArchitecture.getMemoryAddressSize()) {
            case _64_BIT:
                return BoundedBlockingSubpartitionType.FILE_MMAP;
            case _32_BIT:
                return BoundedBlockingSubpartitionType.FILE;
            default:
                LOG.warn("Cannot determine memory architecture. Using pure file-based shuffle.");
                return BoundedBlockingSubpartitionType.FILE;
        }
    }
}
