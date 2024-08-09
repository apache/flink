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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.HashBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.SortBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierFactory;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utils for reading from or writing to tiered storage. */
public class TieredStorageUtils {

    private static final float DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private static final int DEFAULT_NUM_BUFFERS_USE_SORT_ACCUMULATOR_THRESHOLD = 512;

    private static final int DEFAULT_MIN_BUFFERS_PER_GATE = 2;

    private static final int DEFAULT_MIN_BUFFERS_PER_RESULT_PARTITION = 8;

    private static final long DEFAULT_POOL_SIZE_CHECK_INTERVAL = 1000;

    /**
     * When the number of buffers that have been requested exceeds this threshold, trigger the
     * flushing operation in each {@link TierProducerAgent}.
     *
     * @return flush ratio.
     */
    public static float getNumBuffersTriggerFlushRatio() {
        return DEFAULT_NUM_BUFFERS_TRIGGER_FLUSH_RATIO;
    }

    /**
     * Get exclusive buffer number of accumulator.
     *
     * <p>The buffer number is used to compare with the subpartition number to determine the type of
     * {@link BufferAccumulator}.
     *
     * <p>If the exclusive buffer number is larger than (subpartitionNum + 1), the accumulator will
     * use {@link HashBufferAccumulator}. If the exclusive buffer number is equal to or smaller than
     * (subpartitionNum + 1), the accumulator will use {@link SortBufferAccumulator}
     *
     * @return the buffer number.
     */
    public static int getAccumulatorExclusiveBuffers() {
        return DEFAULT_NUM_BUFFERS_USE_SORT_ACCUMULATOR_THRESHOLD;
    }

    /** Get the pool size check interval. */
    public static long getPoolSizeCheckInterval() {
        return DEFAULT_POOL_SIZE_CHECK_INTERVAL;
    }

    /**
     * Get the number of minimum buffers per input gate. It is only used when
     * taskmanager.network.hybrid-shuffle.memory-decoupling.enabled is set to true.
     *
     * @return the buffer number.
     */
    public static int getMinBuffersPerGate() {
        return DEFAULT_MIN_BUFFERS_PER_GATE;
    }

    /**
     * *
     *
     * <p>Get the number of minimum buffers per result partition.
     *
     * @return the buffer number.
     */
    public static int getMinBuffersPerResultPartition() {
        return DEFAULT_MIN_BUFFERS_PER_RESULT_PARTITION;
    }

    public static String getMemoryTierName() {
        return MemoryTierFactory.class.getSimpleName();
    }

    public static String getDiskTierName() {
        return DiskTierFactory.class.getSimpleName();
    }

    public static String getRemoteTierName() {
        return RemoteTierFactory.class.getSimpleName();
    }

    public static ByteBuffer[] generateBufferWithHeaders(
            List<Tuple2<Buffer, Integer>> bufferWithIndexes) {
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithIndexes.size()];

        for (int i = 0; i < bufferWithIndexes.size(); i++) {
            Buffer buffer = bufferWithIndexes.get(i).f0;
            setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
        }
        return bufferWithHeaders;
    }

    private static void setBufferWithHeader(
            Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
    }

    /** Try compress buffer if possible. */
    public static Buffer compressBufferIfPossible(
            Buffer buffer, BufferCompressor bufferCompressor) {
        if (!canBeCompressed(buffer, bufferCompressor)) {
            return buffer;
        }

        return checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    public static boolean canBeCompressed(Buffer buffer, BufferCompressor bufferCompressor) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }

    /**
     * Construct the {@link BufferCompressor} from configuration.
     *
     * <p>Note: This is just a workaround for released version as we can not change the interface of
     * {@link TierFactory}.
     */
    public static BufferCompressor buildBufferCompressor(
            int bufferSizeBytes, Configuration configuration) {
        CompressionCodec compressionCodec =
                configuration.get(NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC);
        boolean compressionEnabled =
                configuration.get(NettyShuffleEnvironmentOptions.BATCH_SHUFFLE_COMPRESSION_ENABLED);
        return compressionEnabled && compressionCodec != CompressionCodec.NONE
                ? new BufferCompressor(bufferSizeBytes, compressionCodec)
                : null;
    }
}
