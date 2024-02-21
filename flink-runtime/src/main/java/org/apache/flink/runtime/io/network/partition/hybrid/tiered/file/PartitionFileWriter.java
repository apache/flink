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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link PartitionFileWriter} interface defines the write logic for different types of shuffle
 * files.
 */
public interface PartitionFileWriter {

    /**
     * Write the buffers to the partition file. The written buffers may belong to multiple
     * subpartitions, but these buffers will be consecutive in the file.
     *
     * @param partitionId the partition id
     * @param buffersToWrite the buffers to be written to the partition file
     * @return the completable future indicating whether the writing file process has finished. If
     *     the {@link CompletableFuture} is completed, the written process is completed.
     */
    CompletableFuture<Void> write(
            TieredStoragePartitionId partitionId, List<SubpartitionBufferContext> buffersToWrite);

    /** Release all the resources of the {@link PartitionFileWriter}. */
    void release();

    /**
     * The {@link SubpartitionBufferContext} contains all the buffers belonging to one subpartition.
     */
    class SubpartitionBufferContext {

        /** The subpartition id. */
        private final int subpartitionId;

        /** The {@link SegmentBufferContext}s belonging to the subpartition. */
        private final List<SegmentBufferContext> segmentBufferContexts;

        public SubpartitionBufferContext(
                int subpartitionId, List<SegmentBufferContext> segmentBufferContexts) {
            this.subpartitionId = subpartitionId;
            this.segmentBufferContexts = segmentBufferContexts;
        }

        public int getSubpartitionId() {
            return subpartitionId;
        }

        public List<SegmentBufferContext> getSegmentBufferContexts() {
            return segmentBufferContexts;
        }
    }

    /**
     * The {@link SegmentBufferContext} contains all the buffers belonging to the segment. Note that
     * when this indicates whether the segment is finished, the field {@code bufferWithIndexes}
     * should be empty.
     */
    class SegmentBufferContext {

        /** The segment id. */
        private final int segmentId;

        /** All the buffers belonging to the segment. */
        private final List<Tuple2<Buffer, Integer>> bufferAndIndexes;

        /** Whether it is necessary to finish the segment. */
        private final boolean segmentFinished;

        public SegmentBufferContext(
                int segmentId,
                List<Tuple2<Buffer, Integer>> bufferAndIndexes,
                boolean segmentFinished) {
            this.segmentId = segmentId;
            this.bufferAndIndexes = bufferAndIndexes;
            this.segmentFinished = segmentFinished;
        }

        public int getSegmentId() {
            return segmentId;
        }

        public List<Tuple2<Buffer, Integer>> getBufferAndIndexes() {
            return bufferAndIndexes;
        }

        public boolean isSegmentFinished() {
            return segmentFinished;
        }
    }
}
