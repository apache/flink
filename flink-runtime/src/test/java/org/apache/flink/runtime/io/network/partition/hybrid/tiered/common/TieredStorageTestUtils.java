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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;

import java.util.ArrayList;
import java.util.List;

/** Test utils for tiered storage. */
public class TieredStorageTestUtils {

    public static List<PartitionFileWriter.SubpartitionBufferContext> generateBuffersToWrite(
            int numSubpartitions, int numSegments, int numBuffersPerSegment, int bufferSizeBytes) {
        List<PartitionFileWriter.SubpartitionBufferContext> subpartitionBuffers = new ArrayList<>();
        for (int i = 0; i < numSubpartitions; i++) {
            List<PartitionFileWriter.SegmentBufferContext> segmentBufferContexts =
                    new ArrayList<>();
            for (int j = 0; j < numSegments; j++) {
                List<Tuple2<Buffer, Integer>> bufferAndIndexes = new ArrayList<>();
                // To create buffer contexts with different segment-finish states(true or false),
                // half of the buffers within the segment are assigned to the context where the
                // segment-finish state is false, and the other half of buffers are designated to
                // the context where the segment-finish state is true.
                for (int k = 0; k < numBuffersPerSegment / 2; k++) {
                    bufferAndIndexes.add(
                            new Tuple2<>(
                                    BufferBuilderTestUtils.buildSomeBuffer(bufferSizeBytes), k));
                }
                segmentBufferContexts.add(
                        new PartitionFileWriter.SegmentBufferContext(j, bufferAndIndexes, false));
                bufferAndIndexes = new ArrayList<>();
                for (int k = numBuffersPerSegment / 2; k < numBuffersPerSegment; k++) {
                    bufferAndIndexes.add(
                            new Tuple2<>(
                                    BufferBuilderTestUtils.buildSomeBuffer(bufferSizeBytes), k));
                }
                segmentBufferContexts.add(
                        new PartitionFileWriter.SegmentBufferContext(j, bufferAndIndexes, true));
            }

            subpartitionBuffers.add(
                    new PartitionFileWriter.SubpartitionBufferContext(i, segmentBufferContexts));
        }
        return subpartitionBuffers;
    }
}
