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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/** Testing implementation for {@link PartitionFileWriter}. */
public class TestingPartitionFileWriter implements PartitionFileWriter {

    private final BiFunction<
                    TieredStoragePartitionId,
                    List<SubpartitionBufferContext>,
                    CompletableFuture<Void>>
            writeFunction;

    private final Runnable releaseRunnable;

    private TestingPartitionFileWriter(
            BiFunction<
                            TieredStoragePartitionId,
                            List<SubpartitionBufferContext>,
                            CompletableFuture<Void>>
                    writeFunction,
            Runnable releaseRunnable) {
        this.writeFunction = writeFunction;
        this.releaseRunnable = releaseRunnable;
    }

    @Override
    public CompletableFuture<Void> write(
            TieredStoragePartitionId partitionId, List<SubpartitionBufferContext> buffersToWrite) {
        return writeFunction.apply(partitionId, buffersToWrite);
    }

    @Override
    public void release() {
        releaseRunnable.run();
    }

    /** Builder for {@link TestingPartitionFileWriter}. */
    public static class Builder {
        private BiFunction<
                        TieredStoragePartitionId,
                        List<SubpartitionBufferContext>,
                        CompletableFuture<Void>>
                writeFunction =
                        (partitionId, subpartitionBufferContexts) ->
                                FutureUtils.completedVoidFuture();

        private Runnable releaseRunnable = () -> {};

        public TestingPartitionFileWriter.Builder setWriteFunction(
                BiFunction<
                                TieredStoragePartitionId,
                                List<SubpartitionBufferContext>,
                                CompletableFuture<Void>>
                        writeFunction) {
            this.writeFunction = writeFunction;
            return this;
        }

        public TestingPartitionFileWriter.Builder setReleaseRunnable(Runnable releaseRunnable) {
            this.releaseRunnable = releaseRunnable;
            return this;
        }

        public TestingPartitionFileWriter build() {
            return new TestingPartitionFileWriter(writeFunction, releaseRunnable);
        }
    }
}
