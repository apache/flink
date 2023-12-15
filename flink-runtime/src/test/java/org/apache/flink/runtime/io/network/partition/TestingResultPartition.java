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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;

import java.io.IOException;
import java.nio.ByteBuffer;

/** {@link ResultPartition} class for testing purposes. */
public class TestingResultPartition extends ResultPartition {
    private final CreateSubpartitionView createSubpartitionViewFunction;

    public TestingResultPartition(
            ResultPartitionID partitionId,
            ResultPartitionManager partitionManager,
            CreateSubpartitionView createSubpartitionViewFunction) {
        super(
                "test",
                0,
                partitionId,
                ResultPartitionType.PIPELINED,
                0,
                0,
                partitionManager,
                null,
                null);
        this.createSubpartitionViewFunction = createSubpartitionViewFunction;
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {}

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {}

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {}

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {}

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {}

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int index, BufferAvailabilityListener availabilityListener) throws IOException {
        return createSubpartitionViewFunction.createSubpartitionView(index, availabilityListener);
    }

    @Override
    public void flushAll() {}

    @Override
    public void flush(int subpartitionIndex) {}

    @Override
    protected void setupInternal() throws IOException {}

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        return 0;
    }

    @Override
    protected void releaseInternal() {}

    public static TestingResultPartitionBuilder newBuilder() {
        return new TestingResultPartitionBuilder();
    }

    /** Factory for {@link TestingResultPartition}. */
    public static class TestingResultPartitionBuilder {
        private ResultPartitionID resultPartitionId = new ResultPartitionID();
        private ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
        private CreateSubpartitionView createSubpartitionViewFunction =
                (index, availabilityListener) -> null;

        public TestingResultPartitionBuilder setCreateSubpartitionViewFunction(
                CreateSubpartitionView createSubpartitionViewFunction) {
            this.createSubpartitionViewFunction = createSubpartitionViewFunction;
            return this;
        }

        public TestingResultPartitionBuilder setResultPartitionID(
                ResultPartitionID resultPartitionId) {
            this.resultPartitionId = resultPartitionId;
            return this;
        }

        public TestingResultPartitionBuilder setResultPartitionManager(
                ResultPartitionManager resultPartitionManager) {
            this.resultPartitionManager = resultPartitionManager;
            return this;
        }

        public TestingResultPartition build() {
            return new TestingResultPartition(
                    resultPartitionId, resultPartitionManager, createSubpartitionViewFunction);
        }
    }

    /** Testing interface for createSubpartitionView. */
    public interface CreateSubpartitionView {
        ResultSubpartitionView createSubpartitionView(
                int index, BufferAvailabilityListener availabilityListener) throws IOException;
    }
}
