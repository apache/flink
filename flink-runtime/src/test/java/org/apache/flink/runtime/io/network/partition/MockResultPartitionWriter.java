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

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** Dummy behaviours of {@link ResultPartitionWriter} for test purpose. */
public class MockResultPartitionWriter implements ResultPartitionWriter {

    private final ResultPartitionID partitionId = new ResultPartitionID();

    @Override
    public void setup() {}

    @Override
    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    @Override
    public int getNumberOfSubpartitions() {
        return 1;
    }

    @Override
    public int getNumTargetKeyGroups() {
        return 1;
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {}

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {}

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {}

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int index, BufferAvailabilityListener availabilityListener) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {}

    @Override
    public void flushAll() {}

    @Override
    public void flush(int subpartitionIndex) {}

    @Override
    public void fail(@Nullable Throwable throwable) {}

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public void release(Throwable cause) {}

    @Override
    public boolean isReleased() {
        return false;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }

    @Override
    public void close() {}
}
