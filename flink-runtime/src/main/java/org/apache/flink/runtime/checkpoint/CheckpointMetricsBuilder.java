/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.concurrent.FutureUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.concurrent.FutureUtils.checkStateAndGet;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A builder for {@link CheckpointMetrics}.
 *
 * <p>This class is not thread safe, but parts of it can actually be used from different threads.
 */
@NotThreadSafe
public class CheckpointMetricsBuilder {
    private CompletableFuture<Long> bytesProcessedDuringAlignment = new CompletableFuture<>();
    private long bytesPersistedDuringAlignment = -1L;
    private CompletableFuture<Long> alignmentDurationNanos = new CompletableFuture<>();
    private long syncDurationMillis = -1L;
    private long asyncDurationMillis = -1L;
    private long checkpointStartDelayNanos = -1L;
    private boolean unalignedCheckpoint = false;
    private long totalBytesPersisted = -1L;

    public CheckpointMetricsBuilder setBytesProcessedDuringAlignment(
            long bytesProcessedDuringAlignment) {
        checkState(
                this.bytesProcessedDuringAlignment.complete(bytesProcessedDuringAlignment),
                "bytesProcessedDuringAlignment has already been completed by someone else");
        return this;
    }

    public CheckpointMetricsBuilder setBytesProcessedDuringAlignment(
            CompletableFuture<Long> bytesProcessedDuringAlignment) {
        this.bytesProcessedDuringAlignment = bytesProcessedDuringAlignment;
        return this;
    }

    public CompletableFuture<Long> getBytesProcessedDuringAlignment() {
        return bytesProcessedDuringAlignment;
    }

    public CheckpointMetricsBuilder setBytesPersistedDuringAlignment(
            long bytesPersistedDuringAlignment) {
        this.bytesPersistedDuringAlignment = bytesPersistedDuringAlignment;
        return this;
    }

    public long getAlignmentDurationNanosOrDefault() {
        return FutureUtils.getOrDefault(alignmentDurationNanos, -1L);
    }

    public CheckpointMetricsBuilder setAlignmentDurationNanos(long alignmentDurationNanos) {
        checkState(
                this.alignmentDurationNanos.complete(alignmentDurationNanos),
                "alignmentDurationNanos has already been completed by someone else");
        return this;
    }

    public CheckpointMetricsBuilder setAlignmentDurationNanos(
            CompletableFuture<Long> alignmentDurationNanos) {
        checkState(
                !this.alignmentDurationNanos.isDone(),
                "alignmentDurationNanos has already been completed by someone else");
        this.alignmentDurationNanos = alignmentDurationNanos;
        return this;
    }

    public CompletableFuture<Long> getAlignmentDurationNanos() {
        return alignmentDurationNanos;
    }

    public CheckpointMetricsBuilder setSyncDurationMillis(long syncDurationMillis) {
        this.syncDurationMillis = syncDurationMillis;
        return this;
    }

    public long getSyncDurationMillis() {
        return syncDurationMillis;
    }

    public CheckpointMetricsBuilder setAsyncDurationMillis(long asyncDurationMillis) {
        this.asyncDurationMillis = asyncDurationMillis;
        return this;
    }

    public long getAsyncDurationMillis() {
        return asyncDurationMillis;
    }

    public CheckpointMetricsBuilder setCheckpointStartDelayNanos(long checkpointStartDelayNanos) {
        this.checkpointStartDelayNanos = checkpointStartDelayNanos;
        return this;
    }

    public long getCheckpointStartDelayNanos() {
        return checkpointStartDelayNanos;
    }

    public CheckpointMetricsBuilder setUnalignedCheckpoint(boolean unalignedCheckpoint) {
        this.unalignedCheckpoint = unalignedCheckpoint;
        return this;
    }

    public CheckpointMetricsBuilder setTotalBytesPersisted(long totalBytesPersisted) {
        this.totalBytesPersisted = totalBytesPersisted;
        return this;
    }

    public CheckpointMetrics build() {
        return new CheckpointMetrics(
                checkStateAndGet(bytesProcessedDuringAlignment),
                bytesPersistedDuringAlignment,
                checkStateAndGet(alignmentDurationNanos),
                syncDurationMillis,
                asyncDurationMillis,
                checkpointStartDelayNanos,
                unalignedCheckpoint,
                totalBytesPersisted);
    }

    public CheckpointMetrics buildIncomplete() {
        return new CheckpointMetrics(
                bytesProcessedDuringAlignment.getNow(CheckpointMetrics.UNSET),
                bytesPersistedDuringAlignment,
                alignmentDurationNanos.getNow(CheckpointMetrics.UNSET),
                syncDurationMillis,
                asyncDurationMillis,
                checkpointStartDelayNanos,
                unalignedCheckpoint,
                totalBytesPersisted);
    }
}
