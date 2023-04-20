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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The interface for a source reader which is responsible for reading the records from the source
 * splits assigned by {@link SplitEnumerator}.
 *
 * <p>For most non-trivial source reader, it is recommended to use {@link
 * org.apache.flink.connector.base.source.reader.SourceReaderBase SourceReaderBase} which provides
 * an efficient hand-over protocol to avoid blocking I/O inside the task thread and supports various
 * split-threading models.
 *
 * <p>Implementations can provide the following metrics:
 *
 * <ul>
 *   <li>{@link OperatorIOMetricGroup#getNumRecordsInCounter()} (highly recommended)
 *   <li>{@link OperatorIOMetricGroup#getNumBytesInCounter()} (recommended)
 *   <li>{@link SourceReaderMetricGroup#getNumRecordsInErrorsCounter()} (recommended)
 *   <li>{@link SourceReaderMetricGroup#setPendingRecordsGauge(Gauge)}
 *   <li>{@link SourceReaderMetricGroup#setPendingBytesGauge(Gauge)}
 * </ul>
 *
 * @param <T> The type of the record emitted by this source reader.
 * @param <SplitT> The type of the source splits.
 */
@Public
public interface SourceReader<T, SplitT extends SourceSplit>
        extends AutoCloseable, CheckpointListener {

    /** Start the reader. */
    void start();

    /**
     * Poll the next available record into the {@link ReaderOutput}.
     *
     * <p>The implementation must make sure this method is non-blocking.
     *
     * <p>Although the implementation can emit multiple records into the given ReaderOutput, it is
     * recommended not doing so. Instead, emit one record into the ReaderOutput and return a {@link
     * InputStatus#MORE_AVAILABLE} to let the caller thread know there are more records available.
     *
     * @return The InputStatus of the SourceReader after the method invocation.
     */
    InputStatus pollNext(ReaderOutput<T> output) throws Exception;

    /**
     * Checkpoint on the state of the source.
     *
     * @return the state of the source.
     */
    List<SplitT> snapshotState(long checkpointId);

    /**
     * Returns a future that signals that data is available from the reader.
     *
     * <p>Once the future completes, the runtime will keep calling the {@link
     * #pollNext(ReaderOutput)} method until that method returns a status other than {@link
     * InputStatus#MORE_AVAILABLE}. After that, the runtime will again call this method to obtain
     * the next future. Once that completes, it will again call {@link #pollNext(ReaderOutput)} and
     * so on.
     *
     * <p>The contract is the following: If the reader has data available, then all futures
     * previously returned by this method must eventually complete. Otherwise the source might stall
     * indefinitely.
     *
     * <p>It is not a problem to have occasional "false positives", meaning to complete a future
     * even if no data is available. However, one should not use an "always complete" future in
     * cases no data is available, because that will result in busy waiting loops calling {@code
     * pollNext(...)} even though no data is available.
     *
     * @return a future that will be completed once there is a record available to poll.
     */
    CompletableFuture<Void> isAvailable();

    /**
     * Adds a list of splits for this reader to read. This method is called when the enumerator
     * assigns a split via {@link SplitEnumeratorContext#assignSplit(SourceSplit, int)} or {@link
     * SplitEnumeratorContext#assignSplits(SplitsAssignment)}.
     *
     * @param splits The splits assigned by the split enumerator.
     */
    void addSplits(List<SplitT> splits);

    /**
     * This method is called when the reader is notified that it will not receive any further
     * splits.
     *
     * <p>It is triggered when the enumerator calls {@link
     * SplitEnumeratorContext#signalNoMoreSplits(int)} with the reader's parallel subtask.
     */
    void notifyNoMoreSplits();

    /**
     * Handle a custom source event sent by the {@link SplitEnumerator}. This method is called when
     * the enumerator sends an event via {@link SplitEnumeratorContext#sendEventToSourceReader(int,
     * SourceEvent)}.
     *
     * <p>This method has a default implementation that does nothing, because most sources do not
     * require any custom events.
     *
     * @param sourceEvent the event sent by the {@link SplitEnumerator}.
     */
    default void handleSourceEvents(SourceEvent sourceEvent) {}

    /**
     * We have an empty default implementation here because most source readers do not have to
     * implement the method.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /**
     * Pauses or resumes reading of individual source splits.
     *
     * <p>Note that no other methods can be called in parallel, so updating subscriptions can be
     * done atomically. This method is simply providing connectors with more expressive APIs the
     * opportunity to update all subscriptions at once.
     *
     * <p>This is currently used to align the watermarks of splits, if watermark alignment is used
     * and the source reads from more than one split.
     *
     * <p>The default implementation throws an {@link UnsupportedOperationException} where the
     * default implementation will be removed in future releases. To be compatible with future
     * releases, it is recommended to implement this method and override the default implementation.
     *
     * @param splitsToPause the splits to pause
     * @param splitsToResume the splits to resume
     */
    @PublicEvolving
    default void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        throw new UnsupportedOperationException(
                "This source reader does not support pausing or resuming splits which can lead to unaligned splits.\n"
                        + "Unaligned splits are splits where the output watermarks of the splits have diverged more than the allowed limit.\n"
                        + "It is highly discouraged to use unaligned source splits, as this leads to unpredictable\n"
                        + "watermark alignment if there is more than a single split per reader. It is recommended to implement pausing splits\n"
                        + "for this source. At your own risk, you can allow unaligned source splits by setting the\n"
                        + "configuration parameter `pipeline.watermark-alignment.allow-unaligned-source-splits' to true.\n"
                        + "Beware that this configuration parameter will be dropped in a future Flink release.");
    }
}
