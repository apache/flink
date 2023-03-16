/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.SnapshotResult;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Allows to write data to the log. Scoped to a single writer (e.g. state backend). */
@Internal
public interface StateChangelogWriter<Handle extends ChangelogStateHandle> extends AutoCloseable {

    /** Get the initial {@link SequenceNumber} that is used for the first element. */
    SequenceNumber initialSequenceNumber();

    /**
     * Get {@link SequenceNumber} to be used for the next element added by {@link #append(int,
     * byte[]) append}.
     */
    SequenceNumber nextSequenceNumber();

    /** Appends the provided **metadata** to this log. No persistency guarantees. */
    void appendMeta(byte[] value) throws IOException;

    /** Appends the provided data to this log. No persistency guarantees. */
    void append(int keyGroup, byte[] value) throws IOException;

    /**
     * Durably persist previously {@link #append(int, byte[]) appended} data starting from the
     * provided {@link SequenceNumber} and up to the latest change added. After this call, one of
     * {@link #confirm(SequenceNumber, SequenceNumber, long) confirm}, {@link #reset(SequenceNumber,
     * SequenceNumber, long) reset}, or {@link #truncate(SequenceNumber) truncate} eventually must
     * be called for the corresponding change set. with reset/truncate/confirm methods?
     *
     * @param from inclusive
     * @param checkpointId to persist
     */
    CompletableFuture<SnapshotResult<Handle>> persist(SequenceNumber from, long checkpointId)
            throws IOException;

    /**
     * Truncate this state changelog to free up the resources and collect any garbage. That means:
     *
     * <ul>
     *   <li>Discard the written state changes - in the provided range [from; to)
     *   <li>Truncate the in-memory view of this changelog - in the range [0; to)
     * </ul>
     *
     * Called upon state materialization. Any ongoing persist calls will not be affected.
     *
     * <p>WARNING: the range [from; to) must not include any range that is included into any
     * checkpoint that is not subsumed or aborted.
     *
     * @param to exclusive
     */
    void truncate(SequenceNumber to);

    /**
     * Mark the given state changes as confirmed by the JM.
     *
     * @param from inclusive
     * @param to exclusive
     * @param checkpointId to confirm
     */
    void confirm(SequenceNumber from, SequenceNumber to, long checkpointId);

    /**
     * Reset the state the given state changes. Called upon abortion so that if requested later then
     * these changes will be re-uploaded.
     */
    void reset(SequenceNumber from, SequenceNumber to, long checkpointId);

    /**
     * Truncate the tail of log and close it. No new appends will be possible. Any appended but not
     * persisted records will be lost.
     *
     * @param from {@link SequenceNumber} from which to truncate the changelog, inclusive
     */
    void truncateAndClose(SequenceNumber from);

    /**
     * Close this log. No new appends will be possible. Any appended but not persisted records will
     * be lost.
     */
    void close();
}
