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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Allows to write data to the log. Scoped to a single writer (e.g. state backend). */
@Internal
public interface StateChangelogWriter<Handle extends StateChangelogHandle<?>>
        extends AutoCloseable {

    /**
     * Get {@link SequenceNumber} of the last element added by {@link #append(int, byte[]) append}.
     */
    SequenceNumber lastAppendedSequenceNumber();

    /** Appends the provided data to this log. No persistency guarantees. */
    void append(int keyGroup, byte[] value);

    /**
     * Durably persist previously {@link #append(int, byte[]) appended} data starting from the
     * provided {@link SequenceNumber} and up to the latest change added. After this call, one of
     * {@link #confirm(SequenceNumber, SequenceNumber) confirm}, {@link #reset(SequenceNumber,
     * SequenceNumber) reset}, or {@link #truncate(SequenceNumber) truncate} eventually must be
     * called for the corresponding change set. with reset/truncate/confirm methods?
     *
     * @param from inclusive
     */
    CompletableFuture<Handle> persist(SequenceNumber from) throws IOException;

    /**
     * Truncate this state changelog to free up resources. Called upon state materialization. Any
     * {@link #persist(SequenceNumber) persisted} state changes will be discarded unless previously
     * {@link #confirm confirmed}.
     *
     * @param to exclusive
     */
    void truncate(SequenceNumber to);

    /**
     * Mark the given state changes as confirmed by the JM.
     *
     * @param from inclusive
     * @param to exclusive
     */
    void confirm(SequenceNumber from, SequenceNumber to);

    /**
     * Reset the state the given state changes. Called upon abortion so that if requested later then
     * these changes will be re-uploaded.
     */
    void reset(SequenceNumber from, SequenceNumber to);

    /**
     * Close this log. No new appends will be possible. Any appended but not persisted records will
     * be lost.
     */
    void close();
}
