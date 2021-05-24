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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import javax.annotation.Nonnull;

/**
 * An entry for the {@link StreamElementQueue}. The stream element queue entry stores the {@link
 * StreamElement} for which the stream element queue entry has been instantiated. Furthermore, it
 * allows to set the result of a completed entry through {@link ResultFuture}.
 */
@Internal
interface StreamElementQueueEntry<OUT> extends ResultFuture<OUT> {

    /**
     * True if the stream element queue entry has been completed; otherwise false.
     *
     * @return True if the stream element queue entry has been completed; otherwise false.
     */
    boolean isDone();

    /**
     * Emits the results associated with this queue entry.
     *
     * @param output the output into which to emit.
     */
    void emitResult(TimestampedCollector<OUT> output);

    /**
     * The input element for this queue entry, for which the calculation is performed
     * asynchronously.
     *
     * @return the input element.
     */
    @Nonnull
    StreamElement getInputElement();

    /** Not supported. Exceptions must be handled in the AsyncWaitOperator. */
    @Override
    default void completeExceptionally(Throwable error) {
        throw new UnsupportedOperationException(
                "This result future should only be used to set completed results.");
    }
}
