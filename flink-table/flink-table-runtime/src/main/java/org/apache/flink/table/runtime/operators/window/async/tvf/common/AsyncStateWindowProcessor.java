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

package org.apache.flink.table.runtime.operators.window.async.tvf.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.AsyncStateKeyContext;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowProcessor;

import javax.annotation.Nullable;

/** A processor that processes elements for windows with async state. */
@Internal
public interface AsyncStateWindowProcessor<W>
        extends WindowProcessor<W, AsyncStateWindowProcessor.AsyncStateContext<W>> {

    /**
     * Process an element with associated key from the input stream. Returns true if this element is
     * dropped because of late arrival.
     *
     * @param key the key associated with the element
     * @param element The element to process.
     */
    StateFuture<Boolean> processElement(RowData key, RowData element) throws Exception;

    /**
     * Advances the progress time, the progress time is watermark if working in event-time mode, or
     * current processing time if working in processing-time mode.
     *
     * <p>This will potentially flush buffered data into states, because the watermark advancement
     * may be in a very small step, but we don't need to flush buffered data for every watermark
     * advancement.
     *
     * <p>Note: There may be multiple different keys within the buffer. When flushing them to the
     * async state, only the async state request for the current key of the operator will be
     * returned as a {@link StateFuture}. Requests for async states for other keys will not be
     * waited on.
     *
     * @param currentKey the current key of the operator used to return the result of accessing
     *     async state associated with the same key. If it is null, it means that the returns of
     *     asynchronous state requests for all keys will not be awaited.
     * @param progress the current progress time
     * @return the future of the flush operation about current key if the current key is not null,
     *     else a {@link StateFutureUtils#completedVoidFuture()} will be returned.
     */
    StateFuture<Void> advanceProgress(@Nullable RowData currentKey, long progress) throws Exception;

    /** Performs a preparation before checkpoint. This usually flushes buffered data into state. */
    StateFuture<Void> prepareCheckpoint() throws Exception;

    /**
     * Emit results of the given window.
     *
     * <p>Note: the key context has been set.
     *
     * @param timerTimestamp the fired timestamp
     * @param window the window to emit
     */
    StateFuture<Void> fireWindow(long timerTimestamp, W window) throws Exception;

    /**
     * Clear state and resources associated with the given window namespace.
     *
     * <p>Note: the key context has been set.
     *
     * @param timerTimestamp the fired timestamp
     * @param window the window to clear
     */
    StateFuture<Void> clearWindow(long timerTimestamp, W window) throws Exception;

    // ------------------------------------------------------------------------------------------

    /** Information available in an invocation of methods of {@link AsyncStateWindowProcessor}. */
    interface AsyncStateContext<W> extends Context<W> {

        /** Returns the current {@link AsyncStateKeyContext}. */
        AsyncStateKeyContext getAsyncKeyContext();
    }
}
