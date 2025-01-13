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

package org.apache.flink.table.runtime.operators.aggregate.asyncwindow.buffers;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.AsyncStateKeyContext;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.WindowAsyncState;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZoneId;

/**
 * A buffer that buffers data in memory and flushes many values to async state together at a time to
 * avoid frequently accessing async state, or flushes to output to reduce shuffling data.
 */
public interface AsyncStateWindowBuffer {

    /**
     * Adds an element with associated key into the buffer. The buffer may temporarily buffer the
     * element, or immediately write it to the stream.
     *
     * <p>It may be that adding this element fills up an internal buffer and causes the buffer
     * flushing of a batch of internally buffered elements.
     *
     * @param dataKey the key associated with the element
     * @param element The element to add.
     * @throws Exception Thrown, if the element cannot be added to the buffer, or if the flushing
     *     throws an exception.
     */
    StateFuture<Void> addElement(RowData dataKey, long window, RowData element) throws Exception;

    /**
     * Advances the progress time, the progress time is watermark if working in event-time mode, or
     * current processing time if working in processing-time mode.
     *
     * <p>This will potentially flush buffered data into states or to the output stream, because the
     * watermark advancement may be in a very small step, but we don't need to flush buffered data
     * for every watermark advancement.
     *
     * <p>Note: There may be multiple different keys within the buffer. When flushing them to the
     * async state, only the async state request for the current key of the operator will be
     * returned as a {@link StateFuture}. Requests for async states for other keys will not be
     * waited on.
     *
     * @param currentKey the current key when processing and is used to return the result of
     *     accessing async state associated with the same key. If it is null, it means that the
     *     returns of asynchronous state requests for all keys will not be awaited.
     * @param progress the current progress time
     * @return the future of the flush operation about current key if the current key is not null,
     *     else a {@link StateFutureUtils#completedVoidFuture()} will be returned.
     */
    StateFuture<Void> advanceProgress(@Nullable RowData currentKey, long progress) throws Exception;

    /**
     * Flushes all intermediate buffered data to the underlying backend or output stream.
     *
     * <p>Note: There may be multiple different keys within the buffer. When flushing them to the
     * async state, only the async state request for the current key of the operator will be
     * returned as a {@link StateFuture}. Requests for async states for other keys will not be
     * waited on.
     *
     * @param currentKey the current key when processing and is used to return the result of
     *     accessing async state associated with the same key. If it is null, it means that the
     *     returns of asynchronous state requests for all keys will not be awaited.
     * @return the future of the flush operation about current key if the current key is not null,
     *     else a {@link StateFutureUtils#completedVoidFuture()} will be returned.
     * @throws Exception Thrown if the buffer cannot be flushed, or if the output stream throws an
     *     exception.
     */
    StateFuture<Void> flush(@Nullable RowData currentKey) throws Exception;

    /** Release resources allocated by this buffer. */
    void close() throws Exception;

    // ------------------------------------------------------------------------

    /** A factory that creates a {@link WindowBuffer} with async state. */
    @FunctionalInterface
    interface Factory extends Serializable {

        /**
         * Creates a {@link WindowBuffer} that buffers elements in memory before flushing.
         *
         * @param operatorOwner the owner of the operator
         * @param memoryManager the manager that governs memory by Flink framework
         * @param memorySize the managed memory size can be used by this operator
         * @param runtimeContext the current {@link RuntimeContext}
         * @param timerService the service to register event-time and processing-time timers
         * @param keyContext the state context to accessing states
         * @param windowState the window async state to flush buffered data into.
         * @param isEventTime indicates whether the operator works in event-time or processing-time
         *     mode, used for register corresponding timers.
         * @param shiftTimeZone the shift timezone of the window
         * @throws IOException thrown if the buffer can't be opened
         */
        AsyncStateWindowBuffer create(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                AsyncStateKeyContext keyContext,
                WindowAsyncState<Long> windowState,
                boolean isEventTime,
                ZoneId shiftTimeZone)
                throws Exception;
    }
}
