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

package org.apache.flink.table.runtime.operators.aggregate.window.buffers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.state.WindowState;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZoneId;

/**
 * A buffer that buffers data in memory and flushes many values to state together at a time to avoid
 * frequently accessing state, or flushes to output to reduce shuffling data.
 */
@Internal
public interface WindowBuffer {

    /**
     * Adds an element with associated key into the buffer. The buffer may temporarily buffer the
     * element, or immediately write it to the stream.
     *
     * <p>It may be that adding this element fills up an internal buffer and causes the buffer
     * flushing of a batch of internally buffered elements.
     *
     * @param key the key associated with the element
     * @param element The element to add.
     * @throws Exception Thrown, if the element cannot be added to the buffer, or if the flushing
     *     throws an exception.
     */
    void addElement(RowData key, long window, RowData element) throws Exception;

    /**
     * Advances the progress time, the progress time is watermark if working in event-time mode, or
     * current processing time if working in processing-time mode.
     *
     * <p>This will potentially flush buffered data into states or to the output stream, because the
     * watermark advancement may be in a very small step, but we don't need to flush buffered data
     * for every watermark advancement.
     *
     * @param progress the current progress time
     */
    void advanceProgress(long progress) throws Exception;

    /**
     * Flushes all intermediate buffered data to the underlying backend or output stream.
     *
     * @throws Exception Thrown if the buffer cannot be flushed, or if the output stream throws an
     *     exception.
     */
    void flush() throws Exception;

    /** Release resources allocated by this buffer. */
    void close() throws Exception;

    // ------------------------------------------------------------------------

    /** A factory that creates a {@link WindowBuffer}. */
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
         * @param stateBackend the state backend to accessing states
         * @param windowState the window state to flush buffered data into.
         * @param isEventTime indicates whether the operator works in event-time or processing-time
         *     mode, used for register corresponding timers.
         * @param shiftTimeZone the shit timezone of the window
         * @throws IOException thrown if the buffer can't be opened
         */
        WindowBuffer create(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowState<Long> windowState,
                boolean isEventTime,
                ZoneId shiftTimeZone)
                throws Exception;
    }

    /** A factory that creates a {@link WindowBuffer}. */
    @FunctionalInterface
    interface LocalFactory extends Serializable {

        /**
         * Creates a {@link WindowBuffer} for local window that buffers elements in memory before
         * flushing.
         *
         * @param operatorOwner the owner of the operator
         * @param memoryManager the manager that governs memory by Flink framework
         * @param memorySize the managed memory size can be used by this operator
         * @param collector collector to emit records
         * @param shiftTimeZone the shit timezone of the window
         * @throws IOException thrown if the buffer can't be opened
         */
        WindowBuffer create(
                Object operatorOwner,
                MemoryManager memoryManager,
                long memorySize,
                RuntimeContext runtimeContext,
                Collector<RowData> collector,
                ZoneId shiftTimeZone)
                throws Exception;
    }
}
