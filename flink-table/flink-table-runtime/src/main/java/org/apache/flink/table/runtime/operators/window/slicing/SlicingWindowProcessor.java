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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/** A processor that processes elements for slicing windows. */
@Internal
public interface SlicingWindowProcessor<W> extends Serializable {

    /** Initialization method for the function. It is called before the actual working methods. */
    void open(Context<W> context) throws Exception;

    /**
     * Process an element with associated key from the input stream. Returns true if this element is
     * dropped because of late arrival.
     *
     * @param key the key associated with the element
     * @param element The element to process.
     */
    boolean processElement(RowData key, RowData element) throws Exception;

    /**
     * Advances the progress time, the progress time is watermark if working in event-time mode, or
     * current processing time if working in processing-time mode.
     *
     * <p>This will potentially flush buffered data into states, because the watermark advancement
     * may be in a very small step, but we don't need to flush buffered data for every watermark
     * advancement.
     *
     * @param progress the current progress time
     */
    void advanceProgress(long progress) throws Exception;

    /** Performs a preparation before checkpoint. This usually flushes buffered data into state. */
    void prepareCheckpoint() throws Exception;

    /**
     * Emit results of the given window.
     *
     * <p>Note: the key context has been set.
     *
     * @param window the window to emit
     */
    void fireWindow(W window) throws Exception;

    /**
     * Clear state and resources associated with the given window namespace.
     *
     * <p>Note: the key context has been set.
     *
     * @param window the window to clear
     */
    void clearWindow(W window) throws Exception;

    /**
     * The tear-down method of the function. It is called after the last call to the main working
     * methods.
     */
    void close() throws Exception;

    /** Returns the serializer of the window type. */
    TypeSerializer<W> createWindowSerializer();

    // ------------------------------------------------------------------------------------------

    /** Information available in an invocation of methods of {@link SlicingWindowProcessor}. */
    interface Context<W> {

        /**
         * Returns the object instance of this operator which is used for tracking managed memories
         * used by this operator.
         */
        Object getOperatorOwner();

        /** Returns the current {@link MemoryManager}. */
        MemoryManager getMemoryManager();

        /** Returns the managed memory size can be used by this operator. */
        long getMemorySize();

        /** Returns the current {@link KeyedStateBackend}. */
        KeyedStateBackend<RowData> getKeyedStateBackend();

        /** Returns the current {@link InternalTimerService}. */
        InternalTimerService<W> getTimerService();

        /** Returns the current {@link RuntimeContext}. */
        RuntimeContext getRuntimeContext();

        /** Outputs results to downstream operators. */
        void output(RowData result);
    }
}
