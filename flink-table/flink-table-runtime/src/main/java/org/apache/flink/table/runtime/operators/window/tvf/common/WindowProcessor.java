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

package org.apache.flink.table.runtime.operators.window.tvf.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.async.tvf.common.AsyncStateWindowProcessor;

import java.io.Serializable;

/**
 * A base window processor provides common methods used for {@link SyncStateWindowProcessor} and
 * {@link AsyncStateWindowProcessor}.
 *
 * @param <W> the window type.
 * @param <C> the context that provides some information for the window processor.
 */
@Internal
public interface WindowProcessor<W, C extends WindowProcessor.Context<W>> extends Serializable {

    /** Initialization method for the function. It is called before the actual working methods. */
    void open(C context) throws Exception;

    /**
     * The tear-down method of the function. It is called after the last call to the main working
     * methods.
     */
    void close() throws Exception;

    /**
     * Initializes the watermark which restores from state. The method is called after open method
     * and before the actual working methods.
     *
     * @param watermark the initial watermark
     */
    void initializeWatermark(long watermark);

    /** Returns the serializer of the window type. */
    TypeSerializer<W> createWindowSerializer();

    // ------------------------------------------------------------------------------------------

    /** Information available in an invocation of methods of {@link WindowProcessor}. */
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

        /** Returns the current {@link InternalTimerService}. */
        InternalTimerService<W> getTimerService();

        /** Returns the current {@link RuntimeContext}. */
        RuntimeContext getRuntimeContext();

        /** Outputs results to downstream operators. */
        void output(RowData result);
    }
}
