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

package org.apache.flink.table.runtime.operators.aggregate.window.combines;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.state.WindowValueState;
import org.apache.flink.table.runtime.util.WindowKey;

import java.io.Serializable;
import java.util.Iterator;

/** The {@link WindowCombineFunction} is used to combine buffered data into state. */
@Internal
public interface WindowCombineFunction {

    /**
     * Combines the buffered data into state based on the given window-key pair.
     *
     * @param windowKey the window-key pair that the buffered data belong to, the window-key object
     *     is reused.
     * @param value the buffered data, the iterator and {@link RowData} objects are reused
     */
    void combine(WindowKey windowKey, Iterator<RowData> value) throws Exception;

    /** Release resources allocated by this combine function. */
    void close() throws Exception;

    // ------------------------------------------------------------------------

    /** A factory that creates a {@link WindowCombineFunction}. */
    @FunctionalInterface
    interface Factory extends Serializable {

        /**
         * Creates a {@link WindowCombineFunction} that can combine buffered data into states.
         *
         * @param runtimeContext the current {@link RuntimeContext}
         * @param timerService the service to register event-time and processing-time timers
         * @param stateBackend the state backend to accessing states
         * @param windowState the window state to flush buffered data into.
         * @param isEventTime indicates whether the operator works in event-time or processing-time
         *     mode, used for register corresponding timers.
         */
        WindowCombineFunction create(
                RuntimeContext runtimeContext,
                InternalTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowValueState<Long> windowState,
                boolean isEventTime)
                throws Exception;
    }
}
