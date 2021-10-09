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

package org.apache.flink.table.runtime.operators.window.combines;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.state.WindowState;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;

/** The {@link RecordsCombiner} is used to combine buffered records into state. */
@Internal
public interface RecordsCombiner {
    /**
     * Combines the buffered data into state based on the given window-key pair.
     *
     * @param windowKey the window-key pair that the buffered data belong to, the window-key object
     *     is reused.
     * @param records the buffered data, the iterator and {@link RowData} objects are reused.
     */
    void combine(WindowKey windowKey, Iterator<RowData> records) throws Exception;

    /** Release resources allocated by this combine function. */
    void close() throws Exception;

    // ------------------------------------------------------------------------

    /** A factory that creates a {@link RecordsCombiner}. */
    @FunctionalInterface
    interface Factory extends Serializable {

        /**
         * Creates a {@link RecordsCombiner} that can combine buffered data into states.
         *
         * @param runtimeContext the current {@link RuntimeContext}
         * @param timerService the service to register event-time and processing-time timers
         * @param stateBackend the state backend to accessing states
         * @param windowState the window state to flush buffered data into.
         * @param isEventTime indicates whether the operator works in event-time or processing-time
         *     mode, used for register corresponding timers.
         */
        RecordsCombiner createRecordsCombiner(
                RuntimeContext runtimeContext,
                WindowTimerService<Long> timerService,
                KeyedStateBackend<RowData> stateBackend,
                WindowState<Long> windowState,
                boolean isEventTime)
                throws Exception;
    }

    /** A factory that creates a {@link RecordsCombiner} used for combining at local stage. */
    @FunctionalInterface
    interface LocalFactory extends Serializable {
        RecordsCombiner createRecordsCombiner(
                RuntimeContext runtimeContext, Collector<RowData> collector) throws Exception;
    }
}
