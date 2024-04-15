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

package org.apache.flink.table.runtime.operators.window.tvf.unslicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.UnsliceWindowAggProcessor;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.tvf.common.ClockService;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner;

import java.util.Optional;

/**
 * A {@link UnsliceAssigner} assigns each element into a single window and not divides the window
 * into finite number of non-overlapping slice. Different with {@link SliceAssigner}, we use the
 * {@link Window} to identifier a window.
 *
 * <p>{@link UnsliceAssigner} is designed for unaligned Windows like session window.
 *
 * <p>Because unaligned Windows are windows determined dynamically based on elements, and its window
 * boundaries are determined based on the messages timestamps and their correlations, some windows
 * may be merged into one.
 *
 * @see UnsliceWindowAggProcessor for more definition of unslice window.
 */
@Internal
public interface UnsliceAssigner<W extends Window> extends WindowAssigner {

    /**
     * Returns the {@link Window} that the given element should belong to be used to trigger on.
     *
     * <p>See more details in {@link MergingWindowProcessFunction#assignActualWindows}.
     *
     * @param element the element to which slice should belong to.
     * @param clock the service to get current processing time.
     * @return if empty, that means the element is late.
     */
    Optional<W> assignActualWindow(
            RowData element, ClockService clock, MergingWindowProcessFunction<?, W> windowFunction)
            throws Exception;

    /**
     * Returns the {@link Window} that the given element should belong to be used as a namespace to
     * restore the state.
     *
     * <p>See more details in {@link MergingWindowProcessFunction#assignStateNamespace}.
     *
     * @param element the element to which slice should belong to.
     * @param clock the service to get current processing time.
     * @return if empty, that means the element is late.
     */
    Optional<W> assignStateNamespace(
            RowData element, ClockService clock, MergingWindowProcessFunction<?, W> windowFunction)
            throws Exception;

    /**
     * Currently, unslice assigner has an inner {@link MergingWindowAssigner} to reuse the logic in
     * {@link
     * org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner} to
     * merge windows.
     */
    MergingWindowAssigner<W> getMergingWindowAssigner();
}
