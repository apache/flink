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

package org.apache.flink.table.runtime.operators.window.tvf.slicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.tvf.common.ClockService;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAssigner;

/**
 * A {@link SliceAssigner} assigns element into a single slice. Note that we use the slice end
 * timestamp to identify a slice.
 *
 * <p>Note: {@link SliceAssigner} servers as a base interface. Concrete assigners should implement
 * interface {@link SliceSharedAssigner} or {@link SliceUnsharedAssigner}.
 *
 * @see SlicingWindowProcessor for more definition of slice.
 */
@Internal
public interface SliceAssigner extends WindowAssigner {

    /**
     * Returns the end timestamp of a slice that the given element should belong.
     *
     * @param element the element to which slice should belong to.
     * @param clock the service to get current processing time.
     */
    long assignSliceEnd(RowData element, ClockService clock);

    /**
     * Returns the last window which the slice belongs to. The window and slices are both identified
     * by the end timestamp.
     */
    long getLastWindowEnd(long sliceEnd);

    /** Returns the corresponding window start timestamp of the given window end timestamp. */
    long getWindowStart(long windowEnd);

    /**
     * Returns an iterator of slices to expire when the given window is emitted. The window and
     * slices are both identified by the end timestamp.
     *
     * @param windowEnd the end timestamp of window emitted.
     */
    Iterable<Long> expiredSlices(long windowEnd);

    /**
     * Returns the interval of slice ends, i.e. the step size to advance of the slice end when a new
     * slice assigned.
     */
    long getSliceEndInterval();
}
