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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.util.IterableIterator;
import org.apache.flink.util.MathUtils;

import org.apache.commons.math3.util.ArithmeticUtils;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Utilities to create {@link SliceAssigner}s. */
@Internal
public final class SliceAssigners {

    // ------—------—------—------—------—------—------—------—------—------—------—------—------—
    // Utilities
    // ------—------—------—------—------—------—------—------—------—------—------—------—------—

    /**
     * Creates a tumbling window {@link SliceAssigner} that assigns elements to slices of tumbling
     * windows.
     *
     * @param rowtimeIndex the index of rowtime field in the input row, {@code -1} if based on
     *     processing time.
     * @param shiftTimeZone The shift timezone of the window, if the proctime or rowtime type is
     *     TIMESTAMP_LTZ, the shift timezone is the timezone user configured in TableConfig, other
     *     cases the timezone is UTC which means never shift when assigning windows.
     * @param size the size of the generated windows.
     */
    public static TumblingSliceAssigner tumbling(
            int rowtimeIndex, ZoneId shiftTimeZone, Duration size) {
        return new TumblingSliceAssigner(rowtimeIndex, shiftTimeZone, size.toMillis(), 0);
    }

    /**
     * Creates a hopping window {@link SliceAssigner} that assigns elements to slices of hopping
     * windows.
     *
     * @param rowtimeIndex the index of rowtime field in the input row, {@code -1} if based on *
     *     processing time.
     * @param shiftTimeZone The shift timezone of the window, if the proctime or rowtime type is
     *     TIMESTAMP_LTZ, the shift timezone is the timezone user configured in TableConfig, other
     *     cases the timezone is UTC which means never shift when assigning windows.
     * @param slide the slide interval of the generated windows.
     */
    public static HoppingSliceAssigner hopping(
            int rowtimeIndex, ZoneId shiftTimeZone, Duration size, Duration slide) {
        return new HoppingSliceAssigner(
                rowtimeIndex, shiftTimeZone, size.toMillis(), slide.toMillis(), 0);
    }

    /**
     * Creates a cumulative window {@link SliceAssigner} that assigns elements to slices of
     * cumulative windows.
     *
     * @param rowtimeIndex the index of rowtime field in the input row, {@code -1} if based on *
     *     processing time.
     * @param shiftTimeZone The shift timezone of the window, if the proctime or rowtime type is
     *     TIMESTAMP_LTZ, the shift timezone is the timezone user configured in TableConfig, other
     *     cases the timezone is UTC which means never shift when assigning windows.
     * @param step the step interval of the generated windows.
     */
    public static CumulativeSliceAssigner cumulative(
            int rowtimeIndex, ZoneId shiftTimeZone, Duration maxSize, Duration step) {
        return new CumulativeSliceAssigner(
                rowtimeIndex, shiftTimeZone, maxSize.toMillis(), step.toMillis(), 0);
    }

    /**
     * Creates a {@link SliceAssigner} that assigns elements which has been attached window start
     * and window end timestamp to slices. The assigned slice is equal to the given window.
     *
     * @param windowEndIndex the index of window end field in the input row, mustn't be a negative
     *     value.
     * @param innerAssigner the inner assigner which assigns the attached windows
     */
    public static WindowedSliceAssigner windowed(int windowEndIndex, SliceAssigner innerAssigner) {
        return new WindowedSliceAssigner(windowEndIndex, innerAssigner);
    }

    /**
     * Creates a {@link SliceAssigner} that assigns elements which has been attached slice end
     * timestamp.
     *
     * @param sliceEndIndex the index of slice end field in the input row, mustn't be a negative
     *     value.
     * @param innerAssigner the inner assigner which assigns the attached windows
     */
    public static SliceAssigner sliced(int sliceEndIndex, SliceAssigner innerAssigner) {
        if (innerAssigner instanceof SliceSharedAssigner) {
            return new SlicedSharedSliceAssigner(
                    sliceEndIndex, (SliceSharedAssigner) innerAssigner);
        } else {
            return new SlicedUnsharedSliceAssigner(sliceEndIndex, innerAssigner);
        }
    }

    // ------—------—------—------—------—------—------—------—------—------—------—------—------—
    // Slice Assigners
    // ------—------—------—------—------—------—------—------—------—------—------—------—------—

    /** The {@link SliceAssigner} for tumbling windows. */
    public static final class TumblingSliceAssigner extends AbstractSliceAssigner
            implements SliceUnsharedAssigner {
        private static final long serialVersionUID = 1L;

        /** Creates a new {@link TumblingSliceAssigner} with a new specified offset. */
        public TumblingSliceAssigner withOffset(Duration offset) {
            return new TumblingSliceAssigner(rowtimeIndex, shiftTimeZone, size, offset.toMillis());
        }

        private final long size;
        private final long offset;
        private final ReusableListIterable reuseExpiredList = new ReusableListIterable();

        private TumblingSliceAssigner(
                int rowtimeIndex, ZoneId shiftTimeZone, long size, long offset) {
            super(rowtimeIndex, shiftTimeZone);
            checkArgument(
                    size > 0,
                    String.format(
                            "Tumbling Window parameters must satisfy size > 0, but got size %dms.",
                            size));
            checkArgument(
                    Math.abs(offset) < size,
                    String.format(
                            "Tumbling Window parameters must satisfy abs(offset) < size, bot got size %dms and offset %dms.",
                            size, offset));
            this.size = size;
            this.offset = offset;
        }

        @Override
        public long assignSliceEnd(long timestamp) {
            long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
            return start + size;
        }

        @Override
        public long getLastWindowEnd(long sliceEnd) {
            return sliceEnd;
        }

        public long getWindowStart(long windowEnd) {
            return windowEnd - size;
        }

        @Override
        public Iterable<Long> expiredSlices(long windowEnd) {
            reuseExpiredList.reset(windowEnd);
            return reuseExpiredList;
        }

        @Override
        public long getSliceEndInterval() {
            return size;
        }
    }

    /** The {@link SliceAssigner} for hopping windows. */
    public static final class HoppingSliceAssigner extends AbstractSliceAssigner
            implements SliceSharedAssigner {
        private static final long serialVersionUID = 1L;

        /** Creates a new {@link HoppingSliceAssigner} with a new specified offset. */
        public HoppingSliceAssigner withOffset(Duration offset) {
            return new HoppingSliceAssigner(
                    rowtimeIndex, shiftTimeZone, size, slide, offset.toMillis());
        }

        private final long size;
        private final long slide;
        private final long offset;
        private final long sliceSize;
        private final int numSlicesPerWindow;
        private final ReusableListIterable reuseExpiredList = new ReusableListIterable();

        protected HoppingSliceAssigner(
                int rowtimeIndex, ZoneId shiftTimeZone, long size, long slide, long offset) {
            super(rowtimeIndex, shiftTimeZone);
            if (size <= 0 || slide <= 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Hopping Window must satisfy slide > 0 and size > 0, but got slide %dms and size %dms.",
                                slide, size));
            }
            if (size % slide != 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Slicing Hopping Window requires size must be an integral multiple of slide, but got size %dms and slide %dms.",
                                size, slide));
            }
            this.size = size;
            this.slide = slide;
            this.offset = offset;
            this.sliceSize = ArithmeticUtils.gcd(size, slide);
            this.numSlicesPerWindow = MathUtils.checkedDownCast(size / sliceSize);
        }

        @Override
        public long assignSliceEnd(long timestamp) {
            long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, sliceSize);
            return start + sliceSize;
        }

        @Override
        public long getLastWindowEnd(long sliceEnd) {
            return sliceEnd - sliceSize + size;
        }

        @Override
        public long getWindowStart(long windowEnd) {
            return windowEnd - size;
        }

        @Override
        public Iterable<Long> expiredSlices(long windowEnd) {
            // we need to cleanup the first slice of the window
            long windowStart = getWindowStart(windowEnd);
            long firstSliceEnd = windowStart + sliceSize;
            reuseExpiredList.reset(firstSliceEnd);
            return reuseExpiredList;
        }

        @Override
        public long getSliceEndInterval() {
            return sliceSize;
        }

        @Override
        public void mergeSlices(long sliceEnd, MergeCallback callback) throws Exception {
            // the iterable to list all the slices of the triggered window
            Iterable<Long> toBeMerged =
                    new HoppingSlicesIterable(sliceEnd, sliceSize, numSlicesPerWindow);
            // null namespace means use heap data views, instead of state data views
            callback.merge(null, toBeMerged);
        }

        @Override
        public Optional<Long> nextTriggerWindow(long windowEnd, Supplier<Boolean> isWindowEmpty) {
            if (isWindowEmpty.get()) {
                return Optional.empty();
            } else {
                return Optional.of(windowEnd + sliceSize);
            }
        }
    }

    /** The {@link SliceAssigner} for cumulative windows. */
    public static final class CumulativeSliceAssigner extends AbstractSliceAssigner
            implements SliceSharedAssigner {
        private static final long serialVersionUID = 1L;

        /** Creates a new {@link CumulativeSliceAssigner} with a new specified offset. */
        public CumulativeSliceAssigner withOffset(Duration offset) {
            return new CumulativeSliceAssigner(
                    rowtimeIndex, shiftTimeZone, maxSize, step, offset.toMillis());
        }

        private final long maxSize;
        private final long step;
        private final long offset;
        private final ReusableListIterable reuseToBeMergedList = new ReusableListIterable();
        private final ReusableListIterable reuseExpiredList = new ReusableListIterable();

        protected CumulativeSliceAssigner(
                int rowtimeIndex, ZoneId shiftTimeZone, long maxSize, long step, long offset) {
            super(rowtimeIndex, shiftTimeZone);
            if (maxSize <= 0 || step <= 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cumulative Window parameters must satisfy maxSize > 0 and step > 0, but got maxSize %dms and step %dms.",
                                maxSize, step));
            }
            if (maxSize % step != 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cumulative Window requires maxSize must be an integral multiple of step, but got maxSize %dms and step %dms.",
                                maxSize, step));
            }

            this.maxSize = maxSize;
            this.step = step;
            this.offset = offset;
        }

        @Override
        public long assignSliceEnd(long timestamp) {
            long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, step);
            return start + step;
        }

        @Override
        public long getLastWindowEnd(long sliceEnd) {
            long windowStart = getWindowStart(sliceEnd);
            return windowStart + maxSize;
        }

        @Override
        public long getWindowStart(long windowEnd) {
            return TimeWindow.getWindowStartWithOffset(windowEnd - 1, offset, maxSize);
        }

        @Override
        public Iterable<Long> expiredSlices(long windowEnd) {
            long windowStart = getWindowStart(windowEnd);
            long firstSliceEnd = windowStart + step;
            long lastSliceEnd = windowStart + maxSize;
            if (windowEnd == firstSliceEnd) {
                // we share state in the first slice, skip cleanup for the first slice
                reuseExpiredList.clear();
            } else if (windowEnd == lastSliceEnd) {
                // when this is the last slice,
                // we need to cleanup the shared state (i.e. first slice) and the current slice
                reuseExpiredList.reset(windowEnd, firstSliceEnd);
            } else {
                // clean up current slice
                reuseExpiredList.reset(windowEnd);
            }
            return reuseExpiredList;
        }

        @Override
        public long getSliceEndInterval() {
            return step;
        }

        @Override
        public void mergeSlices(long sliceEnd, MergeCallback callback) throws Exception {
            long windowStart = getWindowStart(sliceEnd);
            long firstSliceEnd = windowStart + step;
            if (sliceEnd == firstSliceEnd) {
                // if this is the first slice, there is nothing to merge
                reuseToBeMergedList.clear();
            } else {
                // otherwise, merge the current slice state into the first slice state
                reuseToBeMergedList.reset(sliceEnd);
            }
            callback.merge(firstSliceEnd, reuseToBeMergedList);
        }

        @Override
        public Optional<Long> nextTriggerWindow(long windowEnd, Supplier<Boolean> isWindowEmpty) {
            long nextWindowEnd = windowEnd + step;
            long maxWindowEnd = getWindowStart(windowEnd) + maxSize;
            if (nextWindowEnd > maxWindowEnd) {
                return Optional.empty();
            } else {
                return Optional.of(nextWindowEnd);
            }
        }
    }

    /**
     * The {@link SliceAssigner} for elements have been attached window start and end timestamps.
     */
    public static final class WindowedSliceAssigner implements SliceUnsharedAssigner {
        private static final long serialVersionUID = 1L;

        private final int windowEndIndex;
        private final SliceAssigner innerAssigner;
        private final ReusableListIterable reuseExpiredList = new ReusableListIterable();

        public WindowedSliceAssigner(int windowEndIndex, SliceAssigner innerAssigner) {
            checkArgument(
                    windowEndIndex >= 0,
                    "Windowed slice assigner must have a positive window end index.");
            this.windowEndIndex = windowEndIndex;
            this.innerAssigner = innerAssigner;
        }

        @Override
        public long assignSliceEnd(RowData element, ClockService clock) {
            return element.getLong(windowEndIndex);
        }

        @Override
        public long getLastWindowEnd(long sliceEnd) {
            // we shouldn't use innerAssigner.getLastWindowEnd here,
            // because WindowedSliceAssigner is slice unshared, an attached window can't be
            // shared with other windows and the last window should be itself.
            return sliceEnd;
        }

        @Override
        public long getWindowStart(long windowEnd) {
            return innerAssigner.getWindowStart(windowEnd);
        }

        @Override
        public Iterable<Long> expiredSlices(long windowEnd) {
            reuseExpiredList.reset(windowEnd);
            return reuseExpiredList;
        }

        @Override
        public long getSliceEndInterval() {
            return innerAssigner.getSliceEndInterval();
        }

        @Override
        public boolean isEventTime() {
            // it always works in event-time mode if input row has been attached windows
            return true;
        }
    }

    /**
     * The {@link SliceAssigner} for elements have been attached slice end timestamp, and the slices
     * are shared.
     */
    public static final class SlicedSharedSliceAssigner extends AbstractSlicedSliceAssigner
            implements SliceSharedAssigner {
        private static final long serialVersionUID = 1L;
        private final SliceSharedAssigner innerSharedAssigner;

        public SlicedSharedSliceAssigner(int sliceEndIndex, SliceSharedAssigner innerAssigner) {
            super(sliceEndIndex, innerAssigner);
            this.innerSharedAssigner = innerAssigner;
        }

        @Override
        public void mergeSlices(long sliceEnd, MergeCallback callback) throws Exception {
            innerSharedAssigner.mergeSlices(sliceEnd, callback);
        }

        @Override
        public Optional<Long> nextTriggerWindow(long windowEnd, Supplier<Boolean> isWindowEmpty) {
            return innerSharedAssigner.nextTriggerWindow(windowEnd, isWindowEmpty);
        }

        @Override
        public long getLastWindowEnd(long sliceEnd) {
            return innerAssigner.getLastWindowEnd(sliceEnd);
        }
    }

    /**
     * The {@link SliceAssigner} for elements have been attached slice end timestamp, but the slices
     * are not shared, i.e. the assigned slice is equal to the final window.
     */
    public static final class SlicedUnsharedSliceAssigner extends AbstractSlicedSliceAssigner
            implements SliceUnsharedAssigner {

        private static final long serialVersionUID = 1L;

        public SlicedUnsharedSliceAssigner(int sliceEndIndex, SliceAssigner innerAssigner) {
            super(sliceEndIndex, innerAssigner);
        }

        @Override
        public long getLastWindowEnd(long sliceEnd) {
            // we shouldn't use innerAssigner.getLastWindowEnd here,
            // because SlicedUnsharedSliceAssigner is slice unshared, an attached unshared slice
            // can't be shared with other windows and the last window should be itself.
            return sliceEnd;
        }
    }

    /**
     * A basic implementation of {@link SliceAssigner} for elements have been attached window start
     * and end timestamps.
     */
    private abstract static class AbstractSlicedSliceAssigner implements SliceAssigner {
        private static final long serialVersionUID = 1L;

        private final int sliceEndIndex;
        protected final SliceAssigner innerAssigner;

        public AbstractSlicedSliceAssigner(int sliceEndIndex, SliceAssigner innerAssigner) {
            checkArgument(
                    sliceEndIndex >= 0,
                    "Windowed slice assigner must have a positive window end index.");
            this.sliceEndIndex = sliceEndIndex;
            this.innerAssigner = innerAssigner;
        }

        @Override
        public long assignSliceEnd(RowData element, ClockService clock) {
            return element.getLong(sliceEndIndex);
        }

        @Override
        public long getWindowStart(long windowEnd) {
            return innerAssigner.getWindowStart(windowEnd);
        }

        @Override
        public Iterable<Long> expiredSlices(long windowEnd) {
            return innerAssigner.expiredSlices(windowEnd);
        }

        @Override
        public long getSliceEndInterval() {
            return innerAssigner.getSliceEndInterval();
        }

        @Override
        public boolean isEventTime() {
            // it always works in event-time mode if input row has been attached slices
            return true;
        }
    }

    /** A base implementation for {@link SliceAssigner}. */
    private abstract static class AbstractSliceAssigner implements SliceAssigner {
        private static final long serialVersionUID = 1L;

        protected final int rowtimeIndex;
        protected final boolean isEventTime;
        protected final ZoneId shiftTimeZone;

        protected AbstractSliceAssigner(int rowtimeIndex, ZoneId shiftTimeZone) {
            this.rowtimeIndex = rowtimeIndex;
            this.shiftTimeZone = shiftTimeZone;
            this.isEventTime = rowtimeIndex >= 0;
        }

        public abstract long assignSliceEnd(long timestamp);

        @Override
        public final long assignSliceEnd(RowData element, ClockService clock) {
            final long timestamp;
            if (rowtimeIndex >= 0) {
                timestamp = toUtcTimestampMills(element.getLong(rowtimeIndex), shiftTimeZone);
            } else {
                // in processing time mode
                timestamp = toUtcTimestampMills(clock.currentProcessingTime(), shiftTimeZone);
            }
            return assignSliceEnd(timestamp);
        }

        @Override
        public final boolean isEventTime() {
            return isEventTime;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Private Utilities
    // ------------------------------------------------------------------------------------------

    private static final class ReusableListIterable
            implements IterableIterator<Long>, Serializable {
        private static final long serialVersionUID = 1L;

        private final List<Long> values = new ArrayList<>();
        private int index = 0;

        public void clear() {
            values.clear();
            index = 0;
        }

        public void reset(Long slice) {
            values.clear();
            values.add(slice);
            index = 0;
        }

        public void reset(Long slice1, Long slice2) {
            values.clear();
            values.add(slice1);
            values.add(slice2);
            index = 0;
        }

        @Override
        public Iterator<Long> iterator() {
            index = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return index < values.size();
        }

        @Override
        public Long next() {
            Long value = values.get(index);
            index++;
            return value;
        }
    }

    private static final class HoppingSlicesIterable
            implements IterableIterator<Long>, Serializable {
        private static final long serialVersionUID = 1L;

        private final long sliceSize;
        private long lastSliceEnd;
        private int numSlicesRemaining;

        HoppingSlicesIterable(long lastSliceEnd, long sliceSize, int numSlicesPerWindow) {
            this.lastSliceEnd = lastSliceEnd;
            this.sliceSize = sliceSize;
            this.numSlicesRemaining = numSlicesPerWindow;
        }

        @Override
        public boolean hasNext() {
            return numSlicesRemaining > 0;
        }

        @Override
        public Long next() {
            long slice = lastSliceEnd;
            numSlicesRemaining--;
            lastSliceEnd -= sliceSize;
            return slice;
        }

        @Override
        public Iterator<Long> iterator() {
            return this;
        }
    }

    // avoid to initialize the util
    private SliceAssigners() {}
}
