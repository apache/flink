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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.tvf.common.ClockService;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.apache.flink.util.Preconditions.checkState;

/** Utilities to create {@link UnsliceAssigner}s. */
@Internal
public class UnsliceAssigners {

    /**
     * Creates a session window {@link UnsliceAssigner} that assigns elements to windows based on
     * the timestamp.
     *
     * @param rowtimeIndex The index of rowtime field in the input row, {@code -1} if based on
     *     processing time.
     * @param shiftTimeZone The shift timezone of the window, if the proctime or rowtime type is
     *     TIMESTAMP_LTZ, the shift timezone is the timezone user configured in TableConfig, other
     *     cases the timezone is UTC which means never shift when assigning windows.
     * @param gap The session timeout, i.e. the time gap between sessions
     */
    public static SessionUnsliceAssigner session(
            int rowtimeIndex, ZoneId shiftTimeZone, Duration gap) {
        return new SessionUnsliceAssigner(rowtimeIndex, shiftTimeZone, gap.toMillis());
    }

    /** The {@link UnsliceAssigner} for session windows. */
    public static class SessionUnsliceAssigner implements UnsliceAssigner<TimeWindow> {

        private static final long serialVersionUID = 1L;

        private final int rowtimeIndex;
        private final boolean isEventTime;
        private final ZoneId shiftTimeZone;

        private final SessionWindowAssigner innerSessionWindowAssigner;

        public SessionUnsliceAssigner(int rowtimeIndex, ZoneId shiftTimeZone, long sessionGap) {
            this.rowtimeIndex = rowtimeIndex;
            this.shiftTimeZone = shiftTimeZone;
            this.isEventTime = rowtimeIndex >= 0;
            this.innerSessionWindowAssigner =
                    SessionWindowAssigner.withGap(Duration.ofMillis(sessionGap));
            if (isEventTime()) {
                this.innerSessionWindowAssigner.withEventTime();
            } else {
                this.innerSessionWindowAssigner.withProcessingTime();
            }
        }

        @Override
        public MergingWindowAssigner<TimeWindow> getMergingWindowAssigner() {
            return innerSessionWindowAssigner;
        }

        @Override
        public Optional<TimeWindow> assignActualWindow(
                RowData element,
                ClockService clock,
                MergingWindowProcessFunction<?, TimeWindow> windowFunction)
                throws Exception {
            Collection<TimeWindow> windows =
                    windowFunction.assignActualWindows(element, getUtcTimestamp(element, clock));
            checkState(windows.size() <= 1);
            if (windows.size() == 1) {
                return Optional.of(windows.iterator().next());
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<TimeWindow> assignStateNamespace(
                RowData element,
                ClockService clock,
                MergingWindowProcessFunction<?, TimeWindow> windowFunction)
                throws Exception {
            Collection<TimeWindow> windows =
                    windowFunction.assignStateNamespace(element, getUtcTimestamp(element, clock));
            checkState(windows.size() <= 1);
            if (windows.size() == 1) {
                return Optional.of(windows.iterator().next());
            } else {
                return Optional.empty();
            }
        }

        protected long getUtcTimestamp(RowData element, ClockService clock) {
            final long timestamp;
            if (rowtimeIndex >= 0) {
                if (element.isNullAt(rowtimeIndex)) {
                    throw new RuntimeException(
                            "rowtimeIndex should not be null,"
                                    + " please convert it to a non-null long value.");
                }
                // Precision for row timestamp is always 3
                TimestampData rowTime = element.getTimestamp(rowtimeIndex, 3);
                timestamp = toUtcTimestampMills(rowTime.getMillisecond(), shiftTimeZone);
            } else {
                // in processing time mode
                timestamp = toUtcTimestampMills(clock.currentProcessingTime(), shiftTimeZone);
            }
            return timestamp;
        }

        @Override
        public boolean isEventTime() {
            return isEventTime;
        }
    }
}
