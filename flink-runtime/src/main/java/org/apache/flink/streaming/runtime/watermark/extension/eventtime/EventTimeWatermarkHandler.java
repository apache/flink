/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermark.extension.eventtime;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is used to handle {@link EventTimeExtension} related watermarks in operator, such as
 * {@link EventTimeExtension#EVENT_TIME_WATERMARK_DECLARATION} and {@link
 * EventTimeExtension#IDLE_STATUS_WATERMARK_DECLARATION}. It will emit event time watermark and idle
 * status to downstream operators according to received watermarks.
 */
public class EventTimeWatermarkHandler {

    /** number of input of operator, it should between 1 and 2 in current design. */
    private final int numOfInput;

    private final Output<?> output;

    private final List<EventTimeWithIdleStatus> eventTimePerInput;

    /**
     * time service manager is used to advance event time in operator, and it may be null if the
     * operator is not keyed.
     */
    @Nullable private final InternalTimeServiceManager<?> timeServiceManager;

    private long lastEmitWatermark = Long.MIN_VALUE;

    private boolean lastEmitIdleStatus = false;

    /** A bitset to record whether the watermark has been received from each input. */
    private final BitSet hasReceiveWatermarks;

    public EventTimeWatermarkHandler(
            int numOfInput,
            Output<?> output,
            @Nullable InternalTimeServiceManager<?> timeServiceManager) {
        checkArgument(numOfInput >= 1 && numOfInput <= 2, "numOfInput should between 1 and 2");
        this.numOfInput = numOfInput;
        this.output = output;
        this.eventTimePerInput = new ArrayList<>(numOfInput);
        for (int i = 0; i < numOfInput; i++) {
            eventTimePerInput.add(new EventTimeWithIdleStatus());
        }
        this.timeServiceManager = timeServiceManager;
        this.hasReceiveWatermarks = new BitSet(numOfInput);
    }

    private EventTimeUpdateStatus processEventTime(long timestamp, int inputIndex)
            throws Exception {
        checkState(inputIndex < numOfInput);
        hasReceiveWatermarks.set(inputIndex);
        eventTimePerInput.get(inputIndex).setEventTime(timestamp);
        eventTimePerInput.get(inputIndex).setIdleStatus(false);

        return tryAdvanceEventTimeAndEmitWatermark();
    }

    private EventTimeUpdateStatus tryAdvanceEventTimeAndEmitWatermark() throws Exception {
        // if current event time is larger than last emit watermark, emit it
        long currentEventTime = getCurrentEventTime();
        if (currentEventTime > lastEmitWatermark
                && hasReceiveWatermarks.cardinality() == numOfInput) {
            output.emitWatermark(
                    new WatermarkEvent(
                            EventTimeExtension.EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                                    currentEventTime),
                            false));
            lastEmitWatermark = currentEventTime;
            if (timeServiceManager != null) {
                timeServiceManager.advanceWatermark(new Watermark(currentEventTime));
            }
            return EventTimeUpdateStatus.ofUpdatedWatermark(lastEmitWatermark);
        }
        return EventTimeUpdateStatus.NO_UPDATE;
    }

    private void processEventTimeIdleStatus(boolean isIdle, int inputIndex) {
        checkState(inputIndex < numOfInput);
        hasReceiveWatermarks.set(inputIndex);
        eventTimePerInput.get(inputIndex).setIdleStatus(isIdle);
        tryEmitEventTimeIdleStatus();
    }

    private void tryEmitEventTimeIdleStatus() {
        // emit idle status if current idle status is different from last emit
        boolean inputIdle = isAllInputIdle();
        if (inputIdle != lastEmitIdleStatus) {
            output.emitWatermark(
                    new WatermarkEvent(
                            EventTimeExtension.IDLE_STATUS_WATERMARK_DECLARATION.newWatermark(
                                    inputIdle),
                            false));
            lastEmitIdleStatus = inputIdle;
        }
    }

    private long getCurrentEventTime() {
        long currentEventTime = Long.MAX_VALUE;
        for (EventTimeWithIdleStatus eventTimeWithIdleStatus : eventTimePerInput) {
            if (!eventTimeWithIdleStatus.isIdle()) {
                currentEventTime =
                        Math.min(currentEventTime, eventTimeWithIdleStatus.getEventTime());
            }
        }
        return currentEventTime;
    }

    private boolean isAllInputIdle() {
        boolean allInputIsIdle = true;
        for (EventTimeWithIdleStatus eventTimeWithIdleStatus : eventTimePerInput) {
            allInputIsIdle &= eventTimeWithIdleStatus.isIdle();
        }
        return allInputIsIdle;
    }

    public long getLastEmitWatermark() {
        return lastEmitWatermark;
    }

    /**
     * Process EventTimeWatermark/IdleStatusWatermark.
     *
     * <p>It's caller's responsibility to check whether the watermark is
     * EventTimeWatermark/IdleStatusWatermark.
     *
     * @return the status of event time watermark update.
     */
    public EventTimeUpdateStatus processWatermark(
            org.apache.flink.api.common.watermark.Watermark watermark, int inputIndex)
            throws Exception {
        if (EventTimeExtension.isEventTimeWatermark(watermark.getIdentifier())) {
            long timestamp = ((LongWatermark) watermark).getValue();
            return this.processEventTime(timestamp, inputIndex);
        } else if (EventTimeExtension.isIdleStatusWatermark(watermark.getIdentifier())) {
            boolean isIdle = ((BoolWatermark) watermark).getValue();
            this.processEventTimeIdleStatus(isIdle, inputIndex);
        }
        return EventTimeUpdateStatus.NO_UPDATE;
    }

    /** This class represents event-time updated status. */
    public static class EventTimeUpdateStatus {

        public static final EventTimeUpdateStatus NO_UPDATE = new EventTimeUpdateStatus(false, -1L);

        private final boolean isEventTimeUpdated;

        private final long newEventTime;

        private EventTimeUpdateStatus(boolean isEventTimeUpdated, long newEventTime) {
            this.isEventTimeUpdated = isEventTimeUpdated;
            this.newEventTime = newEventTime;
        }

        public boolean isEventTimeUpdated() {
            return isEventTimeUpdated;
        }

        public long getNewEventTime() {
            return newEventTime;
        }

        public static EventTimeUpdateStatus ofUpdatedWatermark(long newEventTime) {
            return new EventTimeUpdateStatus(true, newEventTime);
        }
    }

    static class EventTimeWithIdleStatus {
        private long eventTime = Long.MIN_VALUE;
        private boolean isIdle = false;

        public long getEventTime() {
            return eventTime;
        }

        public void setEventTime(long eventTime) {
            this.eventTime = Math.max(this.eventTime, eventTime);
        }

        public boolean isIdle() {
            return isIdle;
        }

        public void setIdleStatus(boolean idle) {
            isIdle = idle;
        }
    }
}
