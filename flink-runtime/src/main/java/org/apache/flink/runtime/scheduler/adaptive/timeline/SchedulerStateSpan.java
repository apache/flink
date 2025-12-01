/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Utils class to record the information of a scheduler state that contains the span of the time of
 * the adaptive scheduler state, enter timestamp, leave timestamp, the exception if occurred during
 * the adaptive scheduler state.
 */
public class SchedulerStateSpan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String state;

    @Nullable private final Long enterTimestamp;

    @Nullable private final Long leaveTimestamp;

    @Nullable private final Long duration;

    @Nullable private String stringifiedException;

    public SchedulerStateSpan(
            String state,
            Long logicEnterMillis,
            Long logicLeaveMillis,
            Long duration,
            String stringifiedException) {
        this.state = Preconditions.checkNotNull(state);
        this.enterTimestamp = logicEnterMillis;
        this.leaveTimestamp = logicLeaveMillis;
        this.duration = duration;
        this.stringifiedException = stringifiedException;
    }

    @Nullable
    public Long getLeaveTimestamp() {
        return leaveTimestamp;
    }

    public void setStringifiedException(@Nullable String stringifiedException) {
        this.stringifiedException = stringifiedException;
    }

    public String getState() {
        return state;
    }

    @Nullable
    public Long getEnterTimestamp() {
        return enterTimestamp;
    }

    @Nullable
    public Long getDuration() {
        return duration;
    }

    @Nullable
    public String getStringifiedException() {
        return stringifiedException;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchedulerStateSpan that = (SchedulerStateSpan) o;
        return Objects.equals(state, that.state)
                && Objects.equals(enterTimestamp, that.enterTimestamp)
                && Objects.equals(leaveTimestamp, that.leaveTimestamp)
                && Objects.equals(duration, that.duration)
                && Objects.equals(stringifiedException, that.stringifiedException);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, enterTimestamp, leaveTimestamp, duration, stringifiedException);
    }

    @Override
    public String toString() {
        return "SchedulerStateSpan{"
                + "state='"
                + state
                + '\''
                + ", enterTimestamp="
                + enterTimestamp
                + ", leaveTimestamp="
                + leaveTimestamp
                + ", duration="
                + duration
                + ", stringifiedException='"
                + stringifiedException
                + '\''
                + '}';
    }
}
