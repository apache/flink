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
 * the adaptive scheduler state, in timestamp, out timestamp, the exception if occurred during the
 * adaptive scheduler state.
 */
public class SchedulerStateSpan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String state;

    @Nullable private final Long inTimestamp;

    @Nullable private final Long outTimestamp;

    @Nullable private final Long duration;

    @Nullable private String stringedException;

    public SchedulerStateSpan(
            String state,
            Long logicStartMillis,
            Long logicEndMillis,
            Long duration,
            String stringedException) {
        this.state = Preconditions.checkNotNull(state);
        this.inTimestamp = logicStartMillis;
        this.outTimestamp = logicEndMillis;
        this.duration = duration;
        this.stringedException = stringedException;
    }

    @Nullable
    public Long getOutTimestamp() {
        return outTimestamp;
    }

    public void setStringedException(@Nullable String stringedException) {
        this.stringedException = stringedException;
    }

    public String getState() {
        return state;
    }

    @Nullable
    public Long getInTimestamp() {
        return inTimestamp;
    }

    @Nullable
    public Long getDuration() {
        return duration;
    }

    @Nullable
    public String getStringedException() {
        return stringedException;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchedulerStateSpan that = (SchedulerStateSpan) o;
        return Objects.equals(state, that.state)
                && Objects.equals(inTimestamp, that.inTimestamp)
                && Objects.equals(outTimestamp, that.outTimestamp)
                && Objects.equals(duration, that.duration)
                && Objects.equals(stringedException, that.stringedException);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, inTimestamp, outTimestamp, duration, stringedException);
    }

    @Override
    public String toString() {
        return "SchedulerStateSpan{"
                + "state='"
                + state
                + '\''
                + ", inTimestamp="
                + inTimestamp
                + ", outTimestamp="
                + outTimestamp
                + ", duration="
                + duration
                + ", stringedException='"
                + stringedException
                + '\''
                + '}';
    }
}
