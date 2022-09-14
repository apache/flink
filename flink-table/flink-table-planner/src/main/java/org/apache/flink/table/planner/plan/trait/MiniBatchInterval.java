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

package org.apache.flink.table.planner.plan.trait;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The pojo class that describes the mini-batch interval and mini-batch mode. */
public class MiniBatchInterval {
    public static final String FIELD_NAME_INTERVAL = "interval";
    public static final String FIELD_NAME_MODE = "mode";

    /** interval of mini-batch. */
    @JsonProperty(FIELD_NAME_INTERVAL)
    private final long interval;
    /** The type of mini-batch: rowtime/proctime. */
    @JsonProperty(FIELD_NAME_MODE)
    private final MiniBatchMode mode;

    /** default none value. */
    public static final MiniBatchInterval NONE = new MiniBatchInterval(0L, MiniBatchMode.None);

    /**
     * specific for cases when there exists nodes require watermark but mini-batch interval of
     * watermark is disabled by force, e.g. existing window aggregate.
     *
     * <p>The difference between NONE AND NO_MINIBATCH is when merging with other miniBatchInterval,
     * NONE case yields other miniBatchInterval, while NO_MINIBATCH case yields NO_MINIBATCH.
     */
    public static final MiniBatchInterval NO_MINIBATCH =
            new MiniBatchInterval(-1L, MiniBatchMode.None);

    @JsonCreator
    public MiniBatchInterval(
            @JsonProperty(FIELD_NAME_INTERVAL) long interval,
            @JsonProperty(FIELD_NAME_MODE) MiniBatchMode mode) {
        this.interval = interval;
        this.mode = checkNotNull(mode);
    }

    public long getInterval() {
        return interval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MiniBatchInterval that = (MiniBatchInterval) o;
        return interval == that.interval && mode == that.mode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, mode);
    }

    public MiniBatchMode getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return "MiniBatchInterval{" + "interval=" + interval + ", mode=" + mode + '}';
    }
}
