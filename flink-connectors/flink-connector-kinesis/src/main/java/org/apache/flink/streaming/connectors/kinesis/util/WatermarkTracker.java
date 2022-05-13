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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.Closeable;
import java.io.Serializable;

/**
 * The watermark tracker is responsible for aggregating watermarks across distributed operators.
 *
 * <p>It can be used for sub tasks of a single Flink source as well as multiple heterogeneous
 * sources or other operators.
 *
 * <p>The class essentially functions like a distributed hash table that enclosing operators can use
 * to adopt their processing / IO rates.
 */
@PublicEvolving
public abstract class WatermarkTracker implements Closeable, Serializable {

    public static final long DEFAULT_UPDATE_TIMEOUT_MILLIS = 60_000;

    /**
     * Subtasks that have not provided a watermark update within the configured interval will be
     * considered idle and excluded from target watermark calculation.
     */
    private long updateTimeoutMillis = DEFAULT_UPDATE_TIMEOUT_MILLIS;

    /**
     * Unique id for the subtask. Using string (instead of subtask index) so synchronization can
     * spawn across multiple sources.
     */
    private String subtaskId;

    /** Watermark state. */
    protected static class WatermarkState {
        protected long watermark = Long.MIN_VALUE;
        protected long lastUpdated;

        public long getWatermark() {
            return watermark;
        }

        @Override
        public String toString() {
            return "WatermarkState{watermark=" + watermark + ", lastUpdated=" + lastUpdated + '}';
        }
    }

    protected String getSubtaskId() {
        return this.subtaskId;
    }

    protected long getUpdateTimeoutMillis() {
        return this.updateTimeoutMillis;
    }

    public abstract long getUpdateTimeoutCount();

    /**
     * Subtasks that have not provided a watermark update within the configured interval will be
     * considered idle and excluded from target watermark calculation.
     *
     * @param updateTimeoutMillis
     */
    public void setUpdateTimeoutMillis(long updateTimeoutMillis) {
        this.updateTimeoutMillis = updateTimeoutMillis;
    }

    /**
     * Set the current watermark of the owning subtask and return the global low watermark based on
     * the current state snapshot. Periodically called by the enclosing consumer instance, which is
     * responsible for any timer management etc.
     *
     * @param localWatermark
     * @return
     */
    public abstract long updateWatermark(final long localWatermark);

    protected long getCurrentTime() {
        return System.currentTimeMillis();
    }

    public void open(RuntimeContext context) {
        if (context instanceof StreamingRuntimeContext) {
            this.subtaskId =
                    ((StreamingRuntimeContext) context).getOperatorUniqueID()
                            + "-"
                            + context.getIndexOfThisSubtask();
        } else {
            this.subtaskId = context.getTaskNameWithSubtasks();
        }
    }

    @Override
    public void close() {
        // no work to do here
    }
}
