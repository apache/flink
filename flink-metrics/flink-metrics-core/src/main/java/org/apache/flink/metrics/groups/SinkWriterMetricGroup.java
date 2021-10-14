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

package org.apache.flink.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;

/**
 * Pre-defined metrics for sinks.
 *
 * <p>You should only update the metrics in the main operator thread.
 */
public interface SinkWriterMetricGroup extends OperatorMetricGroup {
    /** The total number of records failed to send. */
    Counter getNumRecordsOutErrorsCounter();

    /**
     * Sets an optional gauge for the time it takes to send the last record.
     *
     * <p>This metric is an instantaneous value recorded for the last processed record.
     *
     * <p>If this metric is eagerly calculated, this metric should NOT be updated for each record.
     * Instead, update this metric for each batch of record or sample every X records.
     *
     * <p>Note for asynchronous sinks, the time must be accessible from the main operator thread.
     * For example, a `volatile` field could be set in the async thread and lazily read in the
     * gauge.
     */
    void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge);
}
