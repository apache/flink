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

package org.apache.flink.connector.pulsar.sink.writer.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

/** Util class to provide monitor metrics methods to Sink Writer. */
public class PulsarSinkWriterMetrics {
    private final SinkWriterMetricGroup sinkWriterMetricGroup;

    public PulsarSinkWriterMetrics(SinkWriterMetricGroup sinkWriterMetricGroup) {
        this.sinkWriterMetricGroup = sinkWriterMetricGroup;
    }

    public void recordNumBytesOut(long numBytes) {
        sinkWriterMetricGroup.getIOMetricGroup().getNumBytesOutCounter().inc(numBytes);
    }

    public void recordNumRecordsOut(long numRecords) {
        sinkWriterMetricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(numRecords);
    }

    /**
     * Increase the number of error records. Notice that when the delivery guarantee is set to NONE,
     * error records is not tracked and it should show 0.
     */
    public void recordNumRecordOutErrors() {
        sinkWriterMetricGroup.getNumRecordsOutErrorsCounter().inc();
    }

    /**
     * Set the gauge to track the time spent to send the last record. From when sendAsync() is
     * callled to the message get acked. Notice that when the delivery guarantee is set to None,
     * currentSend time is 0 and not tracked.
     *
     * @param timeGauge
     */
    public void setCurrentSendTimeGauge(Gauge<Long> timeGauge) {
        sinkWriterMetricGroup.setCurrentSendTimeGauge(timeGauge);
    }
}
