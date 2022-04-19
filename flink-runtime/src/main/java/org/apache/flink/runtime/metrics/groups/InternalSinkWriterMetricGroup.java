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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.MetricNames;

/** Special {@link org.apache.flink.metrics.MetricGroup} representing an Operator. */
@Internal
public class InternalSinkWriterMetricGroup extends ProxyMetricGroup<MetricGroup>
        implements SinkWriterMetricGroup {

    // deprecated, use numRecordsSendErrors instead.
    @Deprecated private final Counter numRecordsOutErrors;
    private final Counter numRecordsSendErrors;
    private final Counter numRecordsWritten;
    private final Counter numBytesWritten;
    private final OperatorIOMetricGroup operatorIOMetricGroup;

    private InternalSinkWriterMetricGroup(
            MetricGroup parentMetricGroup, OperatorIOMetricGroup operatorIOMetricGroup) {
        super(parentMetricGroup);
        numRecordsOutErrors = parentMetricGroup.counter(MetricNames.NUM_RECORDS_OUT_ERRORS);
        numRecordsSendErrors = parentMetricGroup.counter(MetricNames.NUM_RECORDS_SEND_ERRORS);
        numRecordsWritten = parentMetricGroup.counter(MetricNames.NUM_RECORDS_SEND);
        numBytesWritten = parentMetricGroup.counter(MetricNames.NUM_BYTES_SEND);
        this.operatorIOMetricGroup = operatorIOMetricGroup;
    }

    public static InternalSinkWriterMetricGroup wrap(OperatorMetricGroup operatorMetricGroup) {
        return new InternalSinkWriterMetricGroup(
                operatorMetricGroup, operatorMetricGroup.getIOMetricGroup());
    }

    @VisibleForTesting
    public static InternalSinkWriterMetricGroup mock(MetricGroup metricGroup) {
        return new InternalSinkWriterMetricGroup(
                metricGroup, UnregisteredMetricsGroup.createOperatorIOMetricGroup());
    }

    @VisibleForTesting
    public static InternalSinkWriterMetricGroup mock(
            MetricGroup metricGroup, OperatorIOMetricGroup operatorIOMetricGroup) {
        return new InternalSinkWriterMetricGroup(metricGroup, operatorIOMetricGroup);
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return operatorIOMetricGroup;
    }

    @Deprecated
    @Override
    public Counter getNumRecordsOutErrorsCounter() {
        return numRecordsOutErrors;
    }

    @Override
    public Counter getNumRecordsSendErrorsCounter() {
        return numRecordsSendErrors;
    }

    @Override
    public Counter getNumRecordsSendCounter() {
        return numRecordsWritten;
    }

    @Override
    public Counter getNumBytesSendCounter() {
        return numBytesWritten;
    }

    @Override
    public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {
        parentMetricGroup.gauge(MetricNames.CURRENT_SEND_TIME, currentSendTimeGauge);
    }
}
