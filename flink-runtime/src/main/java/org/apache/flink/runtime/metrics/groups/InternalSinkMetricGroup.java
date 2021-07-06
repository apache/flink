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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkMetricGroup;
import org.apache.flink.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

/** Special {@link org.apache.flink.metrics.MetricGroup} representing an Operator. */
@Internal
public class InternalSinkMetricGroup extends ProxyMetricGroup<OperatorMetricGroup>
        implements SinkMetricGroup {

    private final Counter numRecordsOutErrors;

    public InternalSinkMetricGroup(OperatorMetricGroup parentMetricGroup) {
        super(parentMetricGroup);
        numRecordsOutErrors = parentMetricGroup.counter(MetricNames.NUM_RECORDS_OUT_ERRORS);
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return parentMetricGroup.getIOMetricGroup();
    }

    @Override
    public TaskIOMetricGroup getTaskIOMetricGroup() {
        return parentMetricGroup.getTaskIOMetricGroup();
    }

    @Override
    public Counter getNumRecordsOutErrorsCounter() {
        return numRecordsOutErrors;
    }

    @Override
    public Gauge<Long> addCurrentSendTimeGauge(Gauge<Long> currentSendTime) {
        return parentMetricGroup.gauge(MetricNames.CURRENT_SEND_TIME, currentSendTime);
    }
}
