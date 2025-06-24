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
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

/** Special {@link MetricGroup} representing an Operator. */
@Internal
public class InternalSinkCommitterMetricGroup extends ProxyMetricGroup<MetricGroup>
        implements SinkCommitterMetricGroup {

    private final Counter numCommittablesTotal;
    private final Counter numCommittablesFailure;
    private final Counter numCommittablesRetry;
    private final Counter numCommitatblesSuccess;
    private final Counter numCommitatblesAlreadyCommitted;
    private final OperatorIOMetricGroup operatorIOMetricGroup;

    @VisibleForTesting
    InternalSinkCommitterMetricGroup(
            MetricGroup parentMetricGroup, OperatorIOMetricGroup operatorIOMetricGroup) {
        super(parentMetricGroup);
        numCommittablesTotal = parentMetricGroup.counter(MetricNames.TOTAL_COMMITTABLES);
        numCommittablesFailure = parentMetricGroup.counter(MetricNames.FAILED_COMMITTABLES);
        numCommittablesRetry = parentMetricGroup.counter(MetricNames.RETRIED_COMMITTABLES);
        numCommitatblesSuccess = parentMetricGroup.counter(MetricNames.SUCCESSFUL_COMMITTABLES);
        numCommitatblesAlreadyCommitted =
                parentMetricGroup.counter(MetricNames.ALREADY_COMMITTED_COMMITTABLES);

        this.operatorIOMetricGroup = operatorIOMetricGroup;
    }

    public static InternalSinkCommitterMetricGroup wrap(OperatorMetricGroup operatorMetricGroup) {
        return new InternalSinkCommitterMetricGroup(
                operatorMetricGroup, operatorMetricGroup.getIOMetricGroup());
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return operatorIOMetricGroup;
    }

    @Override
    public Counter getNumCommittablesTotalCounter() {
        return numCommittablesTotal;
    }

    @Override
    public Counter getNumCommittablesFailureCounter() {
        return numCommittablesFailure;
    }

    @Override
    public Counter getNumCommittablesRetryCounter() {
        return numCommittablesRetry;
    }

    @Override
    public Counter getNumCommittablesSuccessCounter() {
        return numCommitatblesSuccess;
    }

    @Override
    public Counter getNumCommittablesAlreadyCommittedCounter() {
        return numCommitatblesAlreadyCommitted;
    }

    @Override
    public void setCurrentPendingCommittablesGauge(Gauge<Integer> currentPendingCommittablesGauge) {
        parentMetricGroup.gauge(MetricNames.PENDING_COMMITTABLES, currentPendingCommittablesGauge);
    }
}
