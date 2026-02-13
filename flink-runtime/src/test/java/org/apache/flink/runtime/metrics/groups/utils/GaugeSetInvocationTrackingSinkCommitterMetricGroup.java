package org.apache.flink.runtime.metrics.groups.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

import java.util.concurrent.atomic.AtomicInteger;

public class GaugeSetInvocationTrackingSinkCommitterMetricGroup
        extends ProxyMetricGroup<MetricGroup> implements SinkCommitterMetricGroup {

    private final AtomicInteger gaugeCallCount = new AtomicInteger(0);

    private final Counter numCommittablesTotal;
    private final Counter numCommittablesFailure;
    private final Counter numCommittablesRetry;
    private final Counter numCommitatblesSuccess;
    private final Counter numCommitatblesAlreadyCommitted;
    private final OperatorIOMetricGroup operatorIOMetricGroup;

    @VisibleForTesting
    public GaugeSetInvocationTrackingSinkCommitterMetricGroup(
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

    public static GaugeSetInvocationTrackingSinkCommitterMetricGroup wrap(
            OperatorMetricGroup operatorMetricGroup) {
        return new GaugeSetInvocationTrackingSinkCommitterMetricGroup(
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
        gaugeCallCount.incrementAndGet();
        parentMetricGroup.gauge(MetricNames.PENDING_COMMITTABLES, currentPendingCommittablesGauge);
    }

    public int getGaugeCallCount() {
        return gaugeCallCount.get();
    }

    public void resetGaugeCallCount() {
        gaugeCallCount.set(0);
    }
}
