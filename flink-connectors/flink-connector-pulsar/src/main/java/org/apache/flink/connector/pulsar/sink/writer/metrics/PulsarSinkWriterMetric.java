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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Util class to provide monitor metrics methods to Sink Writer. */
@Internal
public class PulsarSinkWriterMetric {
    public static final String PULSAR_SINK_METRIC_GROUP = "PulsarSink";
    public static final String PRODUCER_SUBGROUP = "producer";
    public static final String METRIC_PRODUCER_SEND_LATENCY_MAX = "sendLatencyMax";
    public static final String METRIC_NUM_ACKS_RECEIVED = "numAcksReceived";
    public static final String METRIC_SEND_LATENCY_50_PCT = "sendLatency50Pct";
    public static final String METRIC_SEND_LATENCY_75_PCT = "sendLatency75Pct";
    public static final String METRIC_SEND_LATENCY_95_PCT = "sendLatency95Pct";
    public static final String METRIC_SEND_LATENCY_99_PCT = "sendLatency99Pct";
    public static final String METRIC_SEND_LATENCY_999_PCT = "sendLatency999Pct";
    public static final double INVALID_LATENCY = -1d;
    public static final long INVALID_LAST_SEND_TIME = -1;

    private final SinkWriterMetricGroup sinkWriterMetricGroup;
    private final MetricGroup producerMetricGroup;

    // Standard counter metrics
    private long lastNumBytesOut = 0;
    private long lastNumRecordsOut = 0;
    private long lastNumRecordsOutErrors = 0;

    // Pulsar Producer counter metrics
    private final Counter numAcksReceived;
    private long lastNumAcksReceived = 0;

    public PulsarSinkWriterMetric(SinkWriterMetricGroup sinkWriterMetricGroup) {
        this.sinkWriterMetricGroup = sinkWriterMetricGroup;
        this.producerMetricGroup = sinkWriterMetricGroup.addGroup(PULSAR_SINK_METRIC_GROUP);
        this.numAcksReceived = producerMetricGroup.counter(METRIC_NUM_ACKS_RECEIVED);
    }

    public void updateProducerStats(Map<String, Map<SchemaInfo, Producer<?>>> producerRegister) {
        List<ProducerStats> stats =
                producerRegister.values().stream()
                        .flatMap(producers -> producers.values().stream())
                        .map(Producer::getStats)
                        .collect(Collectors.toList());
        recordNumBytesOut(stats);
        recordNumRecordsOut(stats);
        recordNumRecordsOutErrors(stats);
        recordNumAcksReceived(stats);
    }

    public void registerMaxSendLatencyGauges(
            Map<String, Map<SchemaInfo, Producer<?>>> producerRegister) {
        setSendLatencyMaxMillisGauge(() -> computeMaxSendLatencyInAllProducers(producerRegister));
    }

    public void checkAndRegisterNewProducerGauges(
            Map<String, Map<SchemaInfo, Producer<?>>> producerRegister) {
        for (String topic : producerRegister.keySet()) {
            Map<SchemaInfo, Producer<?>> producers = producerRegister.get(topic);
            for (Producer<?> producer : producers.values()) {
                checkDuplicationAndSetSendLatencyPctGauges(topic, producer);
            }
        }
    }

    /**
     * Standard metrics. Set the gauge to track the time spent to send the last record. From when
     * sendAsync() is callled to the message get acked. Notice that when the delivery guarantee is
     * set to None, currentSend time is 0 and not tracked.
     *
     * @param timeGauge
     */
    public void setCurrentSendTimeGauge(Gauge<Long> timeGauge) {
        sinkWriterMetricGroup.setCurrentSendTimeGauge(timeGauge);
    }

    private double computeMaxSendLatencyInAllProducers(
            Map<String, Map<SchemaInfo, Producer<?>>> producerRegister) {
        return producerRegister.values().stream()
                .flatMap((collection) -> collection.values().stream())
                .map(producer -> producer.getStats().getSendLatencyMillisMax())
                .max(Comparator.naturalOrder())
                .orElse(INVALID_LATENCY);
    }

    private void recordNumBytesOut(List<ProducerStats> stats) {
        long numBytesOut = 0;
        numBytesOut +=
                stats.stream()
                        .map(ProducerStats::getTotalBytesSent)
                        .mapToLong(Long::longValue)
                        .sum();
        sinkWriterMetricGroup
                .getIOMetricGroup()
                .getNumBytesOutCounter()
                .inc(numBytesOut - lastNumBytesOut);
        lastNumBytesOut = numBytesOut;
    }

    private void recordNumRecordsOut(List<ProducerStats> stats) {
        long numRecordsOut = 0;
        numRecordsOut +=
                stats.stream()
                        .map(ProducerStats::getTotalMsgsSent)
                        .mapToLong(Long::longValue)
                        .sum();
        sinkWriterMetricGroup
                .getIOMetricGroup()
                .getNumRecordsOutCounter()
                .inc(numRecordsOut - lastNumRecordsOut);
        lastNumRecordsOut = numRecordsOut;
    }

    private void recordNumRecordsOutErrors(List<ProducerStats> stats) {
        long numRecordsOutErrors = 0;
        numRecordsOutErrors +=
                stats.stream()
                        .map(ProducerStats::getTotalSendFailed)
                        .mapToLong(Long::longValue)
                        .sum();
        sinkWriterMetricGroup
                .getNumRecordsOutErrorsCounter()
                .inc(numRecordsOutErrors - lastNumRecordsOutErrors);
        lastNumRecordsOutErrors = numRecordsOutErrors;
    }

    private void recordNumAcksReceived(List<ProducerStats> stats) {
        long numAcks = 0;
        numAcks +=
                stats.stream()
                        .map(ProducerStats::getTotalAcksReceived)
                        .mapToLong(Long::longValue)
                        .sum();
        numAcksReceived.inc(numAcks - lastNumAcksReceived);
        lastNumAcksReceived = numAcks;
    }

    private void setSendLatencyMaxMillisGauge(Gauge<Double> latencyGauge) {
        producerMetricGroup.gauge(METRIC_PRODUCER_SEND_LATENCY_MAX, latencyGauge);
    }

    /**
     * Use producer and topic name as the identifier of each metric group. The internal
     * implementation of metrics handles duplication of group and metrics, so it is safe to register
     * an already registered producer.
     *
     * @param singleProducer
     */
    private void checkDuplicationAndSetSendLatencyPctGauges(
            String topic, Producer<?> singleProducer) {
        MetricGroup group =
                producerMetricGroup.addGroup(
                        PRODUCER_SUBGROUP, singleProducer.getProducerName() + topic);
        group.gauge(
                METRIC_SEND_LATENCY_50_PCT,
                () -> singleProducer.getStats().getSendLatencyMillis50pct());
        group.gauge(
                METRIC_SEND_LATENCY_75_PCT,
                () -> singleProducer.getStats().getSendLatencyMillis75pct());
        group.gauge(
                METRIC_SEND_LATENCY_95_PCT,
                () -> singleProducer.getStats().getSendLatencyMillis95pct());
        group.gauge(
                METRIC_SEND_LATENCY_99_PCT,
                () -> singleProducer.getStats().getSendLatencyMillis99pct());
        group.gauge(
                METRIC_SEND_LATENCY_999_PCT,
                () -> singleProducer.getStats().getSendLatencyMillis999pct());
    }
}
