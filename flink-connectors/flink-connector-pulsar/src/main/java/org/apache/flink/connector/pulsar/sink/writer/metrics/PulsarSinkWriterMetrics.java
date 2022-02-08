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
import java.util.Map;

/** Util class to provide monitor metrics methods to Sink Writer. */
@Internal
public class PulsarSinkWriterMetrics {
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

    public PulsarSinkWriterMetrics(SinkWriterMetricGroup sinkWriterMetricGroup) {
        this.sinkWriterMetricGroup = sinkWriterMetricGroup;
        this.producerMetricGroup = sinkWriterMetricGroup.addGroup(PULSAR_SINK_METRIC_GROUP);
        this.numAcksReceived = producerMetricGroup.counter(METRIC_NUM_ACKS_RECEIVED);
    }

    public <T> void updateProducerStats(
            Map<String, Map<SchemaInfo, Producer<?>>> producerRegister) {
        long numBytesOut = 0;
        long numRecordsOut = 0;
        long numRecordsOutErrors = 0;
        long numAcksReceived = 0;

        for (Map<SchemaInfo, Producer<?>> producers : producerRegister.values()) {
            for (Producer<?> producer : producers.values()) {
                ProducerStats stats = producer.getStats();
                numBytesOut += stats.getTotalBytesSent();
                numRecordsOut += stats.getTotalMsgsSent();
                numRecordsOutErrors += stats.getTotalSendFailed();
                numAcksReceived += stats.getTotalAcksReceived();
            }
        }

        recordNumBytesOut(numBytesOut - lastNumBytesOut);
        recordNumRecordsOut(numRecordsOut - lastNumRecordsOut);
        recordNumRecordOutErrors(numRecordsOutErrors - lastNumRecordsOutErrors);
        recordNumAcksReceived(numAcksReceived - lastNumAcksReceived);

        lastNumBytesOut = numBytesOut;
        lastNumRecordsOut = numRecordsOut;
        lastNumRecordsOutErrors = numRecordsOutErrors;
        lastNumAcksReceived = numAcksReceived;
    }

    public <T> void registerMaxSendLatencyGauges(
            Map<String, Map<SchemaInfo, Producer<?>>> producerRegister) {
        setSendLatencyMaxMillisGauge(() -> computeMaxSendLatencyInAllProducers(producerRegister));
    }

    public <T> void registerSingleProducerGauges(Producer<T> singleProducer) {
        setSendLatencyPctGauges(singleProducer);
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

    private <T> double computeMaxSendLatencyInAllProducers(
            Map<String, Map<SchemaInfo, Producer<?>>> producerRegister) {
        return producerRegister.values().stream()
                .flatMap((collection) -> collection.values().stream())
                .map(producer -> producer.getStats().getSendLatencyMillisMax())
                .max(Comparator.naturalOrder())
                .orElse(INVALID_LATENCY);
    }

    private void recordNumBytesOut(long numBytes) {
        sinkWriterMetricGroup.getIOMetricGroup().getNumBytesOutCounter().inc(numBytes);
    }

    private void recordNumRecordsOut(long numRecords) {
        sinkWriterMetricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc(numRecords);
    }

    private void recordNumRecordOutErrors(long numErrors) {
        sinkWriterMetricGroup.getNumRecordsOutErrorsCounter().inc(numErrors);
    }

    private void recordNumAcksReceived(long numAcks) {
        numAcksReceived.inc(numAcks);
    }

    private void setSendLatencyMaxMillisGauge(Gauge<Double> latencyGauge) {
        producerMetricGroup.gauge(METRIC_PRODUCER_SEND_LATENCY_MAX, latencyGauge);
    }

    /**
     * A single producer calls getTopic() is is a full topic name. For partitioned topics, the name
     * maps to a unique partition of a partitioned topic.
     *
     * @param singleProducer
     */
    private void setSendLatencyPctGauges(Producer<?> singleProducer) {
        MetricGroup group =
                producerMetricGroup.addGroup(PRODUCER_SUBGROUP, singleProducer.getProducerName());
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
