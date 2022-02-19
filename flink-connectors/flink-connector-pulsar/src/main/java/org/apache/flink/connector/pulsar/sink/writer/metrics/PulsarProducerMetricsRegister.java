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

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicProducerRegister;

/** A class used to register metrics related timer and gauges for Pulsar producer metrics. */
public class PulsarProducerMetricsRegister implements AutoCloseable {
    // Public because we expose this value to unit test
    public static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;
    public static final long PRODUCER_UPDATE_POLLING_INTERVAL_MILLIS = 60000;

    private final ProcessingTimeService timeService;
    private final TopicProducerRegister producerRegister;
    private volatile boolean closed = false;

    // Metrics timer related fields
    private long lastMetricUpdateTimestamp;
    private long lastUpdateProducerTimestamp;

    public PulsarProducerMetricsRegister(
            ProcessingTimeService timeService, TopicProducerRegister producerRegister) {
        this.timeService = timeService;
        this.producerRegister = producerRegister;
    }

    public void initialMetricRegister() {
        producerRegister.registerMaxSendLatencyGauges();

        lastMetricUpdateTimestamp = timeService.getCurrentProcessingTime();
        registerMetricUpdateTimer();

        lastUpdateProducerTimestamp = timeService.getCurrentProcessingTime();
        registerUpdatePerProducerGaugesTimer();
    }

    private void registerMetricUpdateTimer() {
        timeService.registerTimer(
                lastMetricUpdateTimestamp + METRIC_UPDATE_INTERVAL_MILLIS,
                (time) -> {
                    if (closed || producerRegister == null) {
                        return;
                    }
                    producerRegister.updateProducerStats();
                    lastMetricUpdateTimestamp = time;
                    registerMetricUpdateTimer();
                });
    }

    private void registerUpdatePerProducerGaugesTimer() {
        timeService.registerTimer(
                lastUpdateProducerTimestamp + PRODUCER_UPDATE_POLLING_INTERVAL_MILLIS,
                (time) -> {
                    if (closed || producerRegister == null) {
                        return;
                    }
                    producerRegister.checkAndRegisterNewProducerGauges();
                    lastUpdateProducerTimestamp = time;
                    registerUpdatePerProducerGaugesTimer();
                });
    }

    @Override
    public void close() throws Exception {
        closed = true;
    }
}
