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

package org.apache.flink.connector.pulsar.common.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;

/** The metric statistic for Pulsar's {@link Producer}. */
public class ProducerMetricsInterceptor implements ProducerInterceptor {

    private final Counter numRecordsOutErrors;
    private final Counter numRecordsOutCounter;
    private final Counter numBytesOutCounter;

    public ProducerMetricsInterceptor(SinkWriterMetricGroup metricGroup) {
        this.numRecordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsOutCounter = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
    }

    @Override
    public void close() {
        // Nothing to do by default.
    }

    @Override
    public boolean eligible(Message message) {
        return true;
    }

    @Override
    public Message beforeSend(Producer producer, Message message) {
        return message;
    }

    @Override
    public void onSendAcknowledgement(
            Producer producer, Message message, MessageId msgId, Throwable exception) {
        if (exception != null) {
            numRecordsOutErrors.inc(1);
        } else {
            numRecordsOutCounter.inc(1);
            numBytesOutCounter.inc(message.size());
        }
    }
}
