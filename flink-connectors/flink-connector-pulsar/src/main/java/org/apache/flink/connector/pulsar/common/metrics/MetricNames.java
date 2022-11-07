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

/** The constant class for holding all the custom metrics names in Pulsar. */
public final class MetricNames {

    private MetricNames() {
        // No public constructor.
    }

    public static final String PULSAR_PRODUCER_METRIC_NAME = "PulsarProducer";
    public static final String NUM_MSGS_SENT = "numMsgsSent";
    public static final String NUM_BYTES_SENT = "numBytesSent";
    public static final String NUM_SEND_FAILED = "numSendFailed";
    public static final String NUM_ACKS_RECEIVED = "numAcksReceived";
    public static final String SEND_MSGS_RATE = "sendMsgsRate";
    public static final String SEND_BYTES_RATE = "sendBytesRate";
    public static final String SEND_LATENCY_MILLIS_50_PCT = "sendLatencyMillis50pct";
    public static final String SEND_LATENCY_MILLIS_75_PCT = "sendLatencyMillis75pct";
    public static final String SEND_LATENCY_MILLIS_95_PCT = "sendLatencyMillis95pct";
    public static final String SEND_LATENCY_MILLIS_99_PCT = "sendLatencyMillis99pct";
    public static final String SEND_LATENCY_MILLIS_999_PCT = "sendLatencyMillis999pct";
    public static final String SEND_LATENCY_MILLIS_MAX = "sendLatencyMillisMax";
    public static final String TOTAL_MSGS_SENT = "totalMsgsSent";
    public static final String TOTAL_BYTES_SENT = "totalBytesSent";
    public static final String TOTAL_SEND_FAILED = "totalSendFailed";
    public static final String TOTAL_ACKS_RECEIVED = "totalAcksReceived";
    public static final String PENDING_QUEUE_SIZE = "pendingQueueSize";

    public static final String PULSAR_CONSUMER_METRIC_NAME = "PulsarConsumer";
    public static final String NUM_MSGS_RECEIVED = "numMsgsReceived";
    public static final String NUM_BYTES_RECEIVED = "numBytesReceived";
    public static final String RATE_MSGS_RECEIVED = "rateMsgsReceived";
    public static final String RATE_BYTES_RECEIVED = "rateBytesReceived";
    public static final String NUM_ACKS_SENT = "numAcksSent";
    public static final String NUM_ACKS_FAILED = "numAcksFailed";
    public static final String NUM_RECEIVE_FAILED = "numReceiveFailed";
    public static final String NUM_BATCH_RECEIVE_FAILED = "numBatchReceiveFailed";
    public static final String TOTAL_MSGS_RECEIVED = "totalMsgsReceived";
    public static final String TOTAL_BYTES_RECEIVED = "totalBytesReceived";
    public static final String TOTAL_RECEIVED_FAILED = "totalReceivedFailed";
    public static final String TOTAL_BATCH_RECEIVED_FAILED = "totalBatchReceivedFailed";
    public static final String TOTAL_ACKS_SENT = "totalAcksSent";
    public static final String TOTAL_ACKS_FAILED = "totalAcksFailed";
    public static final String MSG_NUM_IN_RECEIVER_QUEUE = "msgNumInReceiverQueue";
}
