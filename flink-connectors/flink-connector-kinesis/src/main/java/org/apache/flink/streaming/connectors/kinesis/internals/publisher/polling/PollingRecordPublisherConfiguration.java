/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.polling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

/** A configuration class for {@link PollingRecordPublisher} instantiated from a properties map. */
@Internal
public class PollingRecordPublisherConfiguration {

    private final boolean adaptiveReads;

    private final int maxNumberOfRecordsPerFetch;

    private final long fetchIntervalMillis;

    public PollingRecordPublisherConfiguration(final Properties consumerConfig) {
        this.maxNumberOfRecordsPerFetch =
                Integer.parseInt(
                        consumerConfig.getProperty(
                                ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
                                Integer.toString(
                                        ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX)));

        this.fetchIntervalMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                                Long.toString(
                                        ConsumerConfigConstants
                                                .DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS)));

        this.adaptiveReads =
                Boolean.parseBoolean(
                        consumerConfig.getProperty(
                                ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS,
                                Boolean.toString(
                                        ConsumerConfigConstants.DEFAULT_SHARD_USE_ADAPTIVE_READS)));
    }

    public boolean isAdaptiveReads() {
        return adaptiveReads;
    }

    public int getMaxNumberOfRecordsPerFetch() {
        return maxNumberOfRecordsPerFetch;
    }

    public long getFetchIntervalMillis() {
        return fetchIntervalMillis;
    }
}
