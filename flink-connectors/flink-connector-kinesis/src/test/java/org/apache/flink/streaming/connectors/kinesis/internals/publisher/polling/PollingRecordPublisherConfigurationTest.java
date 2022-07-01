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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SHARD_GETRECORDS_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PollingRecordPublisherConfiguration}. */
public class PollingRecordPublisherConfigurationTest {

    @Test
    public void testDefaults() {
        PollingRecordPublisherConfiguration configuration =
                new PollingRecordPublisherConfiguration(new Properties());
        assertThat(configuration.getFetchIntervalMillis()).isEqualTo(200);
        assertThat(configuration.getMaxNumberOfRecordsPerFetch()).isEqualTo(10000);
        assertThat(configuration.isAdaptiveReads()).isFalse();
    }

    @Test
    public void testGetFetchIntervalMillis() {
        Properties properties = properties(SHARD_GETRECORDS_INTERVAL_MILLIS, "1");
        PollingRecordPublisherConfiguration configuration =
                new PollingRecordPublisherConfiguration(properties);

        assertThat(configuration.getFetchIntervalMillis()).isEqualTo(1);
    }

    @Test
    public void testGetMaxNumberOfRecordsPerFetch() {
        Properties properties = properties(SHARD_GETRECORDS_MAX, "2");
        PollingRecordPublisherConfiguration configuration =
                new PollingRecordPublisherConfiguration(properties);

        assertThat(configuration.getMaxNumberOfRecordsPerFetch()).isEqualTo(2);
    }

    @Test
    public void testIsAdaptiveReads() {
        Properties properties = properties(SHARD_USE_ADAPTIVE_READS, "true");
        PollingRecordPublisherConfiguration configuration =
                new PollingRecordPublisherConfiguration(properties);

        assertThat(configuration.isAdaptiveReads()).isTrue();
    }

    private Properties properties(final String key, final String value) {
        final Properties properties = new Properties();
        properties.setProperty(key, value);
        return properties;
    }
}
