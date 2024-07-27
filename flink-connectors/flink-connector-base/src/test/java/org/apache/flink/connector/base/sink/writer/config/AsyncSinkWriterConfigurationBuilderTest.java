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

package org.apache.flink.connector.base.sink.writer.config;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration.DEFAULT_FAIL_ON_TIMEOUT;
import static org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration.DEFAULT_REQUEST_TIMEOUT_MS;

/** Tests for {@link AsyncSinkWriterConfiguration#builder()}. */
public class AsyncSinkWriterConfigurationBuilderTest {
    @Test
    public void testBuilderSetsValueForTimeoutAndFailOnTimeout() {
        AsyncSinkWriterConfiguration configuration =
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(1)
                        .setMaxBatchSizeInBytes(10)
                        .setMaxInFlightRequests(1)
                        .setMaxBufferedRequests(2)
                        .setMaxTimeInBufferMS(10)
                        .setMaxRecordSizeInBytes(2)
                        .setRequestTimeoutMS(100)
                        .setFailOnTimeout(true)
                        .build();
        Assertions.assertThat(configuration.getRequestTimeoutMS()).isEqualTo(100);
        Assertions.assertThat(configuration.isFailOnTimeout()).isTrue();
    }

    @Test
    public void testBuilderSetsDefaultValuesForTimeoutAndFailOnTimeout() {
        AsyncSinkWriterConfiguration configuration =
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(1)
                        .setMaxBatchSizeInBytes(10)
                        .setMaxInFlightRequests(1)
                        .setMaxBufferedRequests(2)
                        .setMaxTimeInBufferMS(10)
                        .setMaxRecordSizeInBytes(2)
                        .build();
        Assertions.assertThat(configuration.getRequestTimeoutMS())
                .isEqualTo(DEFAULT_REQUEST_TIMEOUT_MS);
        Assertions.assertThat(configuration.isFailOnTimeout()).isEqualTo(DEFAULT_FAIL_ON_TIMEOUT);
    }
}
