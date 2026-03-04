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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.configuration.CommitFailureStrategy;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricsGroupTestUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommitRequestImplTest {

    private static final SinkCommitterMetricGroup METRIC_GROUP =
            MetricsGroupTestUtils.mockCommitterMetricGroup();

    @Test
    void testSignalFailedWithUnknownReasonDefaultStrategy() {
        CommitRequestImpl<String> request = new CommitRequestImpl<>("test", METRIC_GROUP);
        assertThatThrownBy(
                        () ->
                                request.signalFailedWithUnknownReason(
                                        new RuntimeException("test error")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to commit test");
        assertThat(request.getState()).isEqualTo(CommitRequestState.FAILED);
    }

    @Test
    void testSignalFailedWithUnknownReasonFailStrategy() {
        CommitRequestImpl<String> request = new CommitRequestImpl<>("test", METRIC_GROUP);
        request.setFailureStrategy(CommitFailureStrategy.FAIL);
        assertThatThrownBy(
                        () ->
                                request.signalFailedWithUnknownReason(
                                        new RuntimeException("test error")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to commit test");
        assertThat(request.getState()).isEqualTo(CommitRequestState.FAILED);
    }

    @Test
    void testSignalFailedWithUnknownReasonWarnStrategy() {
        CommitRequestImpl<String> request = new CommitRequestImpl<>("test", METRIC_GROUP);
        request.setFailureStrategy(CommitFailureStrategy.WARN);
        request.signalFailedWithUnknownReason(new RuntimeException("test error"));
        assertThat(request.getState()).isEqualTo(CommitRequestState.FAILED);
    }

    @Test
    void testSignalFailedWithKnownReasonAlwaysDiscardsRegardlessOfStrategy() {
        for (CommitFailureStrategy strategy : CommitFailureStrategy.values()) {
            CommitRequestImpl<String> request = new CommitRequestImpl<>("test", METRIC_GROUP);
            request.setFailureStrategy(strategy);
            assertThatCode(
                            () ->
                                    request.signalFailedWithKnownReason(
                                            new RuntimeException("known error")))
                    .doesNotThrowAnyException();
            assertThat(request.getState()).isEqualTo(CommitRequestState.FAILED);
        }
    }
}
