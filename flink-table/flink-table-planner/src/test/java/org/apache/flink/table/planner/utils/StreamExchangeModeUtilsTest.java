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

package org.apache.flink.table.planner.utils;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.planner.utils.StreamExchangeModeUtils.getBatchStreamExchangeMode;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamExchangeModeUtils}. */
class StreamExchangeModeUtilsTest {

    @Test
    void testBatchStreamExchangeMode() {
        final Configuration configuration = new Configuration();

        assertThat(getBatchStreamExchangeMode(configuration, null))
                .isEqualTo(StreamExchangeMode.BATCH);

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        assertThat(getBatchStreamExchangeMode(configuration, null))
                .isEqualTo(StreamExchangeMode.BATCH);

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL);
        assertThat(getBatchStreamExchangeMode(configuration, null))
                .isEqualTo(StreamExchangeMode.HYBRID_FULL);

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        assertThat(getBatchStreamExchangeMode(configuration, null))
                .isEqualTo(StreamExchangeMode.HYBRID_SELECTIVE);

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED);
        assertThat(getBatchStreamExchangeMode(configuration, null))
                .isEqualTo(StreamExchangeMode.UNDEFINED);

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED);
        assertThat(getBatchStreamExchangeMode(configuration, StreamExchangeMode.BATCH))
                .isEqualTo(StreamExchangeMode.BATCH);
    }
}
