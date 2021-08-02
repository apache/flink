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
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import org.junit.Test;

import static org.apache.flink.table.planner.utils.StreamExchangeModeUtils.getBatchStreamExchangeMode;
import static org.apache.flink.table.planner.utils.StreamExchangeModeUtils.getGlobalStreamExchangeMode;
import static org.junit.Assert.assertEquals;

/** Tests for {@link StreamExchangeModeUtils}. */
public class StreamExchangeModeUtilsTest {

    @Test
    public void testBatchStreamExchangeMode() {
        final Configuration configuration = new Configuration();

        assertEquals(StreamExchangeMode.BATCH, getBatchStreamExchangeMode(configuration, null));

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        assertEquals(StreamExchangeMode.BATCH, getBatchStreamExchangeMode(configuration, null));

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED);
        assertEquals(StreamExchangeMode.UNDEFINED, getBatchStreamExchangeMode(configuration, null));

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED);
        assertEquals(
                StreamExchangeMode.BATCH,
                getBatchStreamExchangeMode(configuration, StreamExchangeMode.BATCH));
    }

    @Test
    public void testBatchStreamExchangeModeLegacyPrecedence() {
        final Configuration configuration = new Configuration();

        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED);
        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING.toString());

        assertEquals(StreamExchangeMode.BATCH, getBatchStreamExchangeMode(configuration, null));
    }

    @Test
    public void testLegacyShuffleMode() {
        final Configuration configuration = new Configuration();

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING.toString());
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                getGlobalStreamExchangeMode(configuration).orElseThrow(AssertionError::new));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED.toString());
        assertEquals(
                GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED,
                getGlobalStreamExchangeMode(configuration).orElseThrow(AssertionError::new));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.POINTWISE_EDGES_PIPELINED.toString());
        assertEquals(
                GlobalStreamExchangeMode.POINTWISE_EDGES_PIPELINED,
                getGlobalStreamExchangeMode(configuration).orElseThrow(AssertionError::new));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED.toString());
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                getGlobalStreamExchangeMode(configuration).orElseThrow(AssertionError::new));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                StreamExchangeModeUtils.ALL_EDGES_BLOCKING_LEGACY);
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                getGlobalStreamExchangeMode(configuration).orElseThrow(AssertionError::new));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                StreamExchangeModeUtils.ALL_EDGES_PIPELINED_LEGACY);
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                getGlobalStreamExchangeMode(configuration).orElseThrow(AssertionError::new));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "Forward_edges_PIPELINED");
        assertEquals(
                GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED,
                StreamExchangeModeUtils.getGlobalStreamExchangeMode(configuration)
                        .orElseThrow(AssertionError::new));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLegacyShuffleMode() {
        final Configuration configuration = new Configuration();
        configuration.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "invalid-value");
        StreamExchangeModeUtils.getGlobalStreamExchangeMode(configuration);
    }
}
