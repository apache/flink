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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link StreamExchangeModeUtils}. */
public class StreamExchangeModeUtilsTest extends TestLogger {

    @Test
    public void testGetValidShuffleMode() {
        final Configuration configuration = new Configuration();

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING.toString());
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED.toString());
        assertEquals(
                GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.POINTWISE_EDGES_PIPELINED.toString());
        assertEquals(
                GlobalStreamExchangeMode.POINTWISE_EDGES_PIPELINED,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED.toString());
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));
    }

    @Test
    public void testGetLegacyShuffleMode() {
        final Configuration configuration = new Configuration();

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                StreamExchangeModeUtils.ALL_EDGES_BLOCKING_LEGACY);
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
                StreamExchangeModeUtils.ALL_EDGES_PIPELINED_LEGACY);
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));
    }

    @Test
    public void testGetShuffleModeIgnoreCases() {
        final Configuration configuration = new Configuration();

        configuration.setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "Forward_edges_PIPELINED");
        assertEquals(
                GlobalStreamExchangeMode.FORWARD_EDGES_PIPELINED,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));

        configuration.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "Pipelined");
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));
    }

    @Test
    public void testGetDefaultShuffleMode() {
        final Configuration configuration = new Configuration();
        assertEquals(
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetInvalidShuffleMode() {
        final Configuration configuration = new Configuration();
        configuration.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "invalid-value");
        StreamExchangeModeUtils.getShuffleModeAsGlobalStreamExchangeMode(configuration);
    }
}
