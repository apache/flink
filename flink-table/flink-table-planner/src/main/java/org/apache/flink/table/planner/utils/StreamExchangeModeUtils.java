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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import java.util.Optional;

/** Utility class to load job-wide exchange mode. */
@Internal
public class StreamExchangeModeUtils {

    static final String ALL_EDGES_BLOCKING_LEGACY = "batch";

    static final String ALL_EDGES_PIPELINED_LEGACY = "pipelined";

    public static StreamExchangeMode getBatchStreamExchangeMode(
            ReadableConfig config, StreamExchangeMode requiredExchangeMode) {
        if (requiredExchangeMode == StreamExchangeMode.BATCH) {
            return StreamExchangeMode.BATCH;
        }

        final GlobalStreamExchangeMode globalExchangeMode =
                getGlobalStreamExchangeMode(config).orElse(null);
        if (globalExchangeMode == GlobalStreamExchangeMode.ALL_EDGES_BLOCKING) {
            return StreamExchangeMode.BATCH;
        }

        final BatchShuffleMode shuffleMode = config.get(ExecutionOptions.BATCH_SHUFFLE_MODE);
        if (shuffleMode == BatchShuffleMode.ALL_EXCHANGES_BLOCKING) {
            return StreamExchangeMode.BATCH;
        }

        return StreamExchangeMode.UNDEFINED;
    }

    /**
     * The {@link GlobalStreamExchangeMode} should be determined by the {@link StreamGraphGenerator}
     * in the future.
     */
    @Deprecated
    static Optional<GlobalStreamExchangeMode> getGlobalStreamExchangeMode(ReadableConfig config) {
        return config.getOptional(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE)
                .map(
                        value -> {
                            try {
                                return GlobalStreamExchangeMode.valueOf(
                                        convertLegacyShuffleMode(value).toUpperCase());
                            } catch (IllegalArgumentException e) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Unsupported value %s for config %s.",
                                                value,
                                                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE
                                                        .key()));
                            }
                        });
    }

    private static String convertLegacyShuffleMode(final String shuffleMode) {
        switch (shuffleMode.toLowerCase()) {
            case ALL_EDGES_BLOCKING_LEGACY:
                return GlobalStreamExchangeMode.ALL_EDGES_BLOCKING.toString();
            case ALL_EDGES_PIPELINED_LEGACY:
                return GlobalStreamExchangeMode.ALL_EDGES_PIPELINED.toString();
            default:
                return shuffleMode;
        }
    }
}
