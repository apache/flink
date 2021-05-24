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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.table.api.TableConfig;

import java.util.List;

/** Utility class to generate StreamGraph and set properties for batch. */
public class ExecutorUtils {

    /** Generate {@link StreamGraph} by {@link StreamGraphGenerator}. */
    public static StreamGraph generateStreamGraph(
            StreamExecutionEnvironment execEnv, List<Transformation<?>> transformations) {
        if (transformations.size() <= 0) {
            throw new IllegalStateException(
                    "No operators defined in streaming topology. Cannot generate StreamGraph.");
        }
        StreamGraphGenerator generator =
                new StreamGraphGenerator(
                                transformations, execEnv.getConfig(), execEnv.getCheckpointConfig())
                        .setStateBackend(execEnv.getStateBackend())
                        .setSavepointDir(execEnv.getDefaultSavepointDirectory())
                        .setChaining(execEnv.isChainingEnabled())
                        .setUserArtifacts(execEnv.getCachedFiles())
                        .setTimeCharacteristic(execEnv.getStreamTimeCharacteristic())
                        .setDefaultBufferTimeout(execEnv.getBufferTimeout());
        return generator.generate();
    }

    /** Sets batch properties for {@link StreamExecutionEnvironment}. */
    public static void setBatchProperties(StreamExecutionEnvironment execEnv) {
        ExecutionConfig executionConfig = execEnv.getConfig();
        executionConfig.enableObjectReuse();
        executionConfig.setLatencyTrackingInterval(-1);
        execEnv.getConfig().setAutoWatermarkInterval(0);
        execEnv.setBufferTimeout(-1);
    }

    /** Sets batch properties for {@link StreamGraph}. */
    public static void setBatchProperties(StreamGraph streamGraph, TableConfig tableConfig) {
        streamGraph
                .getStreamNodes()
                .forEach(sn -> sn.setResources(ResourceSpec.UNKNOWN, ResourceSpec.UNKNOWN));
        streamGraph.setChaining(true);
        streamGraph.setAllVerticesInSameSlotSharingGroupByDefault(false);
        // Configure job type for properly selecting a supported scheduler for batch jobs.
        // LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST is only supported by the Batch scheduler (=Ng
        // scheduler)
        streamGraph.setJobType(JobType.BATCH);
        streamGraph.setStateBackend(null);
        streamGraph.setCheckpointStorage(null);
        if (streamGraph.getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalArgumentException("Checkpoint is not supported for batch jobs.");
        }
        streamGraph.setGlobalDataExchangeMode(getGlobalDataExchangeMode(tableConfig));
    }

    private static GlobalDataExchangeMode getGlobalDataExchangeMode(TableConfig tableConfig) {
        return ShuffleModeUtils.getShuffleModeAsGlobalDataExchangeMode(
                tableConfig.getConfiguration());
    }
}
