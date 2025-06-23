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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.PrintSinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.ParameterTool;

import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.STATE_BACKEND_ROCKS_INCREMENTAL;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

/**
 * A test job for State TTL feature.
 *
 * <p>The test pipeline does the following: - generates random keyed state updates for each state
 * TTL verifier (state type) - performs update of created state with TTL for each verifier - keeps
 * previous updates in other state - verifies expected result of last update against preserved
 * history of updates
 *
 * <p>Program parameters:
 *
 * <ul>
 *   <li>update_generator_source.keyspace (int, default - 100): Number of different keys for updates
 *       emitted by the update generator.
 *   <li>update_generator_source.sleep_time (long, default - 0): Milliseconds to sleep after
 *       emitting updates in the update generator. Set to 0 to disable sleeping.
 *   <li>update_generator_source.sleep_after_elements (long, default - 0): Number of updates to emit
 *       before sleeping in the update generator. Set to 0 to disable sleeping.
 *   <li>state_ttl_verifier.ttl_milli (long, default - 1000): State time-to-live.
 *   <li>report_stat.after_updates_num (long, default - 200): Report state update statistics after
 *       certain number of updates (average update chain length and clashes).
 * </ul>
 */
public class DataStreamStateTTLTestProgram {
    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt, false);

        TtlTestConfig config = TtlTestConfig.fromArgs(pt);
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(config.ttl).cleanupFullSnapshot().build();

        env.addSource(
                        new TtlStateUpdateSource(
                                config.keySpace, config.sleepAfterElements, config.sleepTime))
                .name("TtlStateUpdateSource")
                .keyBy(TtlStateUpdate::getKey)
                .flatMap(new TtlVerifyUpdateFunction(ttlConfig, config.reportStatAfterUpdatesNum))
                .name("TtlVerifyUpdateFunction")
                .addSink(new PrintSinkFunction<>())
                .name("PrintFailedVerifications");

        StreamGraph streamGraph = env.getStreamGraph();
        setBackendWithCustomTTLTimeProvider(streamGraph, pt);
        streamGraph.setJobName("State TTL test job");
        streamGraph.createJobCheckpointingSettings();

        env.execute(streamGraph);
    }

    /**
     * Sets the state backend to a new {@link StubStateBackend} which has a {@link
     * MonotonicTTLTimeProvider}.
     *
     * @param streamGraph The {@link StreamGraph} of the job.
     */
    private static void setBackendWithCustomTTLTimeProvider(
            StreamGraph streamGraph, final ParameterTool pt) {
        final MonotonicTTLTimeProvider ttlTimeProvider = new MonotonicTTLTimeProvider();

        final StateBackend configuredBackend = getConfiguredStateBackend(pt);
        final StateBackend stubBackend = new StubStateBackend(configuredBackend, ttlTimeProvider);
        streamGraph.setStateBackend(stubBackend);
    }

    private static StateBackend getConfiguredStateBackend(final ParameterTool pt) {
        final String stateBackend = pt.get(STATE_BACKEND.key(), STATE_BACKEND.defaultValue());

        if ("hashmap".equalsIgnoreCase(stateBackend)) {
            return new HashMapStateBackend();
        } else if ("rocks".equalsIgnoreCase(stateBackend)) {
            boolean incrementalCheckpoints =
                    pt.getBoolean(
                            STATE_BACKEND_ROCKS_INCREMENTAL.key(),
                            STATE_BACKEND_ROCKS_INCREMENTAL.defaultValue());

            return new EmbeddedRocksDBStateBackend(incrementalCheckpoints);
        } else {
            throw new IllegalArgumentException("Unknown backend requested: " + stateBackend);
        }
    }
}
