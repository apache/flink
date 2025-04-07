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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

/** A utility class for configuring state backend. */
public class StateBackendUtils {

    /**
     * Configures the StreamExecutionEnvironment to use a HashMap-based state backend.
     *
     * @param env the StreamExecutionEnvironment to configure
     */
    public static void configureHashMapStateBackend(StreamExecutionEnvironment env) {
        env.configure(new Configuration().set(StateBackendOptions.STATE_BACKEND, "hashmap"));
    }

    /**
     * Configures the StreamExecutionEnvironment to use a RocksDB state backend.
     *
     * @param env the StreamExecutionEnvironment to configure
     */
    public static void configureRocksDBStateBackend(StreamExecutionEnvironment env) {
        env.configure(new Configuration().set(StateBackendOptions.STATE_BACKEND, "rocksdb"));
    }

    /**
     * Configures the StreamExecutionEnvironment to use a RocksDB state backend with the option for
     * incremental checkpoints.
     *
     * @param env the StreamExecutionEnvironment to configure
     * @param incrementalCheckpoints whether to enable incremental checkpoints
     */
    public static void configureRocksDBStateBackend(
            StreamExecutionEnvironment env, boolean incrementalCheckpoints) {
        env.configure(
                new Configuration()
                        .set(StateBackendOptions.STATE_BACKEND, "rocksdb")
                        .set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incrementalCheckpoints));
    }

    /**
     * Configures the StreamExecutionEnvironment to use a state backend defined by a factory.
     *
     * @param env the StreamExecutionEnvironment to configure
     * @param stateBackendFactory the fully qualified name of the state backend factory class
     */
    public static void configureStateBackendWithFactory(
            StreamExecutionEnvironment env, String stateBackendFactory) {
        env.configure(
                new Configuration().set(StateBackendOptions.STATE_BACKEND, stateBackendFactory));
    }

    /**
     * Configures the state backend for the given StreamExecutionEnvironment and executes the stream
     * job asynchronously.
     *
     * @param env The StreamExecutionEnvironment to configure.
     * @param stateBackend The StateBackend to set for the execution environment.
     */
    public static JobClient configureStateBackendAndExecuteAsync(
            StreamExecutionEnvironment env, StateBackend stateBackend) throws Exception {
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setStateBackend(stateBackend);

        return env.executeAsync(streamGraph);
    }

    /**
     * Configures the state backend for the given StreamExecutionEnvironment and returns the
     * corresponding JobGraph without executing the job.
     *
     * @param env The StreamExecutionEnvironment to configure.
     * @param stateBackend The StateBackend to set for the execution environment.
     * @return The JobGraph representing the configured job.
     */
    public static JobGraph configureStateBackendAndGetJobGraph(
            StreamExecutionEnvironment env, StateBackend stateBackend) {
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setStateBackend(stateBackend);
        return streamGraph.getJobGraph();
    }

    /**
     * Configures the state backend for the given StreamExecutionEnvironment and returns the
     * corresponding JobGraph without executing the job, using a specified ClassLoader and JobID.
     *
     * @param env The StreamExecutionEnvironment to configure.
     * @param stateBackend The StateBackend to set for the execution environment.
     * @param userClassLoader The ClassLoader to use for user-defined classes.
     * @param jobId The JobID to associate with the JobGraph.
     * @return The JobGraph representing the configured job.
     */
    public static JobGraph configureStateBackendAndGetJobGraph(
            StreamExecutionEnvironment env,
            StateBackend stateBackend,
            ClassLoader userClassLoader,
            JobID jobId) {
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setStateBackend(stateBackend);
        return streamGraph.getJobGraph(userClassLoader, jobId);
    }
}
