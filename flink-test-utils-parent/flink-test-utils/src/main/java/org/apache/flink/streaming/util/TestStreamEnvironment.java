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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.test.util.MiniClusterPipelineExecutorServiceLoader;

import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.configuration.CheckpointingOptions.LOCAL_RECOVERY;
import static org.apache.flink.runtime.testutils.PseudoRandomValueSelector.randomize;

/** A {@link StreamExecutionEnvironment} that executes its jobs on {@link MiniCluster}. */
public class TestStreamEnvironment extends StreamExecutionEnvironment {
    private static final String STATE_CHANGE_LOG_CONFIG_ON = "on";
    private static final String STATE_CHANGE_LOG_CONFIG_UNSET = "unset";
    private static final String STATE_CHANGE_LOG_CONFIG_RAND = "random";
    private static final boolean RANDOMIZE_CHECKPOINTING_CONFIG =
            Boolean.parseBoolean(System.getProperty("checkpointing.randomization", "false"));
    private static final String STATE_CHANGE_LOG_CONFIG =
            System.getProperty("checkpointing.changelog", STATE_CHANGE_LOG_CONFIG_UNSET).trim();

    public TestStreamEnvironment(
            MiniCluster miniCluster,
            Configuration config,
            int parallelism,
            Collection<Path> jarFiles,
            Collection<URL> classPaths) {
        super(
                new MiniClusterPipelineExecutorServiceLoader(miniCluster),
                MiniClusterPipelineExecutorServiceLoader.updateConfigurationForMiniCluster(
                        config, jarFiles, classPaths),
                null);

        setParallelism(parallelism);
    }

    public TestStreamEnvironment(MiniCluster miniCluster, int parallelism) {
        this(
                miniCluster,
                new Configuration(),
                parallelism,
                Collections.emptyList(),
                Collections.emptyList());
    }

    /**
     * Sets the streaming context environment to a TestStreamEnvironment that runs its programs on
     * the given cluster with the given default parallelism and the specified jar files and class
     * paths.
     *
     * @param miniCluster The MiniCluster to execute jobs on.
     * @param parallelism The default parallelism for the test programs.
     * @param jarFiles Additional jar files to execute the job with
     * @param classpaths Additional class paths to execute the job with
     */
    public static void setAsContext(
            final MiniCluster miniCluster,
            final int parallelism,
            final Collection<Path> jarFiles,
            final Collection<URL> classpaths) {

        StreamExecutionEnvironmentFactory factory =
                conf -> {
                    TestStreamEnvironment env =
                            new TestStreamEnvironment(
                                    miniCluster, conf, parallelism, jarFiles, classpaths);

                    randomizeConfiguration(miniCluster, conf);

                    env.configure(conf, env.getUserClassloader());
                    return env;
                };

        initializeContextEnvironment(factory);
    }

    /**
     * This is the place for randomization the configuration that relates to DataStream API such as
     * ExecutionConf, CheckpointConf, StreamExecutionEnvironment. List of the configurations can be
     * found here {@link StreamExecutionEnvironment#configure(ReadableConfig, ClassLoader)}. All
     * other configuration should be randomized here {@link
     * org.apache.flink.runtime.testutils.MiniClusterResource#randomizeConfiguration(Configuration)}.
     */
    private static void randomizeConfiguration(MiniCluster miniCluster, Configuration conf) {
        // randomize ITTests for enabling unaligned checkpoint
        if (RANDOMIZE_CHECKPOINTING_CONFIG) {
            randomize(conf, ExecutionCheckpointingOptions.ENABLE_UNALIGNED, true, false);
            randomize(
                    conf,
                    ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                    Duration.ofSeconds(0),
                    Duration.ofMillis(100),
                    Duration.ofSeconds(2));
            randomize(conf, CheckpointingOptions.CLEANER_PARALLEL_MODE, true, false);
        }

        // randomize ITTests for enabling state change log
        if (isConfigurationSupportedByChangelog(miniCluster.getConfiguration())) {
            if (STATE_CHANGE_LOG_CONFIG.equalsIgnoreCase(STATE_CHANGE_LOG_CONFIG_ON)) {
                if (!conf.contains(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
                    conf.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
                    miniCluster.overrideRestoreModeForChangelogStateBackend();
                }
            } else if (STATE_CHANGE_LOG_CONFIG.equalsIgnoreCase(STATE_CHANGE_LOG_CONFIG_RAND)) {
                boolean enabled =
                        randomize(conf, StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true, false);
                if (enabled) {
                    randomize(
                            conf,
                            StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL,
                            Duration.ofMillis(100),
                            Duration.ofMillis(500),
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(-1));
                    miniCluster.overrideRestoreModeForChangelogStateBackend();
                }
            }
        }
    }

    private static boolean isConfigurationSupportedByChangelog(Configuration configuration) {
        return !configuration.get(LOCAL_RECOVERY);
    }

    /**
     * Sets the streaming context environment to a TestStreamEnvironment that runs its programs on
     * the given cluster with the given default parallelism.
     *
     * @param miniCluster The MiniCluster to execute jobs on.
     * @param parallelism The default parallelism for the test programs.
     */
    public static void setAsContext(final MiniCluster miniCluster, final int parallelism) {
        setAsContext(miniCluster, parallelism, Collections.emptyList(), Collections.emptyList());
    }

    /** Resets the streaming context environment to null. */
    public static void unsetAsContext() {
        resetContextEnvironment();
    }
}
