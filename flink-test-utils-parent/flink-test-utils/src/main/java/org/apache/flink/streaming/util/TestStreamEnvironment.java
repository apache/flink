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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.test.util.MiniClusterPipelineExecutorServiceLoader;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;

/** A {@link StreamExecutionEnvironment} that executes its jobs on {@link MiniCluster}. */
public class TestStreamEnvironment extends StreamExecutionEnvironment {

    public TestStreamEnvironment(
            MiniCluster miniCluster,
            int parallelism,
            Collection<Path> jarFiles,
            Collection<URL> classPaths) {
        super(
                new MiniClusterPipelineExecutorServiceLoader(miniCluster),
                MiniClusterPipelineExecutorServiceLoader.createConfiguration(jarFiles, classPaths),
                null);

        setParallelism(parallelism);
    }

    public TestStreamEnvironment(MiniCluster miniCluster, int parallelism) {
        this(miniCluster, parallelism, Collections.emptyList(), Collections.emptyList());
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
                                    miniCluster, parallelism, jarFiles, classpaths);
                    env.configure(conf, env.getUserClassloader());
                    return env;
                };

        initializeContextEnvironment(factory);
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
