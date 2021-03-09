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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.util.Preconditions;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link ExecutionEnvironment} implementation which executes its jobs on a {@link MiniCluster}.
 */
public class TestEnvironment extends ExecutionEnvironment {

    private final MiniCluster miniCluster;

    private TestEnvironment lastEnv;

    public TestEnvironment(
            MiniCluster miniCluster,
            int parallelism,
            boolean isObjectReuseEnabled,
            Collection<Path> jarFiles,
            Collection<URL> classPaths) {
        super(
                new MiniClusterPipelineExecutorServiceLoader(miniCluster),
                MiniClusterPipelineExecutorServiceLoader.createConfiguration(jarFiles, classPaths),
                null);

        this.miniCluster = Preconditions.checkNotNull(miniCluster);

        setParallelism(parallelism);

        if (isObjectReuseEnabled) {
            getConfig().enableObjectReuse();
        } else {
            getConfig().disableObjectReuse();
        }

        lastEnv = null;
    }

    public TestEnvironment(MiniCluster executor, int parallelism, boolean isObjectReuseEnabled) {
        this(
                executor,
                parallelism,
                isObjectReuseEnabled,
                Collections.emptyList(),
                Collections.emptyList());
    }

    @Override
    public JobExecutionResult getLastJobExecutionResult() {
        if (lastEnv == null) {
            return lastJobExecutionResult;
        } else {
            return lastEnv.getLastJobExecutionResult();
        }
    }

    public void setAsContext() {
        ExecutionEnvironmentFactory factory =
                () -> {
                    lastEnv =
                            new TestEnvironment(
                                    miniCluster,
                                    getParallelism(),
                                    getConfig().isObjectReuseEnabled());
                    return lastEnv;
                };

        initializeContextEnvironment(factory);
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Sets the current {@link ExecutionEnvironment} to be a {@link TestEnvironment}. The test
     * environment executes the given jobs on a Flink mini cluster with the given default
     * parallelism and the additional jar files and class paths.
     *
     * @param miniCluster The MiniCluster to execute jobs on.
     * @param parallelism The default parallelism
     * @param jarFiles Additional jar files to execute the job with
     * @param classPaths Additional class paths to execute the job with
     */
    public static void setAsContext(
            final MiniCluster miniCluster,
            final int parallelism,
            final Collection<Path> jarFiles,
            final Collection<URL> classPaths) {

        ExecutionEnvironmentFactory factory =
                () -> new TestEnvironment(miniCluster, parallelism, false, jarFiles, classPaths);

        initializeContextEnvironment(factory);
    }

    /**
     * Sets the current {@link ExecutionEnvironment} to be a {@link TestEnvironment}. The test
     * environment executes the given jobs on a Flink mini cluster with the given default
     * parallelism and the additional jar files and class paths.
     *
     * @param miniCluster The MiniCluster to execute jobs on.
     * @param parallelism The default parallelism
     */
    public static void setAsContext(final MiniCluster miniCluster, final int parallelism) {
        setAsContext(miniCluster, parallelism, Collections.emptyList(), Collections.emptyList());
    }

    public static void unsetAsContext() {
        resetContextEnvironment();
    }
}
