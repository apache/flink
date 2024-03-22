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

package org.apache.flink.datastream.impl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.datastream.api.ExecutionEnvironment;

/**
 * Special {@link ExecutionEnvironment} that will be used in cases where the CLI client or testing
 * utilities create a {@link ExecutionEnvironment} that should be used when {@link
 * ExecutionEnvironment#getInstance()} ()} is called.
 */
public class ExecutionContextEnvironment extends ExecutionEnvironmentImpl {
    public ExecutionContextEnvironment(
            final PipelineExecutorServiceLoader executorServiceLoader,
            final Configuration configuration,
            final ClassLoader userCodeClassLoader) {
        super(executorServiceLoader, configuration, userCodeClassLoader);
    }

    // --------------------------------------------------------------------------------------------

    public static void setAsContext(
            final PipelineExecutorServiceLoader executorServiceLoader,
            final Configuration clusterConfiguration,
            final ClassLoader userCodeClassLoader) {
        final ExecutionEnvironmentFactory factory =
                envInitConfig -> {
                    final Configuration mergedEnvConfig = new Configuration();
                    mergedEnvConfig.addAll(clusterConfiguration);
                    mergedEnvConfig.addAll(envInitConfig);
                    return new ExecutionContextEnvironment(
                            executorServiceLoader, mergedEnvConfig, userCodeClassLoader);
                };
        initializeContextEnvironment(factory);
    }

    public static void unsetAsContext() {
        resetContextEnvironment();
    }
}
