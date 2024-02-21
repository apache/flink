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

package org.apache.flink.table.executor.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.chain.PythonOperatorChainingOptimizer;
import org.apache.flink.python.util.PythonConfigUtil;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** {@link Executor} which will perform chaining optimization before generating the StreamGraph. */
@Internal
public class ChainingOptimizingExecutor implements Executor {

    private final Executor executor;

    public ChainingOptimizingExecutor(Executor executor) {
        this.executor = Preconditions.checkNotNull(executor);
    }

    @Override
    public ReadableConfig getConfiguration() {
        return executor.getConfiguration();
    }

    @Override
    public Pipeline createPipeline(
            List<Transformation<?>> transformations,
            ReadableConfig configuration,
            String defaultJobName) {
        return createPipeline(
                transformations, configuration, defaultJobName, Collections.emptyList());
    }

    @Override
    public Pipeline createPipeline(
            List<Transformation<?>> transformations,
            ReadableConfig configuration,
            @Nullable String defaultJobName,
            List<JobStatusHook> jobStatusHookList) {
        List<Transformation<?>> chainedTransformations = transformations;
        if (configuration
                .getOptional(PythonOptions.PYTHON_OPERATOR_CHAINING_ENABLED)
                .orElse(getConfiguration().get(PythonOptions.PYTHON_OPERATOR_CHAINING_ENABLED))) {
            chainedTransformations = PythonOperatorChainingOptimizer.optimize(transformations);
        }

        PythonConfigUtil.setPartitionCustomOperatorNumPartitions(chainedTransformations);
        return executor.createPipeline(
                chainedTransformations, configuration, defaultJobName, jobStatusHookList);
    }

    @Override
    public JobExecutionResult execute(Pipeline pipeline) throws Exception {
        return executor.execute(pipeline);
    }

    @Override
    public JobClient executeAsync(Pipeline pipeline) throws Exception {
        return executor.executeAsync(pipeline);
    }

    @Override
    public boolean isCheckpointingEnabled() {
        return executor.isCheckpointingEnabled();
    }
}
