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

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.state.api.input.splits.PrioritizedOperatorSubtaskStateInputSplit;
import org.apache.flink.state.api.runtime.NeverFireProcessingTimeService;
import org.apache.flink.state.api.runtime.SavepointEnvironment;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;

/** Utility for creating a {@link StreamOperatorStateContext}. */
@Internal
class StreamOperatorContextBuilder {

    private final RuntimeContext ctx;

    private final ExecutionConfig executionConfig;

    private final Configuration configuration;

    private final OperatorState operatorState;

    private int maxParallelism;

    private final PrioritizedOperatorSubtaskStateInputSplit split;

    private final CloseableRegistry registry;

    @Nullable private final StateBackend applicationStateBackend;

    private KeyContext keyContext = new VoidKeyContext();

    @Nullable private TypeSerializer<?> keySerializer;

    StreamOperatorContextBuilder(
            RuntimeContext ctx,
            Configuration configuration,
            OperatorState operatorState,
            PrioritizedOperatorSubtaskStateInputSplit split,
            CloseableRegistry registry,
            @Nullable StateBackend applicationStateBackend,
            ExecutionConfig executionConfig) {
        this.ctx = ctx;
        this.maxParallelism = ctx.getTaskInfo().getMaxNumberOfParallelSubtasks();
        this.configuration = configuration;
        this.operatorState = operatorState;
        this.split = split;
        this.registry = registry;
        this.applicationStateBackend = applicationStateBackend;
        this.executionConfig = executionConfig;
    }

    StreamOperatorContextBuilder withMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
        return this;
    }

    StreamOperatorContextBuilder withKey(KeyContext keyContext, TypeSerializer<?> keySerializer) {
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        return this;
    }

    StreamOperatorStateContext build(Logger logger) throws IOException {
        final Environment environment =
                new SavepointEnvironment.Builder(ctx, executionConfig, maxParallelism)
                        .setConfiguration(configuration)
                        .setSubtaskIndex(split.getSplitNumber())
                        .setPrioritizedOperatorSubtaskState(
                                split.getPrioritizedOperatorSubtaskState())
                        .build();

        StateBackend stateBackend;
        try {
            Configuration jobConfig = environment.getJobConfiguration();
            jobConfig.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, false);
            Configuration clusterConfig = new Configuration(configuration);
            stateBackend =
                    StateBackendLoader.fromApplicationOrConfigOrDefault(
                            applicationStateBackend,
                            jobConfig,
                            clusterConfig,
                            ctx.getUserCodeClassLoader(),
                            logger);
        } catch (DynamicCodeLoadingException e) {
            throw new IOException("Failed to load state backend", e);
        }

        StreamTaskStateInitializer initializer =
                new StreamTaskStateInitializerImpl(environment, stateBackend);

        try {
            return initializer.streamOperatorStateContext(
                    operatorState.getOperatorID(),
                    operatorState.getOperatorID().toString(),
                    new NeverFireProcessingTimeService(),
                    keyContext,
                    keySerializer,
                    registry,
                    ctx.getMetricGroup(),
                    1.0,
                    false,
                    false);
        } catch (Exception e) {
            throw new IOException("Failed to restore state backend", e);
        }
    }

    private static class VoidKeyContext implements KeyContext {

        @Override
        public void setCurrentKey(Object key) {}

        @Override
        public Object getCurrentKey() {
            return null;
        }
    }
}
