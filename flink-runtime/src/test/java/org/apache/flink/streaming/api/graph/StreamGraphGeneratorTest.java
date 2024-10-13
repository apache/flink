/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

class StreamGraphGeneratorTest {

    @Test
    void testShouldExecuteInBatchModeWithBoundedSourceAndDefaultMode() {
        final Configuration configuration = new Configuration();
        final StreamGraphGenerator streamGraphGenerator =
                getStreamGraphGeneratorWithBoundedSource(configuration);
        assertThat(streamGraphGenerator.shouldExecuteInBatchMode()).isTrue();
    }

    @Test
    void testShouldExecuteInBatchModeWithBoundedSourceAndBatchMode() {
        final Configuration configuration =
                new Configuration() {
                    {
                        set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
                    }
                };
        final StreamGraphGenerator streamGraphGenerator =
                getStreamGraphGeneratorWithBoundedSource(configuration);
        assertThat(streamGraphGenerator.shouldExecuteInBatchMode()).isTrue();
    }

    @Test
    void testShouldExecuteInBatchModeWithBoundedSourceAndStreamingMode() {
        final Configuration configuration =
                new Configuration() {
                    {
                        set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
                    }
                };
        final StreamGraphGenerator streamGraphGenerator =
                getStreamGraphGeneratorWithBoundedSource(configuration);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(streamGraphGenerator::shouldExecuteInBatchMode);
    }

    @Test
    void testShouldExecuteInBatchModeWithUnBoundedSourceAndDefaultMode() {
        final Configuration configuration = new Configuration();
        final StreamGraphGenerator streamGraphGenerator =
                getStreamGraphGeneratorWithUnBoundedSource(configuration);
        assertThat(streamGraphGenerator.shouldExecuteInBatchMode()).isFalse();
    }

    @Test
    void testShouldExecuteInBatchModeWithUnBoundedSourceAndBatchMode() {
        final Configuration configuration =
                new Configuration() {
                    {
                        set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
                    }
                };
        final StreamGraphGenerator streamGraphGenerator =
                getStreamGraphGeneratorWithUnBoundedSource(configuration);

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(streamGraphGenerator::shouldExecuteInBatchMode);
    }

    @Test
    void testShouldExecuteInBatchModeWithUnBoundedSourceAndStreamingMode() {
        final Configuration configuration =
                new Configuration() {
                    {
                        set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
                    }
                };
        final StreamGraphGenerator streamGraphGenerator =
                getStreamGraphGeneratorWithUnBoundedSource(configuration);
        assertThat(streamGraphGenerator.shouldExecuteInBatchMode()).isFalse();
    }

    private StreamGraphGenerator getStreamGraphGeneratorWithBoundedSource(
            final Configuration configuration) {
        final CheckpointConfig checkpointConfig = new CheckpointConfig(configuration);
        final ExecutionConfig executionConfig = new ExecutionConfig(configuration);

        final SourceTransformation boundedSourceTransformation =
                Mockito.mock(SourceTransformation.class);
        when(boundedSourceTransformation.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        return new StreamGraphGenerator(
                Lists.newArrayList(boundedSourceTransformation),
                executionConfig,
                checkpointConfig,
                configuration);
    }

    private StreamGraphGenerator getStreamGraphGeneratorWithUnBoundedSource(
            final Configuration configuration) {
        final CheckpointConfig checkpointConfig = new CheckpointConfig(configuration);
        final ExecutionConfig executionConfig = new ExecutionConfig(configuration);

        final SourceTransformation boundedSourceTransformation =
                Mockito.mock(SourceTransformation.class);
        when(boundedSourceTransformation.getBoundedness())
                .thenReturn(Boundedness.CONTINUOUS_UNBOUNDED);

        return new StreamGraphGenerator(
                Lists.newArrayList(boundedSourceTransformation),
                executionConfig,
                checkpointConfig,
                configuration);
    }
}
