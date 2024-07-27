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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that the configuration mechanism for how tasks react on checkpoint errors works correctly.
 */
class CheckpointExceptionHandlerConfigurationTest {

    @Test
    void testCheckpointConfigDefault() {
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = streamExecutionEnvironment.getCheckpointConfig();
        assertThat(checkpointConfig.isFailOnCheckpointingErrors()).isTrue();
        assertThat(checkpointConfig.getTolerableCheckpointFailureNumber()).isZero();
    }

    @Test
    void testSetCheckpointConfig() {
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = streamExecutionEnvironment.getCheckpointConfig();

        // use deprecated API to set not fail on checkpoint errors
        checkpointConfig.setFailOnCheckpointingErrors(false);
        assertThat(checkpointConfig.isFailOnCheckpointingErrors()).isFalse();
        assertThat(checkpointConfig.getTolerableCheckpointFailureNumber())
                .isEqualTo(CheckpointFailureManager.UNLIMITED_TOLERABLE_FAILURE_NUMBER);

        // use new API to set tolerable declined checkpoint number
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        assertThat(checkpointConfig.getTolerableCheckpointFailureNumber()).isEqualTo(5);

        // after we configure the tolerable declined checkpoint number, deprecated API would not
        // take effect
        checkpointConfig.setFailOnCheckpointingErrors(true);
        assertThat(checkpointConfig.getTolerableCheckpointFailureNumber()).isEqualTo(5);
    }

    @Test
    void testPropagationFailFromCheckpointConfig() {
        try {
            doTestPropagationFromCheckpointConfig(true);
        } catch (IllegalArgumentException ignored) {
            // ignored
        }
    }

    @Test
    void testPropagationDeclineFromCheckpointConfig() {
        doTestPropagationFromCheckpointConfig(false);
    }

    public void doTestPropagationFromCheckpointConfig(boolean failTaskOnCheckpointErrors) {
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        streamExecutionEnvironment.getCheckpointConfig().setCheckpointInterval(1000);
        streamExecutionEnvironment
                .getCheckpointConfig()
                .setFailOnCheckpointingErrors(failTaskOnCheckpointErrors);
        streamExecutionEnvironment
                .addSource(
                        new SourceFunction<Integer>() {

                            @Override
                            public void run(SourceContext<Integer> ctx) {}

                            @Override
                            public void cancel() {}
                        })
                .sinkTo(new DiscardingSink<>());
    }
}
