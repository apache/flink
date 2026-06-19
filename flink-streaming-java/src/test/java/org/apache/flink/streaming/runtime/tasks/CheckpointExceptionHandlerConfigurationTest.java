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

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        assertThat(checkpointConfig.getTolerableCheckpointFailureNumber()).isZero();
    }

    @Test
    void testSetCheckpointConfig() {
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = streamExecutionEnvironment.getCheckpointConfig();

        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        assertThat(checkpointConfig.getTolerableCheckpointFailureNumber()).isEqualTo(5);
    }
}
