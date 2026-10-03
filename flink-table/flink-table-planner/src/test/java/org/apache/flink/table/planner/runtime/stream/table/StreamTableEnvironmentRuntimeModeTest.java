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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link StreamTableEnvironment#create(StreamExecutionEnvironment)} inherits the runtime
 * execution mode from the given {@link StreamExecutionEnvironment} instead of always defaulting to
 * streaming (FLINK-39014).
 */
class StreamTableEnvironmentRuntimeModeTest {

    @Test
    void testCreateInheritsBatchRuntimeMode() {
        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        assertThat(tEnv.getConfig().get(ExecutionOptions.RUNTIME_MODE))
                .isEqualTo(RuntimeExecutionMode.BATCH);
    }

    @Test
    void testCreateDefaultsToStreamingRuntimeMode() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        assertThat(tEnv.getConfig().get(ExecutionOptions.RUNTIME_MODE))
                .isEqualTo(RuntimeExecutionMode.STREAMING);
    }
}
