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

package org.apache.flink.state.api;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.api.utils.CustomStateBackendFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the savepoint writer. */
class SavepointWriterTest {

    @Test
    void testCustomStateBackend() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(
                StateBackendOptions.STATE_BACKEND,
                CustomStateBackendFactory.class.getCanonicalName());
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        assertThatThrownBy(() -> env.configure(configuration))
                .isInstanceOf(CustomStateBackendFactory.ExpectedException.class);
    }

    @Test
    void testCantCreateSavepointFromNothing() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(() -> SavepointWriter.newSavepoint(env, 128).write("file:///tmp/path"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("at least one operator to be created");
    }

    @Test
    @SuppressWarnings("deprecation")
    void testMustContainOneOperatorWithoutEnvironment() {
        assertThatThrownBy(() -> SavepointWriter.newSavepoint(128).write("file:///tmp/path"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("if no execution environment was provided");
    }
}
