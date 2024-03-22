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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.datastream.api.ExecutionEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExecutionEnvironmentImpl}. */
class ExecutionEnvironmentImplTest {
    @Test
    void testSetContextExecutionEnvironment() throws Exception {
        ExecutionEnvironment expectedEnv =
                new ExecutionEnvironmentImpl(
                        new DefaultExecutorServiceLoader(), new Configuration(), null);
        TestingExecutionEnvironmentFactory factory =
                new TestingExecutionEnvironmentFactory((config) -> expectedEnv);
        ExecutionEnvironmentImpl.initializeContextEnvironment(factory);
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        assertThat(env).isSameAs(expectedEnv);

        ExecutionEnvironmentImpl.resetContextEnvironment();
        ExecutionEnvironment env2 = ExecutionEnvironment.getInstance();
        assertThat(env2).isNotSameAs(expectedEnv);
    }

    @Test
    void testAddOperator() {
        ExecutionEnvironmentImpl env =
                new ExecutionEnvironmentImpl(
                        new DefaultExecutorServiceLoader(), new Configuration(), null);

        TestingTransformation<Integer> t1 = new TestingTransformation<>("t1", Types.INT, 10);
        TestingTransformation<String> t2 = new TestingTransformation<>("t2", Types.STRING, 5);
        env.addOperator(t1);
        env.addOperator(t2);
        assertThat(env.getTransformations()).containsExactly(t1, t2);
    }

    @Test
    void testSetExecutionMode() {
        ExecutionEnvironmentImpl env =
                new ExecutionEnvironmentImpl(
                        new DefaultExecutorServiceLoader(), new Configuration(), null);
        env.setExecutionMode(RuntimeExecutionMode.BATCH);
        assertThat(env.getExecutionMode()).isEqualTo(RuntimeExecutionMode.BATCH);
    }
}
