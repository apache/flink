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

package org.apache.flink.state.api.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.flink.runtime.memory.MemoryManager.DEFAULT_PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link SavepointEnvironment} reuses the enclosing task's real {@link IOManager},
 * {@link MemoryManager}, {@link org.apache.flink.runtime.memory.SharedResources} and {@link
 * TaskManagerRuntimeInfo} instead of fabricating its own.
 */
class SavepointEnvironmentTest {

    private final IOManager ioManager = new IOManagerAsync();

    @AfterEach
    void closeIOManager() throws Exception {
        ioManager.close();
    }

    @Test
    void reusesEnclosingTaskResourceManagers() throws Exception {
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(4 * DEFAULT_PAGE_SIZE).build();
        TaskManagerRuntimeInfo taskManagerRuntimeInfo = new TestingTaskManagerRuntimeInfo();

        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setIOManager(ioManager)
                        .setMemoryManager(memoryManager)
                        .setTaskManagerRuntimeInfo(taskManagerRuntimeInfo)
                        .build();

        MockStreamingRuntimeContext ctx = new MockStreamingRuntimeContext(1, 0, environment);

        SavepointEnvironment savepointEnvironment =
                new SavepointEnvironment.Builder(ctx, new ExecutionConfig(), 1).build();

        assertThat(savepointEnvironment.getIOManager()).isSameAs(ioManager);
        assertThat(savepointEnvironment.getMemoryManager()).isSameAs(memoryManager);
        assertThat(savepointEnvironment.getSharedResources())
                .isSameAs(environment.getSharedResources());
        assertThat(savepointEnvironment.getTaskManagerInfo()).isSameAs(taskManagerRuntimeInfo);

        environment.close();
    }

    @Test
    void requiresARealStreamingRuntimeContext() {
        RuntimeContext ctx =
                new RuntimeUDFContext(
                        new TaskInfoImpl("task", 1, 0, 1, 0),
                        getClass().getClassLoader(),
                        new ExecutionConfig(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());

        assertThatThrownBy(
                        () ->
                                new SavepointEnvironment.Builder(ctx, new ExecutionConfig(), 1)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
