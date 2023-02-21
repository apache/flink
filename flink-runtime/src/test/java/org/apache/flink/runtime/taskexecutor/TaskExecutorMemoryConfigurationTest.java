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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.configuration.TaskManagerOptions.FRAMEWORK_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.JVM_METASPACE;
import static org.apache.flink.configuration.TaskManagerOptions.JVM_OVERHEAD_FRACTION;
import static org.apache.flink.configuration.TaskManagerOptions.JVM_OVERHEAD_MAX;
import static org.apache.flink.configuration.TaskManagerOptions.JVM_OVERHEAD_MIN;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_FRACTION;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.NETWORK_MEMORY_FRACTION;
import static org.apache.flink.configuration.TaskManagerOptions.NETWORK_MEMORY_MAX;
import static org.apache.flink.configuration.TaskManagerOptions.NETWORK_MEMORY_MIN;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests the initialization of TaskExecutorMemoryConfiguration. */
public class TaskExecutorMemoryConfigurationTest extends TestLogger {

    @Test
    public void testInitialization() {
        Configuration config = new Configuration();

        config.set(FRAMEWORK_HEAP_MEMORY, new MemorySize(1));
        config.set(TASK_HEAP_MEMORY, new MemorySize(2));
        config.set(FRAMEWORK_OFF_HEAP_MEMORY, new MemorySize(3));
        config.set(TASK_OFF_HEAP_MEMORY, new MemorySize(4));
        config.set(NETWORK_MEMORY_MIN, new MemorySize(6));
        config.set(NETWORK_MEMORY_MAX, new MemorySize(6));
        config.set(NETWORK_MEMORY_FRACTION, 0.1f);
        config.set(MANAGED_MEMORY_SIZE, new MemorySize(7));
        config.set(MANAGED_MEMORY_FRACTION, 0.2f);
        config.set(JVM_METASPACE, new MemorySize(8));
        config.set(JVM_OVERHEAD_MIN, new MemorySize(10));
        config.set(JVM_OVERHEAD_MAX, new MemorySize(10));
        config.set(JVM_OVERHEAD_FRACTION, 0.3f);

        TaskExecutorMemoryConfiguration actual = TaskExecutorMemoryConfiguration.create(config);
        TaskExecutorMemoryConfiguration expected =
                new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 6L, 7L, 8L, 10L, 23L, 41L);

        assertThat(actual, is(expected));
    }
}
