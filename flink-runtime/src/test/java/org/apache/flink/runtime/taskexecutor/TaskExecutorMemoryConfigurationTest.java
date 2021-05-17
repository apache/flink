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
import static org.apache.flink.configuration.TaskManagerOptions.TOTAL_FLINK_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.TOTAL_PROCESS_MEMORY;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests the initialization of TaskExecutorMemoryConfiguration. */
public class TaskExecutorMemoryConfigurationTest extends TestLogger {

    @Test
    public void testInitializationWithAllValuesBeingSet() {
        Configuration config = new Configuration();

        config.set(FRAMEWORK_HEAP_MEMORY, new MemorySize(1));
        config.set(TASK_HEAP_MEMORY, new MemorySize(2));
        config.set(FRAMEWORK_OFF_HEAP_MEMORY, new MemorySize(3));
        config.set(TASK_OFF_HEAP_MEMORY, new MemorySize(4));
        config.set(NETWORK_MEMORY_MIN, new MemorySize(5));
        config.set(NETWORK_MEMORY_MAX, new MemorySize(6));
        config.set(NETWORK_MEMORY_FRACTION, 0.1f);
        config.set(MANAGED_MEMORY_SIZE, new MemorySize(7));
        config.set(MANAGED_MEMORY_FRACTION, 0.2f);
        config.set(JVM_METASPACE, new MemorySize(8));
        config.set(JVM_OVERHEAD_MIN, new MemorySize(9));
        config.set(JVM_OVERHEAD_MAX, new MemorySize(10));
        config.set(JVM_OVERHEAD_FRACTION, 0.3f);
        config.set(TOTAL_FLINK_MEMORY, new MemorySize(11));
        config.set(TOTAL_PROCESS_MEMORY, new MemorySize(12));

        TaskExecutorMemoryConfiguration actual = TaskExecutorMemoryConfiguration.create(config);
        TaskExecutorMemoryConfiguration expected =
                new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 6L, 7L, 8L, 10L, 11L, 12L);

        assertThat(actual, is(expected));
    }

    @Test
    public void testInitializationWithMissingValues() {
        Configuration config = new Configuration();

        TaskExecutorMemoryConfiguration actual = TaskExecutorMemoryConfiguration.create(config);
        TaskExecutorMemoryConfiguration expected =
                new TaskExecutorMemoryConfiguration(
                        FRAMEWORK_HEAP_MEMORY.defaultValue().getBytes(),
                        null,
                        FRAMEWORK_OFF_HEAP_MEMORY.defaultValue().getBytes(),
                        TASK_OFF_HEAP_MEMORY.defaultValue().getBytes(),
                        NETWORK_MEMORY_MAX.defaultValue().getBytes(),
                        null,
                        JVM_METASPACE.defaultValue().getBytes(),
                        JVM_OVERHEAD_MAX.defaultValue().getBytes(),
                        null,
                        null);

        assertThat(actual, is(expected));
    }
}
