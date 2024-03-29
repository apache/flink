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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder for {@link TaskExecutorProcessSpec}. */
public class TaskExecutorProcessSpecBuilder {

    private final Configuration configuration;

    private TaskExecutorProcessSpecBuilder(final Configuration configuration) {
        this.configuration = new Configuration(checkNotNull(configuration));
    }

    static TaskExecutorProcessSpecBuilder newBuilder(final Configuration configuration) {
        return new TaskExecutorProcessSpecBuilder(configuration);
    }

    public TaskExecutorProcessSpecBuilder withCpuCores(double cpuCores) {
        return withCpuCores(new CPUResource(cpuCores));
    }

    public TaskExecutorProcessSpecBuilder withCpuCores(CPUResource cpuCores) {
        configuration.set(TaskManagerOptions.CPU_CORES, cpuCores.getValue().doubleValue());
        return this;
    }

    public TaskExecutorProcessSpecBuilder withTotalProcessMemory(MemorySize totalProcessMemory) {
        configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalProcessMemory);
        return this;
    }

    public TaskExecutorProcessSpec build() {
        return TaskExecutorProcessUtils.processSpecFromConfig(configuration);
    }
}
