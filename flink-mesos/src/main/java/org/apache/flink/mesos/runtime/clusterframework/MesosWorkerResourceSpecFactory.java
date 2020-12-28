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

package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpecFactory;

/** Implementation of {@link WorkerResourceSpecFactory} for Mesos deployments. */
public class MesosWorkerResourceSpecFactory extends WorkerResourceSpecFactory {

    public static final MesosWorkerResourceSpecFactory INSTANCE =
            new MesosWorkerResourceSpecFactory();

    private MesosWorkerResourceSpecFactory() {}

    @Override
    public WorkerResourceSpec createDefaultWorkerResourceSpec(Configuration configuration) {
        return workerResourceSpecFromConfigAndCpu(configuration, getDefaultCpus(configuration));
    }

    private static CPUResource getDefaultCpus(Configuration configuration) {
        double fallback = configuration.getDouble(MesosTaskManagerParameters.MESOS_RM_TASKS_CPUS);
        return TaskExecutorProcessUtils.getCpuCoresWithFallback(configuration, fallback);
    }
}
