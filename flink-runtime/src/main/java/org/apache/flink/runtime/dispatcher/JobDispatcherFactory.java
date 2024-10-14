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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava32.com.google.common.collect.Iterables;

import java.util.Collection;

import static org.apache.flink.runtime.entrypoint.ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE;

/** {@link DispatcherFactory} which creates a {@link MiniDispatcher}. */
public enum JobDispatcherFactory implements DispatcherFactory {
    INSTANCE;

    @Override
    public MiniDispatcher createDispatcher(
            RpcService rpcService,
            DispatcherId fencingToken,
            Collection<ExecutionPlan> recoveredJobs,
            Collection<JobResult> recoveredDirtyJobResults,
            DispatcherBootstrapFactory dispatcherBootstrapFactory,
            PartialDispatcherServicesWithJobPersistenceComponents
                    partialDispatcherServicesWithJobPersistenceComponents)
            throws Exception {
        final ExecutionPlan recoveredExecutionPlan = Iterables.getOnlyElement(recoveredJobs, null);
        final JobResult recoveredDirtyJob =
                Iterables.getOnlyElement(recoveredDirtyJobResults, null);

        Preconditions.checkArgument(
                recoveredExecutionPlan == null ^ recoveredDirtyJob == null,
                "Either the ExecutionPlan or the recovered JobResult needs to be specified.");

        final Configuration configuration =
                partialDispatcherServicesWithJobPersistenceComponents.getConfiguration();
        final String executionModeValue = configuration.get(INTERNAL_CLUSTER_EXECUTION_MODE);
        final ClusterEntrypoint.ExecutionMode executionMode =
                ClusterEntrypoint.ExecutionMode.valueOf(executionModeValue);

        return new MiniDispatcher(
                rpcService,
                fencingToken,
                DispatcherServices.from(
                        partialDispatcherServicesWithJobPersistenceComponents,
                        JobMasterServiceLeadershipRunnerFactory.INSTANCE,
                        CheckpointResourcesCleanupRunnerFactory.INSTANCE),
                recoveredExecutionPlan,
                recoveredDirtyJob,
                dispatcherBootstrapFactory,
                executionMode);
    }
}
