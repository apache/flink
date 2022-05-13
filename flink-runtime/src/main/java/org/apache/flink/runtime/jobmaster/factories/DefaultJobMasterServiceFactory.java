/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTrackerImpl;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentReconciler;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterService;
import org.apache.flink.runtime.jobmaster.SlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.function.FunctionUtils;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class DefaultJobMasterServiceFactory implements JobMasterServiceFactory {

    private final Executor executor;
    private final RpcService rpcService;
    private final JobMasterConfiguration jobMasterConfiguration;
    private final JobGraph jobGraph;
    private final HighAvailabilityServices haServices;
    private final SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory;
    private final JobManagerSharedServices jobManagerSharedServices;
    private final HeartbeatServices heartbeatServices;
    private final JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory;
    private final FatalErrorHandler fatalErrorHandler;
    private final ClassLoader userCodeClassloader;
    private final ShuffleMaster<?> shuffleMaster;
    private final long initializationTimestamp;

    public DefaultJobMasterServiceFactory(
            Executor executor,
            RpcService rpcService,
            JobMasterConfiguration jobMasterConfiguration,
            JobGraph jobGraph,
            HighAvailabilityServices haServices,
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            JobManagerSharedServices jobManagerSharedServices,
            HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
            FatalErrorHandler fatalErrorHandler,
            ClassLoader userCodeClassloader,
            long initializationTimestamp) {
        this.executor = executor;
        this.rpcService = rpcService;
        this.jobMasterConfiguration = jobMasterConfiguration;
        this.jobGraph = jobGraph;
        this.haServices = haServices;
        this.slotPoolServiceSchedulerFactory = slotPoolServiceSchedulerFactory;
        this.jobManagerSharedServices = jobManagerSharedServices;
        this.heartbeatServices = heartbeatServices;
        this.jobManagerJobMetricGroupFactory = jobManagerJobMetricGroupFactory;
        this.fatalErrorHandler = fatalErrorHandler;
        this.userCodeClassloader = userCodeClassloader;
        this.shuffleMaster = jobManagerSharedServices.getShuffleMaster();
        this.initializationTimestamp = initializationTimestamp;
    }

    @Override
    public CompletableFuture<JobMasterService> createJobMasterService(
            UUID leaderSessionId, OnCompletionActions onCompletionActions) {

        return CompletableFuture.supplyAsync(
                FunctionUtils.uncheckedSupplier(
                        () -> internalCreateJobMasterService(leaderSessionId, onCompletionActions)),
                executor);
    }

    private JobMasterService internalCreateJobMasterService(
            UUID leaderSessionId, OnCompletionActions onCompletionActions) throws Exception {

        final JobMaster jobMaster =
                new JobMaster(
                        rpcService,
                        JobMasterId.fromUuidOrNull(leaderSessionId),
                        jobMasterConfiguration,
                        ResourceID.generate(),
                        jobGraph,
                        haServices,
                        slotPoolServiceSchedulerFactory,
                        jobManagerSharedServices,
                        heartbeatServices,
                        jobManagerJobMetricGroupFactory,
                        onCompletionActions,
                        fatalErrorHandler,
                        userCodeClassloader,
                        shuffleMaster,
                        lookup ->
                                new JobMasterPartitionTrackerImpl(
                                        jobGraph.getJobID(), shuffleMaster, lookup),
                        new DefaultExecutionDeploymentTracker(),
                        DefaultExecutionDeploymentReconciler::new,
                        initializationTimestamp);

        jobMaster.start();

        return jobMaster;
    }
}
