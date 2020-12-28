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

package org.apache.flink.runtime.resourcemanager.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.QuadFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/** Implementation of the {@link ResourceManagerGateway} for testing purposes solely. */
public class TestingResourceManagerGateway implements ResourceManagerGateway {

    private final ResourceManagerId resourceManagerId;

    private final ResourceID ownResourceId;

    private final String address;

    private final String hostname;

    private final AtomicReference<CompletableFuture<Acknowledge>> slotFutureReference;

    private volatile Consumer<AllocationID> cancelSlotConsumer;

    private volatile Consumer<SlotRequest> requestSlotConsumer;

    private volatile QuadFunction<
                    JobMasterId, ResourceID, String, JobID, CompletableFuture<RegistrationResponse>>
            registerJobManagerFunction;

    private volatile Consumer<Tuple2<JobID, Throwable>> disconnectJobManagerConsumer;

    private volatile Function<TaskExecutorRegistration, CompletableFuture<RegistrationResponse>>
            registerTaskExecutorFunction;

    private volatile Function<Tuple2<ResourceID, FileType>, CompletableFuture<TransientBlobKey>>
            requestTaskManagerFileUploadByTypeFunction;

    private volatile Function<Tuple2<ResourceID, String>, CompletableFuture<TransientBlobKey>>
            requestTaskManagerFileUploadByNameFunction;

    private volatile Consumer<Tuple2<ResourceID, Throwable>> disconnectTaskExecutorConsumer;

    private volatile Function<
                    Tuple3<ResourceID, InstanceID, SlotReport>, CompletableFuture<Acknowledge>>
            sendSlotReportFunction;

    private volatile BiConsumer<ResourceID, TaskExecutorHeartbeatPayload>
            taskExecutorHeartbeatConsumer;

    private volatile Consumer<Tuple3<InstanceID, SlotID, AllocationID>> notifySlotAvailableConsumer;

    private volatile Function<ResourceID, CompletableFuture<Collection<LogInfo>>>
            requestTaskManagerLogListFunction;

    private volatile Function<ResourceID, CompletableFuture<TaskManagerInfo>>
            requestTaskManagerInfoFunction;

    private volatile Function<ResourceID, CompletableFuture<ThreadDumpInfo>>
            requestThreadDumpFunction;

    private volatile BiFunction<JobMasterId, ResourceRequirements, CompletableFuture<Acknowledge>>
            declareRequiredResourcesFunction =
                    (ignoredA, ignoredB) ->
                            FutureUtils.completedExceptionally(new UnsupportedOperationException());

    public TestingResourceManagerGateway() {
        this(
                ResourceManagerId.generate(),
                ResourceID.generate(),
                "localhost/" + UUID.randomUUID(),
                "localhost");
    }

    public TestingResourceManagerGateway(
            ResourceManagerId resourceManagerId,
            ResourceID resourceId,
            String address,
            String hostname) {
        this.resourceManagerId = Preconditions.checkNotNull(resourceManagerId);
        this.ownResourceId = Preconditions.checkNotNull(resourceId);
        this.address = Preconditions.checkNotNull(address);
        this.hostname = Preconditions.checkNotNull(hostname);
        this.slotFutureReference = new AtomicReference<>();
        this.cancelSlotConsumer = null;
        this.requestSlotConsumer = null;
    }

    public ResourceID getOwnResourceId() {
        return ownResourceId;
    }

    public void setRequestSlotFuture(CompletableFuture<Acknowledge> slotFuture) {
        this.slotFutureReference.set(slotFuture);
    }

    public void setCancelSlotConsumer(Consumer<AllocationID> cancelSlotConsumer) {
        this.cancelSlotConsumer = cancelSlotConsumer;
    }

    public void setRequestSlotConsumer(Consumer<SlotRequest> slotRequestConsumer) {
        this.requestSlotConsumer = slotRequestConsumer;
    }

    public void setRegisterJobManagerFunction(
            QuadFunction<
                            JobMasterId,
                            ResourceID,
                            String,
                            JobID,
                            CompletableFuture<RegistrationResponse>>
                    registerJobManagerFunction) {
        this.registerJobManagerFunction = registerJobManagerFunction;
    }

    public void setDisconnectJobManagerConsumer(
            Consumer<Tuple2<JobID, Throwable>> disconnectJobManagerConsumer) {
        this.disconnectJobManagerConsumer = disconnectJobManagerConsumer;
    }

    public void setRegisterTaskExecutorFunction(
            Function<TaskExecutorRegistration, CompletableFuture<RegistrationResponse>>
                    registerTaskExecutorFunction) {
        this.registerTaskExecutorFunction = registerTaskExecutorFunction;
    }

    public void setRequestTaskManagerFileUploadByTypeFunction(
            Function<Tuple2<ResourceID, FileType>, CompletableFuture<TransientBlobKey>>
                    requestTaskManagerFileUploadByTypeFunction) {
        this.requestTaskManagerFileUploadByTypeFunction =
                requestTaskManagerFileUploadByTypeFunction;
    }

    public void setRequestTaskManagerFileUploadByNameFunction(
            Function<Tuple2<ResourceID, String>, CompletableFuture<TransientBlobKey>>
                    requestTaskManagerFileUploadByNameFunction) {
        this.requestTaskManagerFileUploadByNameFunction =
                requestTaskManagerFileUploadByNameFunction;
    }

    public void setRequestTaskManagerLogListFunction(
            Function<ResourceID, CompletableFuture<Collection<LogInfo>>>
                    requestTaskManagerLogListFunction) {
        this.requestTaskManagerLogListFunction = requestTaskManagerLogListFunction;
    }

    public void setRequestTaskManagerInfoFunction(
            Function<ResourceID, CompletableFuture<TaskManagerInfo>>
                    requestTaskManagerInfoFunction) {
        this.requestTaskManagerInfoFunction = requestTaskManagerInfoFunction;
    }

    public void setDisconnectTaskExecutorConsumer(
            Consumer<Tuple2<ResourceID, Throwable>> disconnectTaskExecutorConsumer) {
        this.disconnectTaskExecutorConsumer = disconnectTaskExecutorConsumer;
    }

    public void setSendSlotReportFunction(
            Function<Tuple3<ResourceID, InstanceID, SlotReport>, CompletableFuture<Acknowledge>>
                    sendSlotReportFunction) {
        this.sendSlotReportFunction = sendSlotReportFunction;
    }

    public void setTaskExecutorHeartbeatConsumer(
            BiConsumer<ResourceID, TaskExecutorHeartbeatPayload> taskExecutorHeartbeatConsumer) {
        this.taskExecutorHeartbeatConsumer = taskExecutorHeartbeatConsumer;
    }

    public void setNotifySlotAvailableConsumer(
            Consumer<Tuple3<InstanceID, SlotID, AllocationID>> notifySlotAvailableConsumer) {
        this.notifySlotAvailableConsumer = notifySlotAvailableConsumer;
    }

    public void setRequestThreadDumpFunction(
            Function<ResourceID, CompletableFuture<ThreadDumpInfo>> requestThreadDumpFunction) {
        this.requestThreadDumpFunction = requestThreadDumpFunction;
    }

    public void setDeclareRequiredResourcesFunction(
            BiFunction<JobMasterId, ResourceRequirements, CompletableFuture<Acknowledge>>
                    declareRequiredResourcesFunction) {
        this.declareRequiredResourcesFunction = declareRequiredResourcesFunction;
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerJobManager(
            JobMasterId jobMasterId,
            ResourceID jobMasterResourceId,
            String jobMasterAddress,
            JobID jobId,
            Time timeout) {
        final QuadFunction<
                        JobMasterId,
                        ResourceID,
                        String,
                        JobID,
                        CompletableFuture<RegistrationResponse>>
                currentConsumer = registerJobManagerFunction;

        if (currentConsumer != null) {
            return currentConsumer.apply(jobMasterId, jobMasterResourceId, jobMasterAddress, jobId);
        }

        return CompletableFuture.completedFuture(getJobMasterRegistrationSuccess());
    }

    public JobMasterRegistrationSuccess getJobMasterRegistrationSuccess() {
        return new JobMasterRegistrationSuccess(resourceManagerId, ownResourceId);
    }

    @Override
    public CompletableFuture<Acknowledge> requestSlot(
            JobMasterId jobMasterId, SlotRequest slotRequest, Time timeout) {
        Consumer<SlotRequest> currentRequestSlotConsumer = requestSlotConsumer;

        if (currentRequestSlotConsumer != null) {
            currentRequestSlotConsumer.accept(slotRequest);
        }

        CompletableFuture<Acknowledge> slotFuture = slotFutureReference.getAndSet(null);

        if (slotFuture != null) {
            return slotFuture;
        } else {
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    @Override
    public CompletableFuture<Acknowledge> declareRequiredResources(
            JobMasterId jobMasterId, ResourceRequirements resourceRequirements, Time timeout) {
        return declareRequiredResourcesFunction.apply(jobMasterId, resourceRequirements);
    }

    @Override
    public void cancelSlotRequest(AllocationID allocationID) {
        Consumer<AllocationID> currentCancelSlotConsumer = cancelSlotConsumer;

        if (currentCancelSlotConsumer != null) {
            currentCancelSlotConsumer.accept(allocationID);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> sendSlotReport(
            ResourceID taskManagerResourceId,
            InstanceID taskManagerRegistrationId,
            SlotReport slotReport,
            Time timeout) {
        final Function<Tuple3<ResourceID, InstanceID, SlotReport>, CompletableFuture<Acknowledge>>
                currentSendSlotReportFunction = sendSlotReportFunction;

        if (currentSendSlotReportFunction != null) {
            return currentSendSlotReportFunction.apply(
                    Tuple3.of(taskManagerResourceId, taskManagerRegistrationId, slotReport));
        } else {
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerTaskExecutor(
            TaskExecutorRegistration taskExecutorRegistration, Time timeout) {
        final Function<TaskExecutorRegistration, CompletableFuture<RegistrationResponse>>
                currentFunction = registerTaskExecutorFunction;

        if (currentFunction != null) {
            return currentFunction.apply(taskExecutorRegistration);
        } else {
            return CompletableFuture.completedFuture(
                    new TaskExecutorRegistrationSuccess(
                            new InstanceID(),
                            ownResourceId,
                            new ClusterInformation("localhost", 1234)));
        }
    }

    @Override
    public void notifySlotAvailable(
            InstanceID instanceId, SlotID slotID, AllocationID oldAllocationId) {
        final Consumer<Tuple3<InstanceID, SlotID, AllocationID>>
                currentNotifySlotAvailableConsumer = notifySlotAvailableConsumer;

        if (currentNotifySlotAvailableConsumer != null) {
            currentNotifySlotAvailableConsumer.accept(
                    Tuple3.of(instanceId, slotID, oldAllocationId));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> deregisterApplication(
            ApplicationStatus finalStatus, String diagnostics) {
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
        return CompletableFuture.completedFuture(0);
    }

    @Override
    public void heartbeatFromTaskManager(
            ResourceID heartbeatOrigin, TaskExecutorHeartbeatPayload heartbeatPayload) {
        final BiConsumer<ResourceID, TaskExecutorHeartbeatPayload>
                currentTaskExecutorHeartbeatConsumer = taskExecutorHeartbeatConsumer;

        if (currentTaskExecutorHeartbeatConsumer != null) {
            currentTaskExecutorHeartbeatConsumer.accept(heartbeatOrigin, heartbeatPayload);
        }
    }

    @Override
    public void heartbeatFromJobManager(ResourceID heartbeatOrigin) {}

    @Override
    public void disconnectTaskManager(ResourceID resourceID, Exception cause) {
        final Consumer<Tuple2<ResourceID, Throwable>> currentConsumer =
                disconnectTaskExecutorConsumer;

        if (currentConsumer != null) {
            currentConsumer.accept(Tuple2.of(resourceID, cause));
        }
    }

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {
        final Consumer<Tuple2<JobID, Throwable>> currentConsumer = disconnectJobManagerConsumer;

        if (currentConsumer != null) {
            currentConsumer.accept(Tuple2.of(jobId, cause));
        }
    }

    @Override
    public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<TaskManagerInfo> requestTaskManagerInfo(
            ResourceID resourceId, Time timeout) {
        final Function<ResourceID, CompletableFuture<TaskManagerInfo>> function =
                requestTaskManagerInfoFunction;

        if (function != null) {
            return function.apply(resourceId);
        } else {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException("No requestTaskManagerInfoFunction was set."));
        }
    }

    @Override
    public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
        return CompletableFuture.completedFuture(
                new ResourceOverview(1, 1, 1, ResourceProfile.ZERO, ResourceProfile.ZERO));
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByType(
            ResourceID taskManagerId, FileType fileType, Time timeout) {
        final Function<Tuple2<ResourceID, FileType>, CompletableFuture<TransientBlobKey>> function =
                requestTaskManagerFileUploadByTypeFunction;

        if (function != null) {
            return function.apply(Tuple2.of(taskManagerId, fileType));
        } else {
            return CompletableFuture.completedFuture(new TransientBlobKey());
        }
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByName(
            ResourceID taskManagerId, String fileName, Time timeout) {
        final Function<Tuple2<ResourceID, String>, CompletableFuture<TransientBlobKey>> function =
                requestTaskManagerFileUploadByNameFunction;

        if (function != null) {
            return function.apply(Tuple2.of(taskManagerId, fileName));
        } else {
            return CompletableFuture.completedFuture(new TransientBlobKey());
        }
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestTaskManagerLogList(
            ResourceID taskManagerId, Time timeout) {
        final Function<ResourceID, CompletableFuture<Collection<LogInfo>>> function =
                this.requestTaskManagerLogListFunction;
        if (function != null) {
            return function.apply(taskManagerId);
        } else {
            return FutureUtils.completedExceptionally(
                    new UnknownTaskExecutorException(taskManagerId));
        }
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(
            ResourceID taskManagerId, Time timeout) {
        final Function<ResourceID, CompletableFuture<ThreadDumpInfo>> function =
                this.requestThreadDumpFunction;

        if (function != null) {
            return function.apply(taskManagerId);
        } else {
            return FutureUtils.completedExceptionally(
                    new UnknownTaskExecutorException(taskManagerId));
        }
    }

    @Override
    public ResourceManagerId getFencingToken() {
        return resourceManagerId;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public CompletableFuture<Map<IntermediateDataSetID, DataSetMetaInfo>> listDataSets() {
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    @Override
    public CompletableFuture<Void> releaseClusterPartitions(
            IntermediateDataSetID dataSetToRelease) {
        return CompletableFuture.completedFuture(null);
    }
}
