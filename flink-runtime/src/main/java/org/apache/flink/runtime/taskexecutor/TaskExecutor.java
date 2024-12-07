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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.JobPermanentBlobService;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.TaskExecutorBlobService;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatReceiver;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionInfo;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationRejection;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.security.token.DelegationTokenReceiverRepository;
import org.apache.flink.runtime.shuffle.DefaultPartitionWithMetrics;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.FileMergingSnapshotManagerClosableWrapper;
import org.apache.flink.runtime.state.TaskExecutorChannelStateExecutorFactoryManager;
import org.apache.flink.runtime.state.TaskExecutorFileMergingManager;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskManagerException;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcGlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcInputSplitProvider;
import org.apache.flink.runtime.taskexecutor.rpc.RpcKvStateRegistryListener;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcTaskOperatorEventGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotActions;
import org.apache.flink.runtime.taskexecutor.slot.SlotAllocationSnapshot;
import org.apache.flink.runtime.taskexecutor.slot.SlotAllocationSnapshotPersistenceService;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotActiveException;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlot;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.GroupCache;
import org.apache.flink.runtime.util.profiler.ProfilingService;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkExpectedException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.MdcUtils.MdcCloseable;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.shuffle.ShuffleServiceOptions.NETTY_SHUFFLE_SERVICE_FACTORY_CLASS;
import static org.apache.flink.runtime.shuffle.ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link Task}.
 */
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

    public static final String TASK_MANAGER_NAME = "taskmanager";

    /** The access to the leader election and retrieval services. */
    private final HighAvailabilityServices haServices;

    private final TaskManagerServices taskExecutorServices;

    /** The task manager configuration. */
    private final TaskManagerConfiguration taskManagerConfiguration;

    /** The fatal error handler to use in case of a fatal error. */
    private final FatalErrorHandler fatalErrorHandler;

    private final TaskExecutorBlobService taskExecutorBlobService;

    private final LibraryCacheManager libraryCacheManager;

    /** The address to metric query service on this Task Manager. */
    @Nullable private final String metricQueryServiceAddress;

    // --------- TaskManager services --------

    /** The connection information of this task manager. */
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

    private final TaskManagerMetricGroup taskManagerMetricGroup;

    /** The state manager for this task, providing state managers per slot. */
    private final TaskExecutorLocalStateStoresManager localStateStoresManager;

    /**
     * The file merging manager for this task, providing file merging snapshot manager per job, see
     * {@link FileMergingSnapshotManager} for details.
     */
    private final TaskExecutorFileMergingManager fileMergingManager;

    /** The changelog manager for this task, providing changelog storage per job. */
    private final TaskExecutorStateChangelogStoragesManager changelogStoragesManager;

    /**
     * The channel state executor factory manager for this task, providing channel state executor
     * factory per job.
     */
    private final TaskExecutorChannelStateExecutorFactoryManager channelStateExecutorFactoryManager;

    /** Information provider for external resources. */
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The network component in the task manager. */
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;

    /** The kvState registration service in the task manager. */
    private final KvStateService kvStateService;

    private final Executor ioExecutor;

    /** {@link MemoryManager} shared across all tasks. */
    private final SharedResources sharedResources;

    // --------- task slot allocation table -----------

    private final TaskSlotTable<Task> taskSlotTable;

    private final Map<JobID, UUID> currentSlotOfferPerJob = new HashMap<>();

    private final JobTable jobTable;

    private final JobLeaderService jobLeaderService;

    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    private final SlotAllocationSnapshotPersistenceService slotAllocationSnapshotPersistenceService;

    // ------------------------------------------------------------------------

    private final HardwareDescription hardwareDescription;

    private final TaskExecutorMemoryConfiguration memoryConfiguration;

    private FileCache fileCache;

    /** The heartbeat manager for job manager in the task manager. */
    private final HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload>
            jobManagerHeartbeatManager;

    /** The heartbeat manager for resource manager in the task manager. */
    private final HeartbeatManager<Void, TaskExecutorHeartbeatPayload>
            resourceManagerHeartbeatManager;

    private final TaskExecutorPartitionTracker partitionTracker;

    private final DelegationTokenReceiverRepository delegationTokenReceiverRepository;

    // --------- resource manager --------

    @Nullable private ResourceManagerAddress resourceManagerAddress;

    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    @Nullable private TaskExecutorToResourceManagerConnection resourceManagerConnection;

    @Nullable private UUID currentRegistrationTimeoutId;

    private final Map<JobID, Collection<CompletableFuture<ExecutionState>>>
            taskResultPartitionCleanupFuturesPerJob = CollectionUtil.newHashMapWithExpectedSize(8);

    private final ThreadInfoSampleService threadInfoSampleService;

    private final GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache;
    private final GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache;
    private final GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup>
            shuffleDescriptorsCache;

    private final ProfilingService profilingService;

    private final Set<JobID> jobPartitionToCleanupSet = new HashSet<>();

    public TaskExecutor(
            RpcService rpcService,
            TaskManagerConfiguration taskManagerConfiguration,
            HighAvailabilityServices haServices,
            TaskManagerServices taskExecutorServices,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            HeartbeatServices heartbeatServices,
            TaskManagerMetricGroup taskManagerMetricGroup,
            @Nullable String metricQueryServiceAddress,
            TaskExecutorBlobService taskExecutorBlobService,
            FatalErrorHandler fatalErrorHandler,
            TaskExecutorPartitionTracker partitionTracker,
            DelegationTokenReceiverRepository delegationTokenReceiverRepository) {

        super(rpcService, RpcServiceUtils.createRandomName(TASK_MANAGER_NAME));

        checkArgument(
                taskManagerConfiguration.getNumberSlots() > 0,
                "The number of slots has to be larger than 0.");

        this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
        this.taskExecutorServices = checkNotNull(taskExecutorServices);
        this.haServices = checkNotNull(haServices);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.partitionTracker = partitionTracker;
        this.delegationTokenReceiverRepository = checkNotNull(delegationTokenReceiverRepository);
        this.taskManagerMetricGroup = checkNotNull(taskManagerMetricGroup);
        this.taskExecutorBlobService = checkNotNull(taskExecutorBlobService);
        this.metricQueryServiceAddress = metricQueryServiceAddress;
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.libraryCacheManager = taskExecutorServices.getLibraryCacheManager();
        this.taskSlotTable = taskExecutorServices.getTaskSlotTable();
        this.jobTable = taskExecutorServices.getJobTable();
        this.jobLeaderService = taskExecutorServices.getJobLeaderService();
        this.unresolvedTaskManagerLocation =
                taskExecutorServices.getUnresolvedTaskManagerLocation();
        this.localStateStoresManager = taskExecutorServices.getTaskManagerStateStore();
        this.fileMergingManager = taskExecutorServices.getTaskManagerFileMergingManager();
        this.changelogStoragesManager = taskExecutorServices.getTaskManagerChangelogManager();
        this.channelStateExecutorFactoryManager =
                taskExecutorServices.getTaskManagerChannelStateManager();
        this.shuffleEnvironment = taskExecutorServices.getShuffleEnvironment();
        this.kvStateService = taskExecutorServices.getKvStateService();
        this.ioExecutor = taskExecutorServices.getIOExecutor();
        this.resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();

        this.hardwareDescription =
                HardwareDescription.extractFromSystem(taskExecutorServices.getManagedMemorySize());
        this.memoryConfiguration =
                TaskExecutorMemoryConfiguration.create(taskManagerConfiguration.getConfiguration());

        this.resourceManagerAddress = null;
        this.resourceManagerConnection = null;
        this.currentRegistrationTimeoutId = null;

        final ResourceID resourceId =
                taskExecutorServices.getUnresolvedTaskManagerLocation().getResourceID();
        this.jobManagerHeartbeatManager =
                createJobManagerHeartbeatManager(heartbeatServices, resourceId);
        this.resourceManagerHeartbeatManager =
                createResourceManagerHeartbeatManager(heartbeatServices, resourceId);

        ExecutorThreadFactory sampleThreadFactory =
                new ExecutorThreadFactory.Builder()
                        .setPoolName("flink-thread-info-sampler")
                        .build();
        ScheduledExecutorService sampleExecutor =
                Executors.newSingleThreadScheduledExecutor(sampleThreadFactory);
        this.threadInfoSampleService = new ThreadInfoSampleService(sampleExecutor);
        this.profilingService =
                ProfilingService.getInstance(taskManagerConfiguration.getConfiguration());

        this.slotAllocationSnapshotPersistenceService =
                taskExecutorServices.getSlotAllocationSnapshotPersistenceService();

        this.sharedResources = taskExecutorServices.getSharedResources();
        this.jobInformationCache = taskExecutorServices.getJobInformationCache();
        this.taskInformationCache = taskExecutorServices.getTaskInformationCache();
        this.shuffleDescriptorsCache = taskExecutorServices.getShuffleDescriptorCache();
    }

    private HeartbeatManager<Void, TaskExecutorHeartbeatPayload>
            createResourceManagerHeartbeatManager(
                    HeartbeatServices heartbeatServices, ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private HeartbeatManager<AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload>
            createJobManagerHeartbeatManager(
                    HeartbeatServices heartbeatServices, ResourceID resourceId) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new JobManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private boolean shouldRetainPartitionsOnJobManagerConnectionLost() {
        return taskManagerConfiguration
                        .getConfiguration()
                        .get(BatchExecutionOptions.JOB_RECOVERY_ENABLED)
                && taskManagerConfiguration
                        .getConfiguration()
                        .get(SHUFFLE_SERVICE_FACTORY_CLASS)
                        .equals(NETTY_SHUFFLE_SERVICE_FACTORY_CLASS);
    }

    @Override
    public CompletableFuture<Boolean> canBeReleased() {
        return CompletableFuture.completedFuture(
                shuffleEnvironment.getPartitionsOccupyingLocalResources().isEmpty());
    }

    @Override
    public CompletableFuture<Collection<LogInfo>> requestLogList(Duration timeout) {
        return CompletableFuture.supplyAsync(
                () -> {
                    final String logDir = taskManagerConfiguration.getTaskManagerLogDir();
                    if (logDir != null) {
                        final File[] logFiles = new File(logDir).listFiles();

                        if (logFiles == null) {
                            throw new CompletionException(
                                    new FlinkException(
                                            String.format(
                                                    "There isn't a log file in TaskExecutor’s log dir %s.",
                                                    logDir)));
                        }

                        return Arrays.stream(logFiles)
                                .filter(File::isFile)
                                .map(
                                        logFile ->
                                                new LogInfo(
                                                        logFile.getName(),
                                                        logFile.length(),
                                                        logFile.lastModified()))
                                .collect(Collectors.toList());
                    }
                    return Collections.emptyList();
                },
                ioExecutor);
    }

    // ------------------------------------------------------------------------
    //  Life cycle
    // ------------------------------------------------------------------------

    @Override
    public void onStart() throws Exception {
        try {
            startTaskExecutorServices();
        } catch (Throwable t) {
            final TaskManagerException exception =
                    new TaskManagerException(
                            String.format("Could not start the TaskExecutor %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }

        startRegistrationTimeout();
    }

    private void startTaskExecutorServices() throws Exception {
        try {
            // start by connecting to the ResourceManager
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

            // tell the task slot table who's responsible for the task slot actions
            taskSlotTable.start(new SlotActionsImpl(), getMainThreadExecutor());

            // start the job leader service
            jobLeaderService.start(
                    getAddress(), getRpcService(), haServices, new JobLeaderListenerImpl());

            fileCache =
                    new FileCache(
                            taskManagerConfiguration.getTmpDirectories(),
                            taskExecutorBlobService.getPermanentBlobService());

            tryLoadLocalAllocationSnapshots();
        } catch (Exception e) {
            handleStartTaskExecutorServicesException(e);
        }
    }

    private void handleStartTaskExecutorServicesException(Exception e) throws Exception {
        try {
            stopTaskExecutorServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    /** Called to shut down the TaskManager. The method closes all TaskManager services. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping TaskExecutor {}.", getAddress());

        Throwable jobManagerDisconnectThrowable = null;

        FlinkExpectedException cause =
                new FlinkExpectedException("The TaskExecutor is shutting down.");

        closeResourceManagerConnection(cause);

        for (JobTable.Job job : jobTable.getJobs()) {
            try {
                closeJob(job, cause);
            } catch (Throwable t) {
                jobManagerDisconnectThrowable =
                        ExceptionUtils.firstOrSuppressed(t, jobManagerDisconnectThrowable);
            }
        }

        changelogStoragesManager.shutdown();
        channelStateExecutorFactoryManager.shutdown();

        jobInformationCache.clear();
        taskInformationCache.clear();
        shuffleDescriptorsCache.clear();

        Preconditions.checkState(jobTable.isEmpty());

        final Throwable throwableBeforeTasksCompletion = jobManagerDisconnectThrowable;

        return FutureUtils.runAfterwards(taskSlotTable.closeAsync(), this::stopTaskExecutorServices)
                .handle(
                        (ignored, throwable) -> {
                            handleOnStopException(throwableBeforeTasksCompletion, throwable);
                            return null;
                        });
    }

    private void handleOnStopException(
            Throwable throwableBeforeTasksCompletion, Throwable throwableAfterTasksCompletion) {
        final Throwable throwable;

        if (throwableBeforeTasksCompletion != null) {
            throwable =
                    ExceptionUtils.firstOrSuppressed(
                            throwableBeforeTasksCompletion, throwableAfterTasksCompletion);
        } else {
            throwable = throwableAfterTasksCompletion;
        }

        if (throwable != null) {
            throw new CompletionException(
                    new FlinkException("Error while shutting the TaskExecutor down.", throwable));
        } else {
            log.info("Stopped TaskExecutor {}.", getAddress());
        }
    }

    private void stopTaskExecutorServices() throws Exception {
        Exception exception = null;

        try {
            threadInfoSampleService.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            taskExecutorServices.shutDown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            fileCache.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        // it will call close() recursively from the parent to children
        taskManagerMetricGroup.close();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ======================================================================
    //  RPC methods
    // ======================================================================

    @Override
    public CompletableFuture<TaskThreadInfoResponse> requestThreadInfoSamples(
            final Collection<ExecutionAttemptID> taskExecutionAttemptIds,
            final ThreadInfoSamplesRequest requestParams,
            final Duration timeout) {

        final Collection<Task> tasks = new ArrayList<>();
        for (ExecutionAttemptID executionAttemptId : taskExecutionAttemptIds) {
            final Task task = taskSlotTable.getTask(executionAttemptId);
            if (task == null) {
                log.warn(
                        String.format(
                                "Cannot sample task %s. "
                                        + "Task is not known to the task manager.",
                                executionAttemptId));
            } else {
                tasks.add(task);
            }
        }

        Map<Long, ExecutionAttemptID> sampleableTasks =
                tasks.stream()
                        .collect(
                                Collectors.toMap(
                                        task -> task.getExecutingThread().getId(),
                                        Task::getExecutionId));

        final CompletableFuture<Map<ExecutionAttemptID, Collection<ThreadInfoSample>>>
                stackTracesFuture =
                        threadInfoSampleService.requestThreadInfoSamples(
                                sampleableTasks, requestParams);

        return stackTracesFuture.thenApply(TaskThreadInfoResponse::new);
    }

    // ----------------------------------------------------------------------
    // Task lifecycle RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> submitTask(
            TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Duration timeout) {

        final JobID jobId = tdd.getJobId();
        // todo: consider adding task info
        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {

            final ExecutionAttemptID executionAttemptID = tdd.getExecutionAttemptId();

            final JobTable.Connection jobManagerConnection =
                    jobTable.getConnection(jobId)
                            .orElseThrow(
                                    () -> {
                                        final String message =
                                                "Could not submit task because there is no JobManager "
                                                        + "associated for the job "
                                                        + jobId
                                                        + '.';

                                        log.debug(message);
                                        return new TaskSubmissionException(message);
                                    });

            if (!Objects.equals(jobManagerConnection.getJobMasterId(), jobMasterId)) {
                final String message =
                        "Rejecting the task submission because the job manager leader id "
                                + jobMasterId
                                + " does not match the expected job manager leader id "
                                + jobManagerConnection.getJobMasterId()
                                + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            if (!taskSlotTable.tryMarkSlotActive(jobId, tdd.getAllocationId())) {
                final String message =
                        "No task slot allocated for job ID "
                                + jobId
                                + " and allocation ID "
                                + tdd.getAllocationId()
                                + '.';
                log.debug(message);
                throw new TaskSubmissionException(message);
            }

            // re-integrate offloaded data and deserialize shuffle descriptors
            try {
                tdd.loadBigData(
                        taskExecutorBlobService.getPermanentBlobService(),
                        jobInformationCache,
                        taskInformationCache,
                        shuffleDescriptorsCache);
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not re-integrate offloaded TaskDeploymentDescriptor data.", e);
            }

            // deserialize the pre-serialized information
            final JobInformation jobInformation;
            final TaskInformation taskInformation;
            try {
                jobInformation = tdd.getJobInformation();
                taskInformation = tdd.getTaskInformation();
            } catch (IOException | ClassNotFoundException e) {
                throw new TaskSubmissionException(
                        "Could not deserialize the job or task information.", e);
            }

            if (!jobId.equals(jobInformation.getJobId())) {
                throw new TaskSubmissionException(
                        "Inconsistent job ID information inside TaskDeploymentDescriptor ("
                                + tdd.getJobId()
                                + " vs. "
                                + jobInformation.getJobId()
                                + ")");
            }

            TaskManagerJobMetricGroup jobGroup =
                    taskManagerMetricGroup.addJob(
                            jobInformation.getJobId(), jobInformation.getJobName());

            // note that a pre-existing job group can NOT be closed concurrently - this is done by
            // the same TM thread in removeJobMetricsGroup
            TaskMetricGroup taskMetricGroup =
                    jobGroup.addTask(tdd.getExecutionAttemptId(), taskInformation.getTaskName());

            InputSplitProvider inputSplitProvider =
                    new RpcInputSplitProvider(
                            jobManagerConnection.getJobManagerGateway(),
                            taskInformation.getJobVertexId(),
                            tdd.getExecutionAttemptId(),
                            taskManagerConfiguration.getRpcTimeout());

            final TaskOperatorEventGateway taskOperatorEventGateway =
                    new RpcTaskOperatorEventGateway(
                            jobManagerConnection.getJobManagerGateway(),
                            executionAttemptID,
                            (t) ->
                                    runAsync(
                                            () ->
                                                    failTask(
                                                            jobInformation.getJobId(),
                                                            executionAttemptID,
                                                            t)));

            TaskManagerActions taskManagerActions = jobManagerConnection.getTaskManagerActions();
            CheckpointResponder checkpointResponder = jobManagerConnection.getCheckpointResponder();
            GlobalAggregateManager aggregateManager =
                    jobManagerConnection.getGlobalAggregateManager();

            LibraryCacheManager.ClassLoaderHandle classLoaderHandle =
                    jobManagerConnection.getClassLoaderHandle();
            PartitionProducerStateChecker partitionStateChecker =
                    jobManagerConnection.getPartitionStateChecker();

            final TaskLocalStateStore localStateStore =
                    localStateStoresManager.localStateStoreForSubtask(
                            jobId,
                            tdd.getAllocationId(),
                            taskInformation.getJobVertexId(),
                            tdd.getSubtaskIndex(),
                            taskManagerConfiguration.getConfiguration(),
                            jobInformation.getJobConfiguration());

            final FileMergingSnapshotManager fileMergingSnapshotManager =
                    fileMergingManager.fileMergingSnapshotManagerForTask(
                            jobId,
                            getResourceID(),
                            tdd.getExecutionAttemptId(),
                            taskManagerConfiguration.getConfiguration(),
                            jobInformation.getJobConfiguration(),
                            jobGroup);

            final FileMergingSnapshotManagerClosableWrapper fileMergingSnapshotManagerClosable =
                    fileMergingSnapshotManager == null
                            ? null
                            : FileMergingSnapshotManagerClosableWrapper.of(
                                    fileMergingSnapshotManager,
                                    () ->
                                            fileMergingManager.releaseMergingSnapshotManagerForTask(
                                                    jobId, tdd.getExecutionAttemptId()));

            // TODO: Pass config value from user program and do overriding here.
            final StateChangelogStorage<?> changelogStorage;
            try {
                changelogStorage =
                        changelogStoragesManager.stateChangelogStorageForJob(
                                jobId,
                                taskManagerConfiguration.getConfiguration(),
                                jobGroup,
                                localStateStore.getLocalRecoveryConfig());
            } catch (IOException e) {
                throw new TaskSubmissionException(e);
            }

            final JobManagerTaskRestore taskRestore = tdd.getTaskRestore();

            final TaskStateManager taskStateManager =
                    new TaskStateManagerImpl(
                            jobId,
                            tdd.getExecutionAttemptId(),
                            localStateStore,
                            fileMergingSnapshotManagerClosable,
                            changelogStorage,
                            changelogStoragesManager,
                            taskRestore,
                            checkpointResponder);

            MemoryManager memoryManager;
            try {
                memoryManager = taskSlotTable.getTaskMemoryManager(tdd.getAllocationId());
            } catch (SlotNotFoundException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            Task task =
                    new Task(
                            jobInformation,
                            taskInformation,
                            tdd.getExecutionAttemptId(),
                            tdd.getAllocationId(),
                            tdd.getProducedPartitions(),
                            tdd.getInputGates(),
                            memoryManager,
                            sharedResources,
                            taskExecutorServices.getIOManager(),
                            taskExecutorServices.getShuffleEnvironment(),
                            taskExecutorServices.getKvStateService(),
                            taskExecutorServices.getBroadcastVariableManager(),
                            taskExecutorServices.getTaskEventDispatcher(),
                            externalResourceInfoProvider,
                            taskStateManager,
                            taskManagerActions,
                            inputSplitProvider,
                            checkpointResponder,
                            taskOperatorEventGateway,
                            aggregateManager,
                            classLoaderHandle,
                            fileCache,
                            taskManagerConfiguration,
                            taskMetricGroup,
                            partitionStateChecker,
                            MdcUtils.scopeToJob(jobId, getRpcService().getScheduledExecutor()),
                            channelStateExecutorFactoryManager.getOrCreateExecutorFactory(jobId));

            taskMetricGroup.gauge(MetricNames.IS_BACK_PRESSURED, task::isBackPressured);

            log.info(
                    "Received task {} ({}), deploy into slot with allocation id {}.",
                    task.getTaskInfo().getTaskNameWithSubtasks(),
                    tdd.getExecutionAttemptId(),
                    tdd.getAllocationId());

            boolean taskAdded;

            try {
                taskAdded = taskSlotTable.addTask(task);
            } catch (SlotNotFoundException | SlotNotActiveException e) {
                throw new TaskSubmissionException("Could not submit task.", e);
            }

            if (taskAdded) {
                task.startTaskThread();

                setupResultPartitionBookkeeping(
                        tdd.getJobId(), tdd.getProducedPartitions(), task.getTerminationFuture());
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                final String message =
                        "TaskManager already contains a task for id " + task.getExecutionId() + '.';

                log.debug(message);
                throw new TaskSubmissionException(message);
            }
        } catch (TaskSubmissionException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    private void setupResultPartitionBookkeeping(
            JobID jobId,
            Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions,
            CompletableFuture<ExecutionState> terminationFuture) {
        final Set<ResultPartitionID> partitionsRequiringRelease =
                filterPartitionsRequiringRelease(producedResultPartitions)
                        .peek(
                                rpdd ->
                                        partitionTracker.startTrackingPartition(
                                                jobId, TaskExecutorPartitionInfo.from(rpdd)))
                        .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                        .map(ShuffleDescriptor::getResultPartitionID)
                        .collect(Collectors.toSet());

        final CompletableFuture<ExecutionState> taskTerminationWithResourceCleanupFuture =
                terminationFuture.thenApplyAsync(
                        executionState -> {
                            if (executionState != ExecutionState.FINISHED) {
                                partitionTracker.stopTrackingPartitions(partitionsRequiringRelease);
                            }
                            return executionState;
                        },
                        getMainThreadExecutor());

        taskResultPartitionCleanupFuturesPerJob.compute(
                jobId,
                (ignored, completableFutures) -> {
                    if (completableFutures == null) {
                        completableFutures = new ArrayList<>(4);
                    }

                    completableFutures.add(taskTerminationWithResourceCleanupFuture);
                    return completableFutures;
                });
    }

    private Stream<ResultPartitionDeploymentDescriptor> filterPartitionsRequiringRelease(
            Collection<ResultPartitionDeploymentDescriptor> producedResultPartitions) {
        return producedResultPartitions.stream()
                // only releaseByScheduler partitions require explicit release call
                .filter(d -> d.getPartitionType().isReleaseByScheduler())
                // partitions without local resources don't store anything on the TaskExecutor
                .filter(d -> d.getShuffleDescriptor().storesLocalResourcesOn().isPresent());
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Duration timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try (MdcCloseable ignored =
                    MdcUtils.withContext(MdcUtils.asContextData(task.getJobID()))) {
                try {
                    task.cancelExecution();
                    return CompletableFuture.completedFuture(Acknowledge.get());
                } catch (Throwable t) {
                    return FutureUtils.completedExceptionally(
                            new TaskException(
                                    "Cannot cancel task for execution " + executionAttemptID + '.',
                                    t));
                }
            }
        } else {
            final String message =
                    "Cannot find task to stop for execution " + executionAttemptID + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskException(message));
        }
    }

    // ----------------------------------------------------------------------
    // Partition lifecycle RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            final ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Duration timeout) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            for (final PartitionInfo partitionInfo : partitionInfos) {
                // Run asynchronously because it might be blocking
                FutureUtils.assertNoException(
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        if (!shuffleEnvironment.updatePartitionInfo(
                                                executionAttemptID, partitionInfo)) {
                                            log.debug(
                                                    "Discard update for input gate partition {} of result {} in task {}. "
                                                            + "The partition is no longer available.",
                                                    partitionInfo
                                                            .getShuffleDescriptor()
                                                            .getResultPartitionID(),
                                                    partitionInfo.getIntermediateDataSetID(),
                                                    executionAttemptID);
                                        }
                                    } catch (IOException | InterruptedException e) {
                                        log.error(
                                                "Could not update input data location for task {}. Trying to fail task.",
                                                task.getTaskInfo().getTaskName(),
                                                e);
                                        task.failExternally(e);
                                    }
                                },
                                getRpcService().getScheduledExecutor()));
            }
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            log.debug(
                    "Discard update for input partitions of task {}. Task is no longer running.",
                    executionAttemptID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    @Override
    public void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds) {
        try {
            partitionTracker.stopTrackingAndReleaseJobPartitions(partitionIds);
            closeJobManagerConnectionIfNoAllocatedResources(jobId);
        } catch (Throwable t) {
            onFatalError(t);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> promotePartitions(
            JobID jobId, Set<ResultPartitionID> partitionIds) {
        CompletableFuture<Acknowledge> future = new CompletableFuture<>();
        try {
            partitionTracker.promoteJobPartitions(partitionIds);
            if (establishedResourceManagerConnection != null) {
                establishedResourceManagerConnection
                        .getResourceManagerGateway()
                        .reportClusterPartitions(
                                getResourceID(), partitionTracker.createClusterPartitionReport())
                        .thenAccept(ignore -> future.complete(Acknowledge.get()));
            } else {
                future.completeExceptionally(
                        new RuntimeException(
                                "Task executor is not connecting to ResourceManager. "
                                        + "Fail to report cluster partition to ResourceManager"));
            }
            closeJobManagerConnectionIfNoAllocatedResources(jobId);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            onFatalError(t);
        }

        return future;
    }

    @Override
    public CompletableFuture<Acknowledge> releaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease, Duration timeout) {
        partitionTracker.stopTrackingAndReleaseClusterPartitions(dataSetsToRelease);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------
    // Heartbeat RPC
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> heartbeatFromJobManager(
            ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
        return jobManagerHeartbeatManager.requestHeartbeat(resourceID, allocatedSlotReport);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(ResourceID resourceID) {
        return resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    // ----------------------------------------------------------------------
    // Checkpointing RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointOptions checkpointOptions) {
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if (task != null) {
            try (MdcCloseable ignored =
                    MdcUtils.withContext(MdcUtils.asContextData(task.getJobID()))) {
                log.debug(
                        "Trigger checkpoint {}@{} for {}.",
                        checkpointId,
                        checkpointTimestamp,
                        executionAttemptID);
                task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions);

                return CompletableFuture.completedFuture(Acknowledge.get());
            }
        } else {
            final String message =
                    "TaskManager received a checkpoint request for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message, CheckpointFailureReason.TASK_CHECKPOINT_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> confirmCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long completedCheckpointId,
            long completedCheckpointTimestamp,
            long lastSubsumedCheckpointId) {
        final Task task = taskSlotTable.getTask(executionAttemptID);
        if (task != null) {
            try (MdcCloseable ignored =
                    MdcUtils.withContext(MdcUtils.asContextData(task.getJobID()))) {
                log.debug(
                        "Confirm completed checkpoint {}@{} and last subsumed checkpoint {} for {}.",
                        completedCheckpointId,
                        completedCheckpointTimestamp,
                        lastSubsumedCheckpointId,
                        executionAttemptID);
                task.notifyCheckpointComplete(completedCheckpointId);

                task.notifyCheckpointSubsumed(lastSubsumedCheckpointId);
                return CompletableFuture.completedFuture(Acknowledge.get());
            }
        } else {
            final String message =
                    "TaskManager received a checkpoint confirmation for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message,
                            CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> abortCheckpoint(
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            long latestCompletedCheckpointId,
            long checkpointTimestamp) {
        final Task task = taskSlotTable.getTask(executionAttemptID);

        if (task != null) {
            try (MdcCloseable ignored =
                    MdcUtils.withContext(MdcUtils.asContextData(task.getJobID()))) {
                log.debug(
                        "Abort checkpoint {}@{} for {}.",
                        checkpointId,
                        checkpointTimestamp,
                        executionAttemptID);
                task.notifyCheckpointAborted(checkpointId, latestCompletedCheckpointId);

                return CompletableFuture.completedFuture(Acknowledge.get());
            }
        } else {
            final String message =
                    "TaskManager received an aborted checkpoint for unknown task "
                            + executionAttemptID
                            + '.';

            log.debug(message);
            return FutureUtils.completedExceptionally(
                    new CheckpointException(
                            message,
                            CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE));
        }
    }

    // ----------------------------------------------------------------------
    // Slot allocation RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> requestSlot(
            final SlotID slotId,
            final JobID jobId,
            final AllocationID allocationId,
            final ResourceProfile resourceProfile,
            final String targetAddress,
            final ResourceManagerId resourceManagerId,
            final Duration timeout) {
        // TODO: Filter invalid requests from the resource manager by using the
        // instance/registration Id

        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            log.info(
                    "Receive slot request {} for job {} from resource manager with leader id {}.",
                    allocationId,
                    jobId,
                    resourceManagerId);

            if (!isConnectedToResourceManager(resourceManagerId)) {
                final String message =
                        String.format(
                                "TaskManager is not connected to the resource manager %s.",
                                resourceManagerId);
                log.debug(message);
                return FutureUtils.completedExceptionally(new TaskManagerException(message));
            }

            tryPersistAllocationSnapshot(
                    new SlotAllocationSnapshot(
                            slotId, jobId, targetAddress, allocationId, resourceProfile));

            try {
                final boolean isConnected =
                        allocateSlotForJob(
                                jobId, slotId, allocationId, resourceProfile, targetAddress);

                if (isConnected) {
                    offerSlotsToJobManager(jobId);
                }

                return CompletableFuture.completedFuture(Acknowledge.get());
            } catch (SlotAllocationException e) {
                log.debug("Could not allocate slot for allocation id {}.", allocationId, e);
                return FutureUtils.completedExceptionally(e);
            }
        }
    }

    private boolean allocateSlotForJob(
            JobID jobId,
            SlotID slotId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            String targetAddress)
            throws SlotAllocationException {
        allocateSlot(slotId, jobId, allocationId, resourceProfile);

        final JobTable.Job job;

        try {
            job =
                    jobTable.getOrCreateJob(
                            jobId, () -> registerNewJobAndCreateServices(jobId, targetAddress));
        } catch (Exception e) {
            // free the allocated slot
            try {
                taskSlotTable.freeSlot(allocationId);
            } catch (SlotNotFoundException slotNotFoundException) {
                // slot no longer existent, this should actually never happen, because we've
                // just allocated the slot. So let's fail hard in this case!
                onFatalError(slotNotFoundException);
            }

            // release local state under the allocation id.
            localStateStoresManager.releaseLocalStateForAllocationId(allocationId);

            // sanity check
            if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
                onFatalError(new Exception("Could not free slot " + slotId));
            }

            throw new SlotAllocationException("Could not create new job.", e);
        }

        return job.isConnected();
    }

    private TaskExecutorJobServices registerNewJobAndCreateServices(
            JobID jobId, String targetAddress) throws Exception {
        jobLeaderService.addJob(jobId, targetAddress);
        final JobPermanentBlobService permanentBlobService =
                taskExecutorBlobService.getPermanentBlobService();
        permanentBlobService.registerJob(jobId);

        return TaskExecutorJobServices.create(
                libraryCacheManager.registerClassLoaderLease(jobId),
                () -> permanentBlobService.releaseJob(jobId));
    }

    private void allocateSlot(
            SlotID slotId, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile)
            throws SlotAllocationException {
        if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
            if (!taskSlotTable.allocateSlot(
                    slotId.getSlotNumber(),
                    jobId,
                    allocationId,
                    resourceProfile,
                    taskManagerConfiguration.getSlotTimeout())) {
                log.info("Could not allocate slot for {}.", allocationId);
                throw new SlotAllocationException("Could not allocate slot.");
            }
        } else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
            final String message =
                    "The slot " + slotId + " has already been allocated for a different job.";

            log.info(message);

            final AllocationID allocationID =
                    taskSlotTable.getCurrentAllocation(slotId.getSlotNumber());
            throw new SlotOccupiedException(
                    message, allocationID, taskSlotTable.getOwningJob(allocationID));
        }
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId, Throwable cause, Duration timeout) {
        freeSlotInternal(allocationId, cause);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void freeInactiveSlots(JobID jobId, Duration timeout) {
        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            log.debug("Freeing inactive slots for job {}.", jobId);

            // need a copy to prevent ConcurrentModificationExceptions
            final ImmutableList<TaskSlot<Task>> inactiveSlots =
                    ImmutableList.copyOf(taskSlotTable.getAllocatedSlots(jobId));
            for (TaskSlot<Task> slot : inactiveSlots) {
                freeSlotInternal(
                        slot.getAllocationId(),
                        new FlinkException("Slot was re-claimed by resource manager."));
            }
        }
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByType(
            FileType fileType, Duration timeout) {
        final String filePath;
        switch (fileType) {
            case LOG:
                filePath = taskManagerConfiguration.getTaskManagerLogPath();
                break;
            case STDOUT:
                filePath = taskManagerConfiguration.getTaskManagerStdoutPath();
                break;
            default:
                filePath = null;
        }
        return requestFileUploadByFilePath(filePath, fileType.toString());
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByName(
            String fileName, Duration timeout) {
        return requestFileUploadByNameAndType(fileName, FileType.LOG, timeout);
    }

    @Override
    public CompletableFuture<TransientBlobKey> requestFileUploadByNameAndType(
            String fileName, FileType fileType, Duration timeout) {
        final String filePath;
        final String baseDir;
        switch (fileType) {
            case LOG:
                baseDir = taskManagerConfiguration.getTaskManagerLogDir();
                break;
            case PROFILER:
                baseDir = profilingService.getProfilingResultDir();
                break;
            default:
                baseDir = null;
        }
        if (StringUtils.isNullOrWhitespaceOnly(baseDir)
                || StringUtils.isNullOrWhitespaceOnly(fileName)) {
            filePath = null;
        } else {
            filePath = new File(baseDir, new File(fileName).getName()).getPath();
        }
        return requestFileUploadByFilePath(filePath, fileName);
    }

    @Override
    public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(
            Duration timeout) {
        return CompletableFuture.completedFuture(
                SerializableOptional.ofNullable(metricQueryServiceAddress));
    }

    // ----------------------------------------------------------------------
    // Disconnection RPCs
    // ----------------------------------------------------------------------

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {
        try (MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
            jobTable.getConnection(jobId)
                    .ifPresent(
                            jobManagerConnection ->
                                    disconnectAndTryReconnectToJobManager(
                                            jobManagerConnection, cause, true));
        }
    }

    private void disconnectAndTryReconnectToJobManager(
            JobTable.Connection jobManagerConnection, Exception cause, boolean releasePartitions) {
        try (MdcCloseable ignored =
                MdcUtils.withContext(MdcUtils.asContextData(jobManagerConnection.getJobId()))) {
            disconnectJobManagerConnection(jobManagerConnection, cause, releasePartitions);
            jobLeaderService.reconnect(jobManagerConnection.getJobId());
        }
    }

    @Override
    public void disconnectResourceManager(Exception cause) {
        if (isRunning()) {
            reconnectToResourceManager(cause);
        }
    }

    // ----------------------------------------------------------------------
    // Other RPCs
    // ----------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID executionAttemptID,
            OperatorID operatorId,
            SerializedValue<OperatorEvent> evt) {

        log.debug("Operator event for {} - {}", executionAttemptID, operatorId);

        final Task task = taskSlotTable.getTask(executionAttemptID);
        if (task == null) {
            return FutureUtils.completedExceptionally(
                    new TaskNotRunningException(
                            "Task " + executionAttemptID + " not running on TaskManager"));
        }

        try {
            task.deliverOperatorEvent(operatorId, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalError(t);
            return FutureUtils.completedExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(Duration timeout) {
        int stacktraceMaxDepth =
                taskManagerConfiguration
                        .getConfiguration()
                        .get(ClusterOptions.THREAD_DUMP_STACKTRACE_MAX_DEPTH);
        return CompletableFuture.completedFuture(ThreadDumpInfo.dumpAndCreate(stacktraceMaxDepth));
    }

    @Override
    public CompletableFuture<Acknowledge> updateDelegationTokens(
            ResourceManagerId resourceManagerId, byte[] tokens) {
        log.info(
                "Receive update delegation tokens from resource manager with leader id {}.",
                resourceManagerId);

        if (!isConnectedToResourceManager(resourceManagerId)) {
            final String message =
                    String.format(
                            "TaskManager is not connected to the resource manager %s.",
                            resourceManagerId);
            log.debug(message);
            return FutureUtils.completedExceptionally(new TaskManagerException(message));
        }

        try {
            delegationTokenReceiverRepository.onNewTokensObtained(tokens);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Throwable t) {
            log.error("Could not update delegation tokens.", t);
            ExceptionUtils.rethrowIfFatalError(t);
            return FutureUtils.completedExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<Collection<PartitionWithMetrics>> getAndRetainPartitionWithMetrics(
            JobID jobId) {
        jobPartitionToCleanupSet.remove(jobId);

        Collection<TaskExecutorPartitionInfo> partitionInfoList =
                partitionTracker.getTrackedPartitionsFor(jobId);
        List<PartitionWithMetrics> partitionWithMetrics = new ArrayList<>();
        partitionInfoList.forEach(
                info -> {
                    ResultPartitionID partitionId = info.getResultPartitionId();
                    shuffleEnvironment
                            .getMetricsIfPartitionOccupyingLocalResource(partitionId)
                            .ifPresent(
                                    metrics ->
                                            partitionWithMetrics.add(
                                                    new DefaultPartitionWithMetrics(
                                                            info.getShuffleDescriptor(), metrics)));
                });

        return CompletableFuture.completedFuture(partitionWithMetrics);
    }

    @Override
    public CompletableFuture<ProfilingInfo> requestProfiling(
            int duration, ProfilingInfo.ProfilingMode mode, Duration timeout) {
        return profilingService.requestProfiling(
                getResourceID().getResourceIdString(), duration, mode);
    }

    @Override
    public CompletableFuture<Collection<ProfilingInfo>> requestProfilingList(Duration timeout) {
        return profilingService.getProfilingList(getResourceID().getResourceIdString());
    }

    // ------------------------------------------------------------------------
    //  Internal resource manager connection methods
    // ------------------------------------------------------------------------

    private void notifyOfNewResourceManagerLeader(
            String newLeaderAddress, ResourceManagerId newResourceManagerId) {
        resourceManagerAddress =
                createResourceManagerAddress(newLeaderAddress, newResourceManagerId);
        reconnectToResourceManager(
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newLeaderAddress, @Nullable ResourceManagerId newResourceManagerId) {
        if (newLeaderAddress == null) {
            return null;
        } else {
            assert (newResourceManagerId != null);
            return new ResourceManagerAddress(newLeaderAddress, newResourceManagerId);
        }
    }

    private void reconnectToResourceManager(Exception cause) {
        closeResourceManagerConnection(cause);
        startRegistrationTimeout();
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {
            connectToResourceManager();
        }
    }

    private void connectToResourceManager() {
        assert (resourceManagerAddress != null);
        assert (establishedResourceManagerConnection == null);
        assert (resourceManagerConnection == null);

        log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

        final TaskExecutorRegistration taskExecutorRegistration =
                new TaskExecutorRegistration(
                        getAddress(),
                        getResourceID(),
                        unresolvedTaskManagerLocation.getDataPort(),
                        JMXService.getPort().orElse(-1),
                        hardwareDescription,
                        memoryConfiguration,
                        taskManagerConfiguration.getDefaultSlotResourceProfile(),
                        taskManagerConfiguration.getTotalResourceProfile(),
                        unresolvedTaskManagerLocation.getNodeId());

        resourceManagerConnection =
                new TaskExecutorToResourceManagerConnection(
                        log,
                        getRpcService(),
                        taskManagerConfiguration.getRetryingRegistrationConfiguration(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        getMainThreadExecutor(),
                        new ResourceManagerRegistrationListener(),
                        taskExecutorRegistration);
        resourceManagerConnection.start();
    }

    private void establishResourceManagerConnection(
            ResourceManagerGateway resourceManagerGateway,
            ResourceID resourceManagerResourceId,
            InstanceID taskExecutorRegistrationId,
            ClusterInformation clusterInformation) {

        final CompletableFuture<Acknowledge> slotReportResponseFuture =
                resourceManagerGateway.sendSlotReport(
                        getResourceID(),
                        taskExecutorRegistrationId,
                        taskSlotTable.createSlotReport(getResourceID()),
                        taskManagerConfiguration.getRpcTimeout());

        slotReportResponseFuture.whenCompleteAsync(
                (acknowledge, throwable) -> {
                    if (throwable != null) {
                        reconnectToResourceManager(
                                new TaskManagerException(
                                        "Failed to send initial slot report to ResourceManager.",
                                        throwable));
                    }
                },
                getMainThreadExecutor());

        // monitor the resource manager as heartbeat target
        resourceManagerHeartbeatManager.monitorTarget(
                resourceManagerResourceId,
                new ResourceManagerHeartbeatReceiver(resourceManagerGateway));

        // set the propagated blob server address
        final InetSocketAddress blobServerAddress =
                new InetSocketAddress(
                        clusterInformation.getBlobServerHostname(),
                        clusterInformation.getBlobServerPort());

        taskExecutorBlobService.setBlobServerAddress(blobServerAddress);

        establishedResourceManagerConnection =
                new EstablishedResourceManagerConnection(
                        resourceManagerGateway,
                        resourceManagerResourceId,
                        taskExecutorRegistrationId);

        stopRegistrationTimeout();
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            final ResourceID resourceManagerResourceId =
                    establishedResourceManagerConnection.getResourceManagerResourceId();

            log.info(
                    "Close ResourceManager connection {}.",
                    resourceManagerResourceId,
                    ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
            ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);
            resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceId);

            ResourceManagerGateway resourceManagerGateway =
                    establishedResourceManagerConnection.getResourceManagerGateway();
            resourceManagerGateway.disconnectTaskManager(getResourceID(), cause);

            establishedResourceManagerConnection = null;

            partitionTracker.stopTrackingAndReleaseAllClusterPartitions();
        }

        if (resourceManagerConnection != null) {
            if (!resourceManagerConnection.isConnected()) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress(),
                            cause);
                } else {
                    log.info(
                            "Terminating registration attempts towards ResourceManager {}.",
                            resourceManagerConnection.getTargetAddress());
                }
            }

            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void startRegistrationTimeout() {
        final Duration maxRegistrationDuration =
                taskManagerConfiguration.getMaxRegistrationDuration();

        if (maxRegistrationDuration != null) {
            final UUID newRegistrationTimeoutId = UUID.randomUUID();
            currentRegistrationTimeoutId = newRegistrationTimeoutId;
            scheduleRunAsync(
                    () -> registrationTimeout(newRegistrationTimeoutId), maxRegistrationDuration);
        }
    }

    private void stopRegistrationTimeout() {
        currentRegistrationTimeoutId = null;
    }

    private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {
        if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
            final Duration maxRegistrationDuration =
                    taskManagerConfiguration.getMaxRegistrationDuration();

            onFatalError(
                    new RegistrationTimeoutException(
                            String.format(
                                    "Could not register at the ResourceManager within the specified maximum "
                                            + "registration duration %s. This indicates a problem with this instance. Terminating now.",
                                    maxRegistrationDuration)));
        }
    }

    // ------------------------------------------------------------------------
    //  Internal job manager connection methods
    // ------------------------------------------------------------------------

    private void offerSlotsToJobManager(final JobID jobId) {
        jobTable.getConnection(jobId).ifPresent(this::internalOfferSlotsToJobManager);
    }

    private void internalOfferSlotsToJobManager(JobTable.Connection jobManagerConnection) {
        final JobID jobId = jobManagerConnection.getJobId();

        if (taskSlotTable.hasAllocatedSlots(jobId)) {
            log.info("Offer reserved slots to the leader of job {}.", jobId);

            final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();

            final Iterator<TaskSlot<Task>> reservedSlotsIterator =
                    taskSlotTable.getAllocatedSlots(jobId);
            final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

            final Collection<SlotOffer> reservedSlots =
                    CollectionUtil.newHashSetWithExpectedSize(2);

            while (reservedSlotsIterator.hasNext()) {
                SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
                reservedSlots.add(offer);
            }

            final UUID slotOfferId = UUID.randomUUID();
            currentSlotOfferPerJob.put(jobId, slotOfferId);

            CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture =
                    jobMasterGateway.offerSlots(
                            getResourceID(),
                            reservedSlots,
                            taskManagerConfiguration.getRpcTimeout());

            acceptedSlotsFuture.whenCompleteAsync(
                    handleAcceptedSlotOffers(
                            jobId, jobMasterGateway, jobMasterId, reservedSlots, slotOfferId),
                    getMainThreadExecutor(jobId));
        } else {
            log.debug("There are no unassigned slots for the job {}.", jobId);
        }
    }

    @Nonnull
    private BiConsumer<Iterable<SlotOffer>, Throwable> handleAcceptedSlotOffers(
            JobID jobId,
            JobMasterGateway jobMasterGateway,
            JobMasterId jobMasterId,
            Collection<SlotOffer> offeredSlots,
            UUID offerId) {
        return (Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
            // check if this is the latest offer
            if (!offerId.equals(currentSlotOfferPerJob.get(jobId))) {
                // If this offer is outdated then it can be safely ignored.
                // If the response for a given slot is identical in both offers (accepted/rejected),
                // then this is naturally the case since the end-result is the same.
                // If the responses differ, then there are 2 cases to consider:
                // 1) initially rejected, later accepted
                //   This can happen when the resource requirements of a job increases between
                //   offers.
                //   In this case the first response MUST be ignored, so that
                //   the slot can be properly activated when the second response arrives.
                // 2) initially accepted, later rejected
                //   This can happen when the resource requirements of a job decrease between
                //   offers.
                //   In this case the first response MAY be ignored, because the job no longer
                //   requires the slot (and already has initiated steps to free it) and we can thus
                //   assume that any in-flight task submissions are no longer relevant for the job
                //   execution.

                log.debug(
                        "Discard slot offer response since there is a newer offer for the job {}.",
                        jobId);
                return;
            }

            if (throwable != null) {
                if (throwable instanceof TimeoutException) {
                    log.info(
                            "Slot offering to JobManager did not finish in time. Retrying the slot offering.");
                    // We ran into a timeout. Try again.
                    offerSlotsToJobManager(jobId);
                } else {
                    log.warn(
                            "Slot offering to JobManager failed. Freeing the slots "
                                    + "and returning them to the ResourceManager.",
                            throwable);

                    // We encountered an exception. Free the slots and return them to the RM.
                    for (SlotOffer reservedSlot : offeredSlots) {
                        freeSlotInternal(reservedSlot.getAllocationId(), throwable);
                    }
                }
            } else {
                // check if the response is still valid
                if (isJobManagerConnectionValid(jobId, jobMasterId)) {
                    // mark accepted slots active
                    for (SlotOffer acceptedSlot : acceptedSlots) {
                        final AllocationID allocationId = acceptedSlot.getAllocationId();
                        try {
                            if (!taskSlotTable.markSlotActive(allocationId)) {
                                // the slot is either free or releasing at the moment
                                final String message =
                                        "Could not mark slot " + allocationId + " active.";
                                log.debug(message);
                                jobMasterGateway.failSlot(
                                        getResourceID(), allocationId, new FlinkException(message));
                            }
                        } catch (SlotNotFoundException e) {
                            final String message =
                                    "Could not mark slot " + allocationId + " active.";
                            jobMasterGateway.failSlot(
                                    getResourceID(), allocationId, new FlinkException(message));
                        }

                        offeredSlots.remove(acceptedSlot);
                    }

                    final Exception e = new Exception("The slot was rejected by the JobManager.");

                    for (SlotOffer rejectedSlot : offeredSlots) {
                        freeSlotInternal(rejectedSlot.getAllocationId(), e);
                    }
                } else {
                    // discard the response since there is a new leader for the job
                    log.debug(
                            "Discard slot offer response since there is a new leader "
                                    + "for the job {}.",
                            jobId);
                }
            }
        };
    }

    private void establishJobManagerConnection(
            JobTable.Job job,
            final JobMasterGateway jobMasterGateway,
            JMTMRegistrationSuccess registrationSuccess) {

        final JobID jobId = job.getJobId();
        final Optional<JobTable.Connection> connection = job.asConnection();

        if (connection.isPresent()) {
            JobTable.Connection oldJobManagerConnection = connection.get();

            if (Objects.equals(
                    oldJobManagerConnection.getJobMasterId(), jobMasterGateway.getFencingToken())) {
                // we already are connected to the given job manager
                log.debug(
                        "Ignore JobManager gained leadership message for {} because we are already connected to it.",
                        jobMasterGateway.getFencingToken());
                return;
            } else {
                disconnectJobManagerConnection(
                        oldJobManagerConnection,
                        new Exception("Found new job leader for job id " + jobId + '.'),
                        true);
            }
        }

        log.info("Establish JobManager connection for job {}.", jobId);

        ResourceID jobManagerResourceID = registrationSuccess.getResourceID();

        final JobTable.Connection establishedConnection =
                associateWithJobManager(job, jobManagerResourceID, jobMasterGateway);

        // monitor the job manager as heartbeat target
        jobManagerHeartbeatManager.monitorTarget(
                jobManagerResourceID, new JobManagerHeartbeatReceiver(jobMasterGateway));

        internalOfferSlotsToJobManager(establishedConnection);
    }

    private void closeJob(JobTable.Job job, Exception cause) {
        job.asConnection()
                .ifPresent(
                        jobManagerConnection ->
                                disconnectJobManagerConnection(jobManagerConnection, cause, true));

        job.close();
    }

    private void disconnectJobManagerConnection(
            JobTable.Connection jobManagerConnection, Exception cause, boolean releasePartitions) {
        final JobID jobId = jobManagerConnection.getJobId();
        jobPartitionToCleanupSet.add(jobId);

        log.info(
                "Close JobManager connection for job {}.",
                jobId,
                ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
        ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);

        // 1. fail tasks running under this JobID
        Iterator<Task> tasks = taskSlotTable.getTasks(jobId);

        final FlinkException failureCause =
                new FlinkException(
                        String.format("Disconnect from JobManager responsible for %s.", jobId),
                        cause);

        while (tasks.hasNext()) {
            tasks.next().failExternally(failureCause);
        }

        // 2. Move the active slots to state allocated (possible to time out again)
        Set<AllocationID> activeSlotAllocationIDs =
                taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(jobId);

        final FlinkException freeingCause =
                new FlinkException("Slot could not be marked inactive.");

        for (AllocationID activeSlotAllocationID : activeSlotAllocationIDs) {
            try {
                if (!taskSlotTable.markSlotInactive(
                        activeSlotAllocationID, taskManagerConfiguration.getSlotTimeout())) {
                    freeSlotInternal(activeSlotAllocationID, freeingCause);
                }
            } catch (SlotNotFoundException e) {
                log.debug("Could not mark the slot {} inactive.", activeSlotAllocationID, e);
            }
        }

        if (!releasePartitions) {
            // this branch is for job recovery from master failures
            final Duration maxRegistrationDuration =
                    taskManagerConfiguration.getMaxRegistrationDuration();

            if (maxRegistrationDuration != null) {
                log.info(
                        "Waiting for {} mills for job {} to recover. If the job manager is not reconnected, "
                                + "the job's partitions will be cleaned up.",
                        maxRegistrationDuration.toMillis(),
                        jobId);
                scheduleRunAsync(
                        () -> {
                            // If the job is not recovery after wait for a period of time, we will
                            // clean up the partitions
                            Optional<JobTable.Job> job = jobTable.getJob(jobId);
                            if (!job.isPresent()
                                    || !job.get().isConnected()
                                    || jobPartitionToCleanupSet.contains(jobId)) {
                                scheduleResultPartitionCleanup(jobId);
                            }
                        },
                        maxRegistrationDuration);
            }
        } else {
            // cleanup remaining partitions once all tasks for this job have completed
            scheduleResultPartitionCleanup(jobId);
        }

        // 3. Disassociate from the JobManager
        try {
            jobManagerHeartbeatManager.unmonitorTarget(jobManagerConnection.getResourceId());
            disassociateFromJobManager(jobManagerConnection, cause);
        } catch (IOException e) {
            log.warn(
                    "Could not properly disassociate from JobManager {}.",
                    jobManagerConnection.getJobManagerGateway().getAddress(),
                    e);
        }

        jobManagerConnection.disconnect();
    }

    private JobTable.Connection associateWithJobManager(
            JobTable.Job job, ResourceID resourceID, JobMasterGateway jobMasterGateway) {
        checkNotNull(resourceID);
        checkNotNull(jobMasterGateway);

        TaskManagerActions taskManagerActions =
                new TaskManagerActionsImpl(job.getJobId(), jobMasterGateway);

        CheckpointResponder checkpointResponder = new RpcCheckpointResponder(jobMasterGateway);
        GlobalAggregateManager aggregateManager = new RpcGlobalAggregateManager(jobMasterGateway);

        PartitionProducerStateChecker partitionStateChecker =
                new RpcPartitionStateChecker(jobMasterGateway);

        registerQueryableState(job.getJobId(), jobMasterGateway);

        return job.connect(
                resourceID,
                jobMasterGateway,
                taskManagerActions,
                checkpointResponder,
                aggregateManager,
                partitionStateChecker);
    }

    private void disassociateFromJobManager(
            JobTable.Connection jobManagerConnection, Exception cause) throws IOException {
        checkNotNull(jobManagerConnection);

        final JobID jobId = jobManagerConnection.getJobId();

        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateRegistry != null) {
            kvStateRegistry.unregisterListener(jobId);
        }

        final KvStateClientProxy kvStateClientProxy = kvStateService.getKvStateClientProxy();

        if (kvStateClientProxy != null) {
            kvStateClientProxy.updateKvStateLocationOracle(jobManagerConnection.getJobId(), null);
        }

        JobMasterGateway jobManagerGateway = jobManagerConnection.getJobManagerGateway();
        jobManagerGateway.disconnectTaskManager(getResourceID(), cause);
    }

    private void handleRejectedJobManagerConnection(
            JobID jobId, String targetAddress, JMTMRegistrationRejection rejection) {
        log.info(
                "The JobManager under {} rejected the registration for job {}: {}. Releasing all job related resources.",
                targetAddress,
                jobId,
                rejection.getReason());

        releaseJobResources(
                jobId,
                new FlinkException(
                        String.format("JobManager %s has rejected the registration.", jobId)));
    }

    private void releaseJobResources(JobID jobId, Exception cause) {
        log.debug("Releasing job resources for job {}.", jobId, cause);

        if (partitionTracker.isTrackingPartitionsFor(jobId)) {
            // stop tracking job partitions
            partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
        }

        // free slots
        final Set<AllocationID> allocationIds = taskSlotTable.getAllocationIdsPerJob(jobId);

        if (!allocationIds.isEmpty()) {
            for (AllocationID allocationId : allocationIds) {
                freeSlotInternal(allocationId, cause);
            }
        }

        jobLeaderService.removeJob(jobId);
        jobTable.getJob(jobId)
                .ifPresent(
                        job -> {
                            closeJob(job, cause);
                        });
        taskManagerMetricGroup.removeJobMetricsGroup(jobId);
        changelogStoragesManager.releaseResourcesForJob(jobId);
        currentSlotOfferPerJob.remove(jobId);
        channelStateExecutorFactoryManager.releaseResourcesForJob(jobId);
        jobInformationCache.clearCacheForGroup(jobId);
        taskInformationCache.clearCacheForGroup(jobId);
        shuffleDescriptorsCache.clearCacheForGroup(jobId);
        fileMergingManager.releaseMergingSnapshotManagerForJob(jobId);
    }

    private void scheduleResultPartitionCleanup(JobID jobId) {
        final Collection<CompletableFuture<ExecutionState>> taskTerminationFutures =
                taskResultPartitionCleanupFuturesPerJob.remove(jobId);
        if (taskTerminationFutures != null) {
            FutureUtils.waitForAll(taskTerminationFutures)
                    .thenRunAsync(
                            () -> {
                                partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
                                jobPartitionToCleanupSet.remove(jobId);
                            },
                            getMainThreadExecutor());
        }
    }

    private void registerQueryableState(JobID jobId, JobMasterGateway jobMasterGateway) {
        final KvStateServer kvStateServer = kvStateService.getKvStateServer();
        final KvStateRegistry kvStateRegistry = kvStateService.getKvStateRegistry();

        if (kvStateServer != null && kvStateRegistry != null) {
            kvStateRegistry.registerListener(
                    jobId,
                    new RpcKvStateRegistryListener(
                            jobMasterGateway, kvStateServer.getServerAddress()));
        }

        final KvStateClientProxy kvStateProxy = kvStateService.getKvStateClientProxy();

        if (kvStateProxy != null) {
            kvStateProxy.updateKvStateLocationOracle(jobId, jobMasterGateway);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal task methods
    // ------------------------------------------------------------------------

    private void failTask(
            final JobID jobID, final ExecutionAttemptID executionAttemptID, final Throwable cause) {
        try (MdcUtils.MdcCloseable mdcCloseable =
                MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
            final Task task = taskSlotTable.getTask(executionAttemptID);

            if (task != null) {
                try {
                    task.failExternally(cause);
                } catch (Throwable t) {
                    log.error("Could not fail task {}.", executionAttemptID, t);
                }
            } else {
                log.info(
                        "Cannot find task to fail for execution {} with exception:",
                        executionAttemptID,
                        cause);
            }
        }
    }

    private void updateTaskExecutionState(
            final JobID jobID,
            final JobMasterGateway jobMasterGateway,
            final TaskExecutionState taskExecutionState) {
        final ExecutionAttemptID executionAttemptID = taskExecutionState.getID();

        CompletableFuture<Acknowledge> futureAcknowledge =
                jobMasterGateway.updateTaskExecutionState(taskExecutionState);

        futureAcknowledge.whenCompleteAsync(
                (ack, throwable) -> {
                    if (throwable != null) {
                        failTask(jobID, executionAttemptID, throwable);
                    }
                },
                getMainThreadExecutor());
    }

    private void unregisterTaskAndNotifyFinalState(
            final JobMasterGateway jobMasterGateway, final ExecutionAttemptID executionAttemptID) {

        Task task = taskSlotTable.removeTask(executionAttemptID);
        if (task != null) {
            if (!task.getExecutionState().isTerminal()) {
                try {
                    task.failExternally(
                            new IllegalStateException("Task is being remove from TaskManager."));
                } catch (Exception e) {
                    log.error("Could not properly fail task.", e);
                }
            }

            log.info(
                    "Un-registering task and sending final execution state {} to JobManager for task {} {}.",
                    task.getExecutionState(),
                    task.getTaskInfo().getTaskNameWithSubtasks(),
                    task.getExecutionId());

            AccumulatorSnapshot accumulatorSnapshot = task.getAccumulatorRegistry().getSnapshot();

            updateTaskExecutionState(
                    task.getJobID(),
                    jobMasterGateway,
                    new TaskExecutionState(
                            task.getExecutionId(),
                            task.getExecutionState(),
                            task.getFailureCause(),
                            accumulatorSnapshot,
                            task.getMetricGroup().getIOMetricGroup().createSnapshot()));
        } else {
            log.error("Cannot find task with ID {} to unregister.", executionAttemptID);
        }
    }

    private void freeSlotInternal(AllocationID allocationId, Throwable cause) {
        checkNotNull(allocationId);

        // only respond to freeing slots when not shutting down to avoid freeing slot allocation
        // information
        if (isRunning()) {
            final JobID jobId = taskSlotTable.getOwningJob(allocationId);
            try (MdcCloseable ignored =
                    MdcUtils.withContext(
                            jobId == null
                                    ? Collections.emptyMap()
                                    : MdcUtils.asContextData(jobId))) {

                log.debug(
                        "Free slot with allocation id {} because: {}",
                        allocationId,
                        cause.getMessage());

                final int slotIndex = taskSlotTable.freeSlot(allocationId, cause);

                slotAllocationSnapshotPersistenceService.deleteAllocationSnapshot(slotIndex);

                if (slotIndex != -1) {

                    if (isConnectedToResourceManager()) {
                        // the slot was freed. Tell the RM about it
                        ResourceManagerGateway resourceManagerGateway =
                                establishedResourceManagerConnection.getResourceManagerGateway();

                        resourceManagerGateway.notifySlotAvailable(
                                establishedResourceManagerConnection
                                        .getTaskExecutorRegistrationId(),
                                new SlotID(getResourceID(), slotIndex),
                                allocationId);
                    }

                    if (jobId != null) {
                        closeJobManagerConnectionIfNoAllocatedResources(jobId);
                    }
                }
            } catch (SlotNotFoundException e) {
                log.debug("Could not free slot for allocation id {}.", allocationId, e);
            }

            localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
        } else {
            log.debug(
                    "Ignoring the freeing of slot {} because the TaskExecutor is shutting down.",
                    allocationId);
        }
    }

    private void closeJobManagerConnectionIfNoAllocatedResources(JobID jobId) {
        // check whether we still have allocated slots for the same job
        if (taskSlotTable.getAllocationIdsPerJob(jobId).isEmpty()
                && !partitionTracker.isTrackingPartitionsFor(jobId)) {
            // we can remove the job from the job leader service

            final FlinkExpectedException cause =
                    new FlinkExpectedException(
                            "TaskExecutor "
                                    + getAddress()
                                    + " has no more allocated slots for job "
                                    + jobId
                                    + '.');

            releaseJobResources(jobId, cause);
        }
    }

    private void timeoutSlot(AllocationID allocationId, UUID ticket) {
        checkNotNull(allocationId);
        checkNotNull(ticket);

        if (taskSlotTable.isValidTimeout(allocationId, ticket)) {
            freeSlotInternal(
                    allocationId, new Exception("The slot " + allocationId + " has timed out."));
        } else {
            log.debug(
                    "Received an invalid timeout for allocation id {} with ticket {}.",
                    allocationId,
                    ticket);
        }
    }

    /**
     * Syncs the TaskExecutor's view on its allocated slots with the JobMaster's view. Slots which
     * are no longer reported by the JobMaster are being freed. Slots which the JobMaster thinks it
     * still owns but which are no longer allocated to it will be failed via {@link
     * JobMasterGateway#failSlot}.
     *
     * @param jobMasterGateway jobMasterGateway to talk to the connected job master
     * @param allocatedSlotReport represents the JobMaster's view on the current slot allocation
     *     state
     */
    private void syncSlotsWithSnapshotFromJobMaster(
            JobMasterGateway jobMasterGateway, AllocatedSlotReport allocatedSlotReport) {
        failNoLongerAllocatedSlots(allocatedSlotReport, jobMasterGateway);
        freeNoLongerUsedSlots(allocatedSlotReport);
    }

    private void failNoLongerAllocatedSlots(
            AllocatedSlotReport allocatedSlotReport, JobMasterGateway jobMasterGateway) {
        for (AllocatedSlotInfo allocatedSlotInfo : allocatedSlotReport.getAllocatedSlotInfos()) {
            final AllocationID allocationId = allocatedSlotInfo.getAllocationId();
            if (!taskSlotTable.isAllocated(
                    allocatedSlotInfo.getSlotIndex(),
                    allocatedSlotReport.getJobId(),
                    allocationId)) {
                jobMasterGateway.failSlot(
                        getResourceID(),
                        allocationId,
                        new FlinkException(
                                String.format(
                                        "Slot %s on TaskExecutor %s is not allocated by job %s.",
                                        allocatedSlotInfo.getSlotIndex(),
                                        getResourceID().getStringWithMetadata(),
                                        allocatedSlotReport.getJobId())));
            }
        }
    }

    private void freeNoLongerUsedSlots(AllocatedSlotReport allocatedSlotReport) {
        final Set<AllocationID> activeSlots =
                taskSlotTable.getActiveTaskSlotAllocationIdsPerJob(allocatedSlotReport.getJobId());
        final Set<AllocationID> reportedSlots =
                allocatedSlotReport.getAllocatedSlotInfos().stream()
                        .map(AllocatedSlotInfo::getAllocationId)
                        .collect(Collectors.toSet());

        final Sets.SetView<AllocationID> difference = Sets.difference(activeSlots, reportedSlots);

        for (AllocationID allocationID : difference) {
            freeSlotInternal(
                    allocationID,
                    new FlinkException(
                            String.format(
                                    "%s is no longer allocated by job %s.",
                                    allocationID, allocatedSlotReport.getJobId())));
        }
    }

    private void tryPersistAllocationSnapshot(SlotAllocationSnapshot slotAllocationSnapshot) {
        try {
            slotAllocationSnapshotPersistenceService.persistAllocationSnapshot(
                    slotAllocationSnapshot);
        } catch (IOException e) {
            log.debug("Cannot persist the slot allocation snapshot {}.", slotAllocationSnapshot, e);
        }
    }

    /**
     * This method tries to repopulate the {@link JobTable} and {@link TaskSlotTable} from the local
     * filesystem in a best-effort manner.
     */
    private void tryLoadLocalAllocationSnapshots() {
        Collection<SlotAllocationSnapshot> slotAllocationSnapshots =
                slotAllocationSnapshotPersistenceService.loadAllocationSnapshots();

        log.debug("Recovered slot allocation snapshots {}.", slotAllocationSnapshots);

        final Set<AllocationID> allocatedSlots = new HashSet<>();
        for (SlotAllocationSnapshot slotAllocationSnapshot : slotAllocationSnapshots) {
            try {
                allocateSlotForJob(
                        slotAllocationSnapshot.getJobId(),
                        slotAllocationSnapshot.getSlotID(),
                        slotAllocationSnapshot.getAllocationId(),
                        slotAllocationSnapshot.getResourceProfile(),
                        slotAllocationSnapshot.getJobTargetAddress());

            } catch (SlotAllocationException e) {
                log.debug("Cannot reallocate restored slot {}.", slotAllocationSnapshot, e);
            }

            allocatedSlots.add(slotAllocationSnapshot.getAllocationId());
        }

        localStateStoresManager.retainLocalStateForAllocations(allocatedSlots);
    }

    // ------------------------------------------------------------------------
    //  Internal utility methods
    // ------------------------------------------------------------------------

    private boolean isConnectedToResourceManager() {
        return establishedResourceManagerConnection != null;
    }

    private boolean isConnectedToResourceManager(ResourceManagerId resourceManagerId) {
        return establishedResourceManagerConnection != null
                && resourceManagerAddress != null
                && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
    }

    private boolean isJobManagerConnectionValid(JobID jobId, JobMasterId jobMasterId) {
        return jobTable.getConnection(jobId)
                .map(jmConnection -> Objects.equals(jmConnection.getJobMasterId(), jobMasterId))
                .orElse(false);
    }

    private CompletableFuture<TransientBlobKey> requestFileUploadByFilePath(
            String filePath, String fileTag) {
        log.debug("Received file upload request for file {}", fileTag);
        if (!StringUtils.isNullOrWhitespaceOnly(filePath)) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        final File file = new File(filePath);
                        if (file.exists()) {
                            try {
                                return putTransientBlobStream(new FileInputStream(file), fileTag)
                                        .get();
                            } catch (Exception e) {
                                log.debug("Could not upload file {}.", fileTag, e);
                                throw new CompletionException(
                                        new FlinkException(
                                                "Could not upload file " + fileTag + '.', e));
                            }
                        } else {
                            log.debug(
                                    "The file {} does not exist on the TaskExecutor {}.",
                                    fileTag,
                                    getResourceID().getStringWithMetadata());
                            throw new CompletionException(
                                    new FileNotFoundException(
                                            "The file "
                                                    + fileTag
                                                    + " does not exist on the TaskExecutor."));
                        }
                    },
                    ioExecutor);
        } else {
            log.debug(
                    "The file {} is unavailable on the TaskExecutor {}.",
                    fileTag,
                    getResourceID().getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new FlinkException(
                            "The file " + fileTag + " is not available on the TaskExecutor."));
        }
    }

    private CompletableFuture<TransientBlobKey> putTransientBlobStream(
            InputStream inputStream, String fileTag) {
        final TransientBlobService transientBlobService =
                taskExecutorBlobService.getTransientBlobService();
        final TransientBlobKey transientBlobKey;

        try {
            transientBlobKey = transientBlobService.putTransient(inputStream);
        } catch (IOException e) {
            log.debug("Could not upload file {}.", fileTag, e);
            return FutureUtils.completedExceptionally(
                    new FlinkException("Could not upload file " + fileTag + '.', e));
        }
        return CompletableFuture.completedFuture(transientBlobKey);
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public ResourceID getResourceID() {
        return unresolvedTaskManagerLocation.getResourceID();
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    void onFatalError(final Throwable t) {
        try {
            log.error("Fatal error occurred in TaskExecutor {}.", getAddress(), t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        fatalErrorHandler.onFatalError(t);
    }

    // ------------------------------------------------------------------------
    //  Access to fields for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    TaskExecutorToResourceManagerConnection getResourceManagerConnection() {
        return resourceManagerConnection;
    }

    @VisibleForTesting
    HeartbeatManager<Void, TaskExecutorHeartbeatPayload> getResourceManagerHeartbeatManager() {
        return resourceManagerHeartbeatManager;
    }

    // ------------------------------------------------------------------------
    //  Utility classes
    // ------------------------------------------------------------------------

    private static final class JobManagerHeartbeatReceiver
            extends HeartbeatReceiver<TaskExecutorToJobManagerHeartbeatPayload> {
        private final JobMasterGateway jobMasterGateway;

        private JobManagerHeartbeatReceiver(JobMasterGateway jobMasterGateway) {
            this.jobMasterGateway = jobMasterGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(
                ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
            return jobMasterGateway.heartbeatFromTaskManager(resourceID, payload);
        }
    }

    private static final class ResourceManagerHeartbeatReceiver
            extends HeartbeatReceiver<TaskExecutorHeartbeatPayload> {
        private final ResourceManagerGateway resourceManagerGateway;

        private ResourceManagerHeartbeatReceiver(ResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(
                ResourceID resourceID, TaskExecutorHeartbeatPayload heartbeatPayload) {
            return resourceManagerGateway.heartbeatFromTaskManager(resourceID, heartbeatPayload);
        }
    }

    /** The listener for leader changes of the resource manager. */
    private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            runAsync(
                    () ->
                            notifyOfNewResourceManagerLeader(
                                    leaderAddress,
                                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(Exception exception) {
            onFatalError(exception);
        }
    }

    private final class JobLeaderListenerImpl implements JobLeaderListener {

        @Override
        public void jobManagerGainedLeadership(
                final JobID jobId,
                final JobMasterGateway jobManagerGateway,
                final JMTMRegistrationSuccess registrationMessage) {
            runAsync(
                    () ->
                            jobTable.getJob(jobId)
                                    .ifPresent(
                                            job ->
                                                    establishJobManagerConnection(
                                                            job,
                                                            jobManagerGateway,
                                                            registrationMessage)));
        }

        @Override
        public void jobManagerLostLeadership(final JobID jobId, final JobMasterId jobMasterId) {
            log.info(
                    "JobManager for job {} with leader id {} lost leadership.", jobId, jobMasterId);

            runAsync(
                    () ->
                            jobTable.getConnection(jobId)
                                    .ifPresent(
                                            jobManagerConnection ->
                                                    disconnectJobManagerConnection(
                                                            jobManagerConnection,
                                                            new Exception(
                                                                    "Job leader for job id "
                                                                            + jobId
                                                                            + " lost leadership."),
                                                            !shouldRetainPartitionsOnJobManagerConnectionLost())));
        }

        @Override
        public void handleError(Throwable throwable) {
            onFatalError(throwable);
        }

        @Override
        public void jobManagerRejectedRegistration(
                JobID jobId, String targetAddress, JMTMRegistrationRejection rejection) {
            runAsync(() -> handleRejectedJobManagerConnection(jobId, targetAddress, rejection));
        }
    }

    private final class ResourceManagerRegistrationListener
            implements RegistrationConnectionListener<
                    TaskExecutorToResourceManagerConnection,
                    TaskExecutorRegistrationSuccess,
                    TaskExecutorRegistrationRejection> {

        @Override
        public void onRegistrationSuccess(
                TaskExecutorToResourceManagerConnection connection,
                TaskExecutorRegistrationSuccess success) {
            final ResourceID resourceManagerId = success.getResourceManagerId();
            final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
            final ClusterInformation clusterInformation = success.getClusterInformation();
            final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();

            byte[] tokens = success.getInitialTokens();
            if (tokens != null) {
                try {
                    log.info("Receive initial delegation tokens from resource manager");
                    delegationTokenReceiverRepository.onNewTokensObtained(tokens);
                } catch (Throwable t) {
                    log.error("Could not update delegation tokens.", t);
                    ExceptionUtils.rethrowIfFatalError(t);
                }
            }

            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (resourceManagerConnection == connection) {
                            try {
                                establishResourceManagerConnection(
                                        resourceManagerGateway,
                                        resourceManagerId,
                                        taskExecutorRegistrationId,
                                        clusterInformation);
                            } catch (Throwable t) {
                                log.error(
                                        "Establishing Resource Manager connection in Task Executor failed",
                                        t);
                            }
                        }
                    });
        }

        @Override
        public void onRegistrationFailure(Throwable failure) {
            onFatalError(failure);
        }

        @Override
        public void onRegistrationRejection(
                String targetAddress, TaskExecutorRegistrationRejection rejection) {
            onFatalError(
                    new FlinkException(
                            String.format(
                                    "The TaskExecutor's registration at the ResourceManager %s has been rejected: %s",
                                    targetAddress, rejection)));
        }
    }

    private final class TaskManagerActionsImpl implements TaskManagerActions {
        private final JobID jobId;
        private final JobMasterGateway jobMasterGateway;

        private TaskManagerActionsImpl(JobID jobId, JobMasterGateway jobMasterGateway) {
            this.jobId = jobId;
            this.jobMasterGateway = checkNotNull(jobMasterGateway);
        }

        @Override
        public void notifyFatalError(String message, Throwable cause) {
            try {
                log.error(message, cause);
            } catch (Throwable ignored) {
            }

            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        }

        @Override
        public void failTask(final ExecutionAttemptID executionAttemptID, final Throwable cause) {
            runAsync(() -> TaskExecutor.this.failTask(jobId, executionAttemptID, cause));
        }

        @Override
        public void updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
            if (taskExecutionState.getExecutionState().isTerminal()) {
                runAsync(
                        () ->
                                unregisterTaskAndNotifyFinalState(
                                        jobMasterGateway, taskExecutionState.getID()));
            } else {
                TaskExecutor.this.updateTaskExecutionState(
                        jobId, jobMasterGateway, taskExecutionState);
            }
        }

        @Override
        public void notifyEndOfData(final ExecutionAttemptID executionAttemptID) {
            runAsync(() -> jobMasterGateway.notifyEndOfData(executionAttemptID));
        }
    }

    private class SlotActionsImpl implements SlotActions {

        @Override
        public void freeSlot(final AllocationID allocationId) {
            runAsync(
                    () ->
                            freeSlotInternal(
                                    allocationId,
                                    new FlinkException(
                                            "TaskSlotTable requested freeing the TaskSlot "
                                                    + allocationId
                                                    + '.')));
        }

        @Override
        public void timeoutSlot(final AllocationID allocationId, final UUID ticket) {
            runAsync(() -> TaskExecutor.this.timeoutSlot(allocationId, ticket));
        }
    }

    private class JobManagerHeartbeatListener
            implements HeartbeatListener<
                    AllocatedSlotReport, TaskExecutorToJobManagerHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            final String message =
                    String.format(
                            "The heartbeat of JobManager with id %s timed out.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleJobManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "JobManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleJobManagerConnectionLoss(resourceID, new TaskManagerException(message));
        }

        private void handleJobManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            jobTable.getConnection(resourceID)
                    .ifPresent(
                            jobManagerConnection ->
                                    disconnectAndTryReconnectToJobManager(
                                            jobManagerConnection,
                                            cause,
                                            !shouldRetainPartitionsOnJobManagerConnectionLost()));
        }

        @Override
        public void reportPayload(ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            validateRunsInMainThread();
            OptionalConsumer.of(jobTable.getConnection(allocatedSlotReport.getJobId()))
                    .ifPresent(
                            jobManagerConnection -> {
                                syncSlotsWithSnapshotFromJobMaster(
                                        jobManagerConnection.getJobManagerGateway(),
                                        allocatedSlotReport);
                            })
                    .ifNotPresent(
                            () ->
                                    log.debug(
                                            "Ignoring allocated slot report from job {} because there is no active leader.",
                                            allocatedSlotReport.getJobId()));
        }

        @Override
        public TaskExecutorToJobManagerHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return jobTable.getConnection(resourceID)
                    .map(
                            jobManagerConnection -> {
                                JobID jobId = jobManagerConnection.getJobId();

                                Set<ExecutionAttemptID> deployedExecutions = new HashSet<>();
                                List<AccumulatorSnapshot> accumulatorSnapshots =
                                        new ArrayList<>(16);
                                Iterator<Task> allTasks = taskSlotTable.getTasks(jobId);

                                while (allTasks.hasNext()) {
                                    Task task = allTasks.next();
                                    deployedExecutions.add(task.getExecutionId());
                                    accumulatorSnapshots.add(
                                            task.getAccumulatorRegistry().getSnapshot());
                                }
                                return new TaskExecutorToJobManagerHeartbeatPayload(
                                        new AccumulatorReport(accumulatorSnapshots),
                                        new ExecutionDeploymentReport(deployedExecutions));
                            })
                    .orElseGet(TaskExecutorToJobManagerHeartbeatPayload::empty);
        }
    }

    private class ResourceManagerHeartbeatListener
            implements HeartbeatListener<Void, TaskExecutorHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            final String message =
                    String.format(
                            "The heartbeat of ResourceManager with id %s timed out.",
                            resourceId.getStringWithMetadata());

            log.info(message);
            handleResourceManagerConnectionLoss(resourceId, new TaskManagerException(message));
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "ResourceManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceID, new TaskManagerException(message));
        }

        private void handleResourceManagerConnectionLoss(
                ResourceID resourceId, TaskManagerException cause) {
            validateRunsInMainThread();

            // first check whether the timeout is still valid
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceId()
                            .equals(resourceId)) {

                reconnectToResourceManager(cause);
            }
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public TaskExecutorHeartbeatPayload retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return new TaskExecutorHeartbeatPayload(
                    taskSlotTable.createSlotReport(getResourceID()),
                    partitionTracker.createClusterPartitionReport());
        }
    }

    @VisibleForTesting
    static final class TaskExecutorJobServices implements JobTable.JobServices {

        private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

        private final Runnable closeHook;

        private TaskExecutorJobServices(
                LibraryCacheManager.ClassLoaderLease classLoaderLease, Runnable closeHook) {
            this.classLoaderLease = classLoaderLease;
            this.closeHook = closeHook;
        }

        @Override
        public LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle() {
            return classLoaderLease;
        }

        @Override
        public void close() {
            classLoaderLease.release();
            closeHook.run();
        }

        @VisibleForTesting
        static TaskExecutorJobServices create(
                LibraryCacheManager.ClassLoaderLease classLoaderLease, Runnable closeHook) {
            return new TaskExecutorJobServices(classLoaderLease, closeHook);
        }
    }
}
