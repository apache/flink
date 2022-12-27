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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.ThresholdMeter;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceAllocator;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceDeclaration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.FlinkExpectedException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An active implementation of {@link ResourceManager}.
 *
 * <p>This resource manager actively requests and releases resources from/to the external resource
 * management frameworks. With different {@link ResourceManagerDriver} provided, this resource
 * manager can work with various frameworks.
 */
public class ActiveResourceManager<WorkerType extends ResourceIDRetrievable>
        extends ResourceManager<WorkerType> implements ResourceEventHandler<WorkerType> {

    protected final Configuration flinkConfig;

    private final Duration startWorkerRetryInterval;

    private final ResourceManagerDriver<WorkerType> resourceManagerDriver;

    /** All workers maintained by {@link ActiveResourceManager}. */
    private final Map<ResourceID, WorkerType> workerNodeMap;

    /** Number of requested and not registered workers per worker resource spec. */
    private final WorkerCounter pendingWorkerCounter;

    /** Number of requested or registered recovered workers per worker resource spec. */
    private final WorkerCounter totalWorkerCounter;

    /** Identifiers and worker resource spec of all allocated workers. */
    private final Map<ResourceID, WorkerResourceSpec> workerResourceSpecs;

    private final Map<CompletableFuture<WorkerType>, WorkerResourceSpec> unallocatedWorkerFutures;

    /** Identifiers of requested not registered workers. */
    private final Set<ResourceID> currentAttemptUnregisteredWorkers;

    /** Identifiers of recovered and not registered workers. */
    private final Set<ResourceID> previousAttemptUnregisteredWorkers;

    private final ThresholdMeter startWorkerFailureRater;

    private final Duration workerRegistrationTimeout;

    /**
     * Incompletion of this future indicates that the max failure rate of start worker is reached
     * and the resource manager should not retry starting new worker until the future become
     * completed again. It's guaranteed to be modified in main thread.
     */
    private CompletableFuture<Void> startWorkerCoolDown;

    /** The future indicates whether the rm is ready to serve. */
    private final CompletableFuture<Void> readyToServeFuture;

    /** Timeout to wait for all the previous attempts workers to recover. */
    private final Duration previousWorkerRecoverTimeout;

    /** ResourceDeclaration of {@link SlotManager}. */
    private Collection<ResourceDeclaration> resourceDeclarations;

    public ActiveResourceManager(
            ResourceManagerDriver<WorkerType> resourceManagerDriver,
            Configuration flinkConfig,
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ThresholdMeter startWorkerFailureRater,
            Duration retryInterval,
            Duration workerRegistrationTimeout,
            Duration previousWorkerRecoverTimeout,
            Executor ioExecutor) {
        super(
                rpcService,
                leaderSessionId,
                resourceId,
                heartbeatServices,
                delegationTokenManager,
                slotManager,
                clusterPartitionTrackerFactory,
                blocklistHandlerFactory,
                jobLeaderIdService,
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                Time.fromDuration(
                        Preconditions.checkNotNull(flinkConfig)
                                .get(AkkaOptions.ASK_TIMEOUT_DURATION)),
                ioExecutor);

        this.flinkConfig = flinkConfig;
        this.resourceManagerDriver = resourceManagerDriver;
        this.workerNodeMap = new HashMap<>();
        this.pendingWorkerCounter = new WorkerCounter();
        this.totalWorkerCounter = new WorkerCounter();
        this.workerResourceSpecs = new HashMap<>();
        this.unallocatedWorkerFutures = new HashMap<>();
        this.currentAttemptUnregisteredWorkers = new HashSet<>();
        this.previousAttemptUnregisteredWorkers = new HashSet<>();
        this.startWorkerFailureRater = checkNotNull(startWorkerFailureRater);
        this.startWorkerRetryInterval = retryInterval;
        this.workerRegistrationTimeout = workerRegistrationTimeout;
        this.startWorkerCoolDown = FutureUtils.completedVoidFuture();
        this.previousWorkerRecoverTimeout = previousWorkerRecoverTimeout;
        this.readyToServeFuture = new CompletableFuture<>();
        this.resourceDeclarations = new HashSet<>();
    }

    // ------------------------------------------------------------------------
    //  ResourceManager
    // ------------------------------------------------------------------------

    @Override
    protected void initialize() throws ResourceManagerException {
        try {
            resourceManagerDriver.initialize(
                    this,
                    new GatewayMainThreadExecutor(),
                    ioExecutor,
                    blocklistHandler::getAllBlockedNodeIds);
        } catch (Exception e) {
            throw new ResourceManagerException("Cannot initialize resource provider.", e);
        }
    }

    @Override
    protected void terminate() throws ResourceManagerException {
        try {
            resourceManagerDriver.terminate();
        } catch (Exception e) {
            throw new ResourceManagerException("Cannot terminate resource provider.", e);
        }
    }

    @Override
    protected void internalDeregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics)
            throws ResourceManagerException {
        try {
            resourceManagerDriver.deregisterApplication(finalStatus, optionalDiagnostics);
        } catch (Exception e) {
            throw new ResourceManagerException("Cannot deregister application.", e);
        }
    }

    @Override
    protected Optional<WorkerType> getWorkerNodeIfAcceptRegistration(ResourceID resourceID) {
        return Optional.ofNullable(workerNodeMap.get(resourceID));
    }

    @VisibleForTesting
    public void declareResourceNeeded(Collection<ResourceDeclaration> resourceDeclarations) {
        this.resourceDeclarations = Collections.unmodifiableCollection(resourceDeclarations);
        log.debug("Update resource declarations to {}.", resourceDeclarations);

        checkResourceDeclarations();
    }

    @Override
    protected void onWorkerRegistered(WorkerType worker, WorkerResourceSpec workerResourceSpec) {
        final ResourceID resourceId = worker.getResourceID();
        log.info("Worker {} is registered.", resourceId.getStringWithMetadata());

        tryRemovePreviousPendingRecoveryTaskManager(resourceId);

        if (!workerResourceSpecs.containsKey(worker.getResourceID())) {
            workerResourceSpecs.put(worker.getResourceID(), workerResourceSpec);
            totalWorkerCounter.increaseAndGet(workerResourceSpec);
            log.info(
                    "Recovered worker {} with resource spec {} registered",
                    resourceId.getStringWithMetadata(),
                    workerResourceSpec);
        }

        if (currentAttemptUnregisteredWorkers.remove(resourceId)) {
            final int count = pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
            log.info(
                    "Worker {} with resource spec {} was requested in current attempt."
                            + " Current pending count after registering: {}.",
                    resourceId.getStringWithMetadata(),
                    workerResourceSpec,
                    count);
        }
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();
        resourceManagerMetricGroup.meter(
                MetricNames.START_WORKER_FAILURE_RATE, startWorkerFailureRater);
        resourceManagerMetricGroup.gauge(
                MetricNames.NUM_PENDING_TASK_MANAGERS, pendingWorkerCounter::getTotalNum);
    }

    // ------------------------------------------------------------------------
    //  ResourceEventListener
    // ------------------------------------------------------------------------

    @Override
    public void onPreviousAttemptWorkersRecovered(Collection<WorkerType> recoveredWorkers) {
        getMainThreadExecutor().assertRunningInMainThread();
        log.info("Recovered {} workers from previous attempt.", recoveredWorkers.size());
        for (WorkerType worker : recoveredWorkers) {
            final ResourceID resourceId = worker.getResourceID();
            workerNodeMap.put(resourceId, worker);
            previousAttemptUnregisteredWorkers.add(resourceId);
            scheduleWorkerRegistrationTimeoutCheck(resourceId);
            log.info(
                    "Worker {} recovered from previous attempt.",
                    resourceId.getStringWithMetadata());
        }
        if (recoveredWorkers.size() > 0 && !previousWorkerRecoverTimeout.isZero()) {
            scheduleRunAsync(
                    () -> {
                        readyToServeFuture.complete(null);
                        log.info(
                                "Timeout to wait recovery taskmanagers, recovery future is completed.");
                    },
                    previousWorkerRecoverTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
        } else {
            readyToServeFuture.complete(null);
        }
    }

    @Override
    public void onWorkerTerminated(ResourceID resourceId, String diagnostics) {
        if (currentAttemptUnregisteredWorkers.contains(resourceId)) {
            recordWorkerFailureAndPauseWorkerCreationIfNeeded();
        }

        if (clearStateForWorker(resourceId)) {
            log.info(
                    "Worker {} is terminated. Diagnostics: {}",
                    resourceId.getStringWithMetadata(),
                    diagnostics);
            checkResourceDeclarations();
        }
        closeTaskManagerConnection(resourceId, new Exception(diagnostics));
    }

    @Override
    public void onError(Throwable exception) {
        onFatalError(exception);
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    private void checkResourceDeclarations() {
        validateRunsInMainThread();

        for (ResourceDeclaration resourceDeclaration : resourceDeclarations) {
            WorkerResourceSpec workerResourceSpec = resourceDeclaration.getSpec();
            int declaredWorkerNumber = resourceDeclaration.getNumNeeded();

            final int releaseOrRequestWorkerNumber =
                    totalWorkerCounter.getNum(workerResourceSpec) - declaredWorkerNumber;

            if (releaseOrRequestWorkerNumber > 0) {
                log.info(
                        "need release {} workers, current worker number {}, declared worker number {}",
                        releaseOrRequestWorkerNumber,
                        totalWorkerCounter.getNum(workerResourceSpec),
                        declaredWorkerNumber);

                // release unwanted workers.
                int remainingReleasingWorkerNumber =
                        releaseUnWantedResources(
                                resourceDeclaration.getUnwantedWorkers(),
                                releaseOrRequestWorkerNumber);

                if (remainingReleasingWorkerNumber > 0) {
                    // release not allocated workers
                    remainingReleasingWorkerNumber =
                            releaseUnallocatedWorkers(
                                    workerResourceSpec, remainingReleasingWorkerNumber);
                }

                if (remainingReleasingWorkerNumber > 0) {
                    // release starting workers
                    remainingReleasingWorkerNumber =
                            releaseAllocatedWorkers(
                                    currentAttemptUnregisteredWorkers,
                                    workerResourceSpec,
                                    remainingReleasingWorkerNumber);
                }

                if (remainingReleasingWorkerNumber > 0) {
                    // release registered workers
                    remainingReleasingWorkerNumber =
                            releaseAllocatedWorkers(
                                    workerNodeMap.keySet(),
                                    workerResourceSpec,
                                    remainingReleasingWorkerNumber);
                }

                checkState(
                        remainingReleasingWorkerNumber == 0,
                        "there are no more workers to release");
            } else if (releaseOrRequestWorkerNumber < 0) {
                // In case of start worker failures, we should wait for an interval before
                // trying to start new workers.
                // Otherwise, ActiveResourceManager will always re-requesting the worker,
                // which keeps the main thread busy.
                if (startWorkerCoolDown.isDone()) {
                    int requestWorkerNumber = -releaseOrRequestWorkerNumber;
                    log.info(
                            "need request {} new workers, current worker number {}, declared worker number {}",
                            requestWorkerNumber,
                            totalWorkerCounter.getNum(workerResourceSpec),
                            declaredWorkerNumber);
                    for (int i = 0; i < requestWorkerNumber; i++) {
                        requestNewWorker(workerResourceSpec);
                    }
                } else {
                    startWorkerCoolDown.thenRun(this::checkResourceDeclarations);
                }
            } else {
                log.debug(
                        "current worker number {} meets the declared worker {}",
                        totalWorkerCounter.getNum(workerResourceSpec),
                        declaredWorkerNumber);
            }
        }
    }

    private int releaseUnWantedResources(
            Collection<InstanceID> unwantedWorkers, int needReleaseWorkerNumber) {

        Exception cause =
                new FlinkExpectedException(
                        "slot manager has determined that the resource is no longer needed");
        for (InstanceID unwantedWorker : unwantedWorkers) {
            if (needReleaseWorkerNumber <= 0) {
                break;
            }
            if (releaseResource(unwantedWorker, cause)) {
                needReleaseWorkerNumber--;
            }
        }
        return needReleaseWorkerNumber;
    }

    private int releaseUnallocatedWorkers(
            WorkerResourceSpec workerResourceSpec, int needReleaseWorkerNumber) {
        Set<CompletableFuture<WorkerType>> unallocatedWorkerFuturesShouldRelease =
                unallocatedWorkerFutures.entrySet().stream()
                        .filter(e -> e.getValue().equals(workerResourceSpec))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
        for (CompletableFuture<WorkerType> requestFuture : unallocatedWorkerFuturesShouldRelease) {
            if (needReleaseWorkerNumber <= 0) {
                break;
            }
            if (requestFuture.cancel(true)) {
                needReleaseWorkerNumber--;
            }
        }
        return needReleaseWorkerNumber;
    }

    private int releaseAllocatedWorkers(
            Collection<ResourceID> candidateWorkers,
            WorkerResourceSpec workerResourceSpec,
            int needReleaseWorkerNumber) {
        List<ResourceID> workerCanRelease =
                candidateWorkers.stream()
                        .filter(r -> workerResourceSpec.equals(workerResourceSpecs.get(r)))
                        .collect(Collectors.toList());

        Exception cause = new FlinkExpectedException("resource is no longer needed");
        for (ResourceID resourceID : workerCanRelease) {
            if (needReleaseWorkerNumber <= 0) {
                break;
            }

            if (releaseResource(resourceID, cause)) {
                needReleaseWorkerNumber--;
            } else {
                log.warn("Resource {} could not release.", resourceID);
            }
        }

        return needReleaseWorkerNumber;
    }

    private boolean releaseResource(InstanceID instanceId, Exception cause) {
        WorkerType worker = getWorkerByInstanceId(instanceId);
        if (worker != null) {
            return releaseResource(worker.getResourceID(), cause);
        } else {
            log.debug("Instance {} not found in ResourceManager.", instanceId);
            return false;
        }
    }

    private boolean releaseResource(ResourceID resourceID, Exception cause) {
        if (workerNodeMap.containsKey(resourceID)) {
            internalStopWorker(resourceID);
            closeTaskManagerConnection(resourceID, cause);
            return true;
        }
        return false;
    }

    /**
     * Allocates a resource using the worker resource specification.
     *
     * @param workerResourceSpec workerResourceSpec specifies the size of the to be allocated
     *     resource
     */
    @VisibleForTesting
    public void requestNewWorker(WorkerResourceSpec workerResourceSpec) {
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(
                        flinkConfig, workerResourceSpec);
        final int pendingCount = pendingWorkerCounter.increaseAndGet(workerResourceSpec);
        totalWorkerCounter.increaseAndGet(workerResourceSpec);

        log.info(
                "Requesting new worker with resource spec {}, current pending count: {}.",
                workerResourceSpec,
                pendingCount);

        final CompletableFuture<WorkerType> requestResourceFuture =
                resourceManagerDriver.requestResource(taskExecutorProcessSpec);
        unallocatedWorkerFutures.put(requestResourceFuture, workerResourceSpec);

        FutureUtils.assertNoException(
                requestResourceFuture.handle(
                        (worker, exception) -> {
                            unallocatedWorkerFutures.remove(requestResourceFuture);

                            if (exception != null) {
                                final int count =
                                        pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
                                totalWorkerCounter.decreaseAndGet(workerResourceSpec);
                                if (exception instanceof CancellationException) {
                                    log.info(
                                            "Requesting worker with resource spec {} canceled, current pending count: {}",
                                            workerResourceSpec,
                                            count);
                                } else {
                                    log.warn(
                                            "Failed requesting worker with resource spec {}, current pending count: {}",
                                            workerResourceSpec,
                                            count,
                                            exception);
                                    recordWorkerFailureAndPauseWorkerCreationIfNeeded();
                                    checkResourceDeclarations();
                                }
                            } else {
                                final ResourceID resourceId = worker.getResourceID();
                                workerNodeMap.put(resourceId, worker);
                                workerResourceSpecs.put(resourceId, workerResourceSpec);
                                currentAttemptUnregisteredWorkers.add(resourceId);
                                scheduleWorkerRegistrationTimeoutCheck(resourceId);
                                log.info(
                                        "Requested worker {} with resource spec {}.",
                                        resourceId.getStringWithMetadata(),
                                        workerResourceSpec);
                            }
                            return null;
                        }));
    }

    private void scheduleWorkerRegistrationTimeoutCheck(final ResourceID resourceId) {
        scheduleRunAsync(
                () -> {
                    if (currentAttemptUnregisteredWorkers.contains(resourceId)
                            || previousAttemptUnregisteredWorkers.contains(resourceId)) {
                        log.warn(
                                "Worker {} did not register in {}, will stop it and request a new one if needed.",
                                resourceId,
                                workerRegistrationTimeout);
                        internalStopWorker(resourceId);
                        checkResourceDeclarations();
                    }
                },
                workerRegistrationTimeout);
    }

    private void internalStopWorker(final ResourceID resourceId) {
        log.info("Stopping worker {}.", resourceId.getStringWithMetadata());

        final WorkerType worker = workerNodeMap.get(resourceId);
        if (worker != null) {
            resourceManagerDriver.releaseResource(worker);
        }

        clearStateForWorker(resourceId);
    }

    /**
     * Clear states for a terminated worker.
     *
     * @param resourceId Identifier of the worker
     * @return True if the worker is known and states are cleared; false if the worker is unknown
     *     (duplicate call to already cleared worker)
     */
    private boolean clearStateForWorker(ResourceID resourceId) {
        WorkerType worker = workerNodeMap.remove(resourceId);
        if (worker == null) {
            log.debug("Ignore unrecognized worker {}.", resourceId.getStringWithMetadata());
            return false;
        }

        WorkerResourceSpec workerResourceSpec = workerResourceSpecs.remove(resourceId);
        tryRemovePreviousPendingRecoveryTaskManager(resourceId);
        if (workerResourceSpec != null) {
            totalWorkerCounter.decreaseAndGet(workerResourceSpec);
            if (currentAttemptUnregisteredWorkers.remove(resourceId)) {
                final int count = pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
                log.info(
                        "Worker {} with resource spec {} was requested in current attempt and has not registered."
                                + " Current pending count after removing: {}.",
                        resourceId.getStringWithMetadata(),
                        workerResourceSpec,
                        count);
            }
        }
        return true;
    }

    private void recordWorkerFailureAndPauseWorkerCreationIfNeeded() {
        if (recordStartWorkerFailure()) {
            // if exceed failure rate try to slow down
            tryResetWorkerCreationCoolDown();
        }
    }

    /**
     * Record failure number of starting worker in ResourceManagers. Return whether maximum failure
     * rate is reached.
     *
     * @return whether max failure rate is reached
     */
    private boolean recordStartWorkerFailure() {
        startWorkerFailureRater.markEvent();

        try {
            startWorkerFailureRater.checkAgainstThreshold();
        } catch (ThresholdMeter.ThresholdExceedException e) {
            log.warn("Reaching max start worker failure rate: {}", e.getMessage());
            return true;
        }

        return false;
    }

    private void tryResetWorkerCreationCoolDown() {
        if (startWorkerCoolDown.isDone()) {
            log.info("Will not retry creating worker in {}.", startWorkerRetryInterval);
            startWorkerCoolDown = new CompletableFuture<>();
            scheduleRunAsync(() -> startWorkerCoolDown.complete(null), startWorkerRetryInterval);
        }
    }

    @Override
    public CompletableFuture<Void> getReadyToServeFuture() {
        return readyToServeFuture;
    }

    @Override
    protected ResourceAllocator getResourceAllocator() {
        return new ResourceAllocatorImpl();
    }

    private void tryRemovePreviousPendingRecoveryTaskManager(ResourceID resourceID) {
        long sizeBeforeRemove = previousAttemptUnregisteredWorkers.size();
        if (previousAttemptUnregisteredWorkers.remove(resourceID)) {
            log.info(
                    "Pending recovery taskmanagers {} -> {}.{}",
                    sizeBeforeRemove,
                    previousAttemptUnregisteredWorkers.size(),
                    previousAttemptUnregisteredWorkers.size() == 0
                            ? " Resource manager is ready to serve."
                            : "");
        }
        if (previousAttemptUnregisteredWorkers.size() == 0) {
            readyToServeFuture.complete(null);
        }
    }

    /** Always execute on the current main thread executor. */
    private class GatewayMainThreadExecutor implements ScheduledExecutor {

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return getMainThreadExecutor().schedule(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return getMainThreadExecutor().schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            return getMainThreadExecutor().scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return getMainThreadExecutor()
                    .scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        @Override
        public void execute(Runnable command) {
            getMainThreadExecutor().execute(command);
        }
    }

    private class ResourceAllocatorImpl implements ResourceAllocator {

        @Override
        public boolean isSupported() {
            return true;
        }

        @Override
        public void cleaningUpDisconnectedResource(ResourceID resourceID) {
            validateRunsInMainThread();
            internalStopWorker(resourceID);
        }

        @Override
        public void declareResourceNeeded(Collection<ResourceDeclaration> resourceDeclarations) {
            validateRunsInMainThread();
            ActiveResourceManager.this.declareResourceNeeded(resourceDeclarations);
        }
    }

    // ------------------------------------------------------------------------
    //  Testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    <T> CompletableFuture<T> runInMainThread(Callable<T> callable, Time timeout) {
        return callAsync(callable, TimeUtils.toDuration(timeout));
    }
}
