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
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.ThresholdMeter;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

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

    private final Time startWorkerRetryInterval;

    private final ResourceManagerDriver<WorkerType> resourceManagerDriver;

    /** All workers maintained by {@link ActiveResourceManager}. */
    private final Map<ResourceID, WorkerType> workerNodeMap;

    /** Number of requested and not registered workers per worker resource spec. */
    private final PendingWorkerCounter pendingWorkerCounter;

    /** Identifiers and worker resource spec of requested not registered workers. */
    private final Map<ResourceID, WorkerResourceSpec> currentAttemptUnregisteredWorkers;

    /** Identifiers of recovered and not registered workers. */
    private final Set<ResourceID> previousAttemptUnregisteredWorkers;

    private final ThresholdMeter startWorkerFailureRater;

    private final Time workerRegistrationTimeout;

    /**
     * Incompletion of this future indicates that the max failure rate of start worker is reached
     * and the resource manager should not retry starting new worker until the future become
     * completed again. It's guaranteed to be modified in main thread.
     */
    private CompletableFuture<Void> startWorkerCoolDown;

    public ActiveResourceManager(
            ResourceManagerDriver<WorkerType> resourceManagerDriver,
            Configuration flinkConfig,
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            ThresholdMeter startWorkerFailureRater,
            Duration retryInterval,
            Duration workerRegistrationTimeout,
            Executor ioExecutor) {
        super(
                rpcService,
                leaderSessionId,
                resourceId,
                heartbeatServices,
                slotManager,
                clusterPartitionTrackerFactory,
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
        this.pendingWorkerCounter = new PendingWorkerCounter();
        this.currentAttemptUnregisteredWorkers = new HashMap<>();
        this.previousAttemptUnregisteredWorkers = new HashSet<>();
        this.startWorkerFailureRater = checkNotNull(startWorkerFailureRater);
        this.startWorkerRetryInterval = Time.of(retryInterval.toMillis(), TimeUnit.MILLISECONDS);
        this.workerRegistrationTimeout =
                Time.of(workerRegistrationTimeout.toMillis(), TimeUnit.MILLISECONDS);
        this.startWorkerCoolDown = FutureUtils.completedVoidFuture();
    }

    // ------------------------------------------------------------------------
    //  ResourceManager
    // ------------------------------------------------------------------------

    @Override
    protected void initialize() throws ResourceManagerException {
        try {
            resourceManagerDriver.initialize(this, new GatewayMainThreadExecutor(), ioExecutor);
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
    public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
        requestNewWorker(workerResourceSpec);
        return true;
    }

    @Override
    protected WorkerType workerStarted(ResourceID resourceID) {
        return workerNodeMap.get(resourceID);
    }

    @Override
    public boolean stopWorker(WorkerType worker) {
        internalStopWorker(worker.getResourceID());
        return true;
    }

    @Override
    protected void onWorkerRegistered(WorkerType worker) {
        final ResourceID resourceId = worker.getResourceID();
        log.info("Worker {} is registered.", resourceId.getStringWithMetadata());

        final WorkerResourceSpec workerResourceSpec =
                currentAttemptUnregisteredWorkers.remove(resourceId);
        previousAttemptUnregisteredWorkers.remove(resourceId);
        if (workerResourceSpec != null) {
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
    }

    @Override
    public void onWorkerTerminated(ResourceID resourceId, String diagnostics) {
        if (currentAttemptUnregisteredWorkers.containsKey(resourceId)) {
            recordWorkerFailureAndPauseWorkerCreationIfNeeded();
        }

        if (clearStateForWorker(resourceId)) {
            log.info(
                    "Worker {} is terminated. Diagnostics: {}",
                    resourceId.getStringWithMetadata(),
                    diagnostics);
            requestWorkerIfRequired();
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

    private void requestNewWorker(WorkerResourceSpec workerResourceSpec) {
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(
                        flinkConfig, workerResourceSpec);
        final int pendingCount = pendingWorkerCounter.increaseAndGet(workerResourceSpec);

        log.info(
                "Requesting new worker with resource spec {}, current pending count: {}.",
                workerResourceSpec,
                pendingCount);

        // In case of start worker failures, we should wait for an interval before
        // trying to start new workers.
        // Otherwise, ActiveResourceManager will always re-requesting the worker,
        // which keeps the main thread busy.
        final CompletableFuture<WorkerType> requestResourceFuture =
                startWorkerCoolDown.thenCompose(
                        (ignore) -> resourceManagerDriver.requestResource(taskExecutorProcessSpec));
        FutureUtils.assertNoException(
                requestResourceFuture.handle(
                        (worker, exception) -> {
                            if (exception != null) {
                                final int count =
                                        pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
                                log.warn(
                                        "Failed requesting worker with resource spec {}, current pending count: {}",
                                        workerResourceSpec,
                                        count,
                                        exception);
                                recordWorkerFailureAndPauseWorkerCreationIfNeeded();
                                requestWorkerIfRequired();
                            } else {
                                final ResourceID resourceId = worker.getResourceID();
                                workerNodeMap.put(resourceId, worker);
                                currentAttemptUnregisteredWorkers.put(
                                        resourceId, workerResourceSpec);
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
                    if (currentAttemptUnregisteredWorkers.containsKey(resourceId)
                            || previousAttemptUnregisteredWorkers.contains(resourceId)) {
                        log.warn(
                                "Worker {} did not register in {}, will stop it and request a new one if needed.",
                                resourceId,
                                workerRegistrationTimeout);
                        internalStopWorker(resourceId);
                        requestWorkerIfRequired();
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

        WorkerResourceSpec workerResourceSpec =
                currentAttemptUnregisteredWorkers.remove(resourceId);
        previousAttemptUnregisteredWorkers.remove(resourceId);
        if (workerResourceSpec != null) {
            final int count = pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
            log.info(
                    "Worker {} with resource spec {} was requested in current attempt and has not registered."
                            + " Current pending count after removing: {}.",
                    resourceId.getStringWithMetadata(),
                    workerResourceSpec,
                    count);
        }
        return true;
    }

    private void requestWorkerIfRequired() {
        for (Map.Entry<WorkerResourceSpec, Integer> entry : getRequiredResources().entrySet()) {
            final WorkerResourceSpec workerResourceSpec = entry.getKey();
            final int requiredCount = entry.getValue();

            while (requiredCount > pendingWorkerCounter.getNum(workerResourceSpec)) {
                requestNewWorker(workerResourceSpec);
            }
        }
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

    // ------------------------------------------------------------------------
    //  Testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    <T> CompletableFuture<T> runInMainThread(Callable<T> callable, Time timeout) {
        return callAsync(callable, timeout);
    }
}
