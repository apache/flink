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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.scheduler.ConnectionMonitor;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.TaskSchedulerBuilder;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;
import org.apache.flink.mesos.scheduler.messages.Disconnected;
import org.apache.flink.mesos.scheduler.messages.OfferRescinded;
import org.apache.flink.mesos.scheduler.messages.ReRegistered;
import org.apache.flink.mesos.scheduler.messages.Registered;
import org.apache.flink.mesos.scheduler.messages.ResourceOffers;
import org.apache.flink.mesos.scheduler.messages.StatusUpdate;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.UntypedAbstractActor;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.Option;
import scala.concurrent.duration.FiniteDuration;

/**
 * Implementation of {@link ResourceManagerDriver} for Mesos deployment.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class MesosResourceManagerDriver
        extends AbstractResourceManagerDriver<RegisteredMesosWorkerNode> {

    /** The Mesos configuration (master and framework info). */
    private final MesosConfiguration mesosConfig;

    /** The Mesos services needed by the resource manager. */
    private final MesosServices mesosServices;

    /** The TaskManager container parameters (like container memory size). */
    private final MesosTaskManagerParameters taskManagerParameters;

    /** Container specification for launching a TM. */
    private final ContainerSpecification taskManagerContainerSpec;

    /** Server for HTTP artifacts. */
    private final MesosArtifactServer artifactServer;

    /** Persistent storage of allocated containers. */
    private MesosWorkerStore workerStore;

    /** Factory for creating local actors. */
    private final MesosResourceManagerActorFactory actorFactory;

    /** Web url to show in mesos page. */
    @Nullable private final String webUiUrl;

    /** Mesos scheduler driver. */
    private SchedulerDriver schedulerDriver;

    /** an adapter to receive messages from Akka actors. */
    private ActorRef selfActor;

    private ActorRef connectionMonitor;

    private ActorRef taskMonitor;

    private ActorRef launchCoordinator;

    private ActorRef reconciliationCoordinator;

    /** Workers that are requested but not yet offered. */
    private final Map<ResourceID, MesosWorkerStore.Worker> workersInNew;

    private final Map<ResourceID, CompletableFuture<RegisteredMesosWorkerNode>>
            requestResourceFutures;

    private MesosConfiguration initializedMesosConfig;

    public MesosResourceManagerDriver(
            Configuration flinkConfig,
            MesosServices mesosServices,
            MesosConfiguration mesosConfig,
            MesosTaskManagerParameters taskManagerParameters,
            ContainerSpecification taskManagerContainerSpec,
            @Nullable String webUiUrl) {
        super(flinkConfig, GlobalConfiguration.loadConfiguration());

        this.mesosServices = Preconditions.checkNotNull(mesosServices);
        this.actorFactory =
                Preconditions.checkNotNull(mesosServices.createMesosResourceManagerActorFactory());

        this.mesosConfig = Preconditions.checkNotNull(mesosConfig);

        this.artifactServer = Preconditions.checkNotNull(mesosServices.getArtifactServer());

        this.taskManagerParameters = Preconditions.checkNotNull(taskManagerParameters);
        this.taskManagerContainerSpec = Preconditions.checkNotNull(taskManagerContainerSpec);
        this.webUiUrl = webUiUrl;

        this.workersInNew = new HashMap<>();
        this.requestResourceFutures = new HashMap<>();
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerDriver
    // ------------------------------------------------------------------------

    @Override
    protected void initializeInternal() throws Exception {
        // create and start the worker store
        try {
            this.workerStore = mesosServices.createMesosWorkerStore(flinkConfig);
            workerStore.start();
        } catch (Exception e) {
            throw new ResourceManagerException("Unable to initialize the worker store.", e);
        }

        // Prepare to register with Mesos
        Protos.FrameworkInfo.Builder frameworkInfo =
                mesosConfig.frameworkInfo().clone().setCheckpoint(true);
        if (webUiUrl != null) {
            frameworkInfo.setWebuiUrl(webUiUrl);
        }

        try {
            Option<Protos.FrameworkID> frameworkID = workerStore.getFrameworkID();
            if (frameworkID.isEmpty()) {
                log.info("Registering as new framework.");
            } else {
                log.info(
                        "Recovery scenario: re-registering using framework ID {}.",
                        frameworkID.get().getValue());
                frameworkInfo.setId(frameworkID.get());
            }
        } catch (Exception e) {
            throw new ResourceManagerException("Unable to recover the framework ID.", e);
        }

        initializedMesosConfig = mesosConfig.withFrameworkInfo(frameworkInfo);
        MesosConfiguration.logMesosConfig(log, initializedMesosConfig);

        this.selfActor = actorFactory.createSelfActorForMesosResourceManagerDriver(this);

        // configure the artifact server to serve the TM container artifacts
        try {
            LaunchableMesosWorker.configureArtifactServer(artifactServer, taskManagerContainerSpec);
        } catch (IOException e) {
            throw new ResourceManagerException(
                    "Unable to configure the artifact server with TaskManager artifacts.", e);
        }
    }

    @Override
    public CompletableFuture<Void> terminate() {
        return stopSupportingActorsAsync();
    }

    @Override
    public CompletableFuture<Void> onGrantLeadership() {
        Preconditions.checkState(initializedMesosConfig != null);

        schedulerDriver =
                mesosServices.createMesosSchedulerDriver(
                        initializedMesosConfig, new MesosResourceManagerSchedulerCallback(), false);

        // create supporting actors
        connectionMonitor = actorFactory.createConnectionMonitor(flinkConfig);
        launchCoordinator =
                actorFactory.createLaunchCoordinator(
                        flinkConfig, selfActor, schedulerDriver, createOptimizer());
        reconciliationCoordinator =
                actorFactory.createReconciliationCoordinator(flinkConfig, schedulerDriver);
        taskMonitor = actorFactory.createTaskMonitor(flinkConfig, selfActor, schedulerDriver);

        return getWorkersAsync()
                .thenApplyAsync(
                        (tasksFromPreviousAttempts) -> {
                            // recover state
                            recoverWorkers(tasksFromPreviousAttempts);

                            // begin scheduling
                            connectionMonitor.tell(new ConnectionMonitor.Start(), selfActor);
                            schedulerDriver.start();

                            log.info("Mesos resource manager started.");
                            return null;
                        },
                        getMainThreadExecutor());
    }

    @Override
    public CompletableFuture<Void> onRevokeLeadership() {
        schedulerDriver.stop(true);

        workersInNew.clear();
        requestResourceFutures.clear();

        return stopSupportingActorsAsync();
    }

    @Override
    public void deregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) throws Exception {
        log.info("Shutting down and unregistering as a Mesos framework.");

        Exception exception = null;

        try {
            // unregister the framework, which implicitly removes all tasks.
            schedulerDriver.stop(false);
        } catch (Exception ex) {
            exception = new Exception("Could not unregister the Mesos framework.", ex);
        }

        try {
            workerStore.stop(true);
        } catch (Exception ex) {
            exception =
                    ExceptionUtils.firstOrSuppressed(
                            new Exception("Could not stop the Mesos worker store.", ex), exception);
        }

        if (exception != null) {
            throw new ResourceManagerException(
                    "Could not properly shut down the Mesos application.", exception);
        }
    }

    @Override
    public CompletableFuture<RegisteredMesosWorkerNode> requestResource(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        Preconditions.checkArgument(
                Objects.equals(
                        taskExecutorProcessSpec,
                        taskManagerParameters
                                .containeredParameters()
                                .getTaskExecutorProcessSpec()));
        log.info("Starting a new worker.");

        try {
            // generate new workers into persistent state and launch associated actors
            MesosWorkerStore.Worker worker =
                    MesosWorkerStore.Worker.newWorker(workerStore.newTaskID());
            workerStore.putWorker(worker);

            final ResourceID resourceId = extractResourceID(worker.taskID());
            workersInNew.put(resourceId, worker);

            final CompletableFuture<RegisteredMesosWorkerNode> requestResourceFuture =
                    new CompletableFuture<>();
            requestResourceFutures.put(resourceId, requestResourceFuture);

            LaunchableMesosWorker launchable = createLaunchableMesosWorker(worker.taskID());

            log.info(
                    "Scheduling Mesos task {} with ({} MB, {} cpus, {} gpus, {} disk MB, {} Mbps).",
                    launchable.taskID().getValue(),
                    launchable.taskRequest().getMemory(),
                    launchable.taskRequest().getCPUs(),
                    launchable.taskRequest().getScalarRequests().get("gpus"),
                    launchable.taskRequest().getDisk(),
                    launchable.taskRequest().getNetworkMbps());

            // tell the task monitor about the new plans
            taskMonitor.tell(
                    new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)), selfActor);

            // tell the launch coordinator to launch the new tasks
            launchCoordinator.tell(
                    new LaunchCoordinator.Launch(Collections.singletonList(launchable)), selfActor);

            return requestResourceFuture;
        } catch (Exception ex) {
            final ResourceManagerException exception =
                    new ResourceManagerException("Unable to request new workers.", ex);
            getResourceEventHandler().onError(exception);
            return FutureUtils.completedExceptionally(exception);
        }
    }

    @Override
    public void releaseResource(RegisteredMesosWorkerNode workerNode) {
        try {
            // update persistent state of worker to Released
            MesosWorkerStore.Worker worker = workerNode.getWorker();
            worker = worker.releaseWorker();
            workerStore.putWorker(worker);

            taskMonitor.tell(
                    new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)), selfActor);

            if (worker.hostname().isDefined()) {
                // tell the launch coordinator that the task is being unassigned from the host, for
                // planning purposes
                launchCoordinator.tell(
                        new LaunchCoordinator.Unassign(worker.taskID(), worker.hostname().get()),
                        selfActor);
            }
        } catch (Exception e) {
            getResourceEventHandler()
                    .onError(new ResourceManagerException("Unable to release a worker.", e));
        }
    }

    // ------------------------------------------------------------------------
    //  Mesos Specific
    // ------------------------------------------------------------------------

    private void registered(Registered message) {
        connectionMonitor.tell(message, selfActor);
        try {
            workerStore.setFrameworkID(Option.apply(message.frameworkId()));
        } catch (Exception ex) {
            getResourceEventHandler()
                    .onError(
                            new ResourceManagerException(
                                    "Unable to store the assigned framework ID.", ex));
            return;
        }

        launchCoordinator.tell(message, selfActor);
        reconciliationCoordinator.tell(message, selfActor);
        taskMonitor.tell(message, selfActor);
    }

    /** Called when reconnected to Mesos following a failover event. */
    private void reregistered(ReRegistered message) {
        connectionMonitor.tell(message, selfActor);
        launchCoordinator.tell(message, selfActor);
        reconciliationCoordinator.tell(message, selfActor);
        taskMonitor.tell(message, selfActor);
    }

    /** Called when disconnected from Mesos. */
    private void disconnected(Disconnected message) {
        connectionMonitor.tell(message, selfActor);
        launchCoordinator.tell(message, selfActor);
        reconciliationCoordinator.tell(message, selfActor);
        taskMonitor.tell(message, selfActor);
    }

    /** Called when resource offers are made to the framework. */
    private void resourceOffers(ResourceOffers message) {
        launchCoordinator.tell(message, selfActor);
    }

    /** Called when resource offers are rescinded. */
    private void offerRescinded(OfferRescinded message) {
        launchCoordinator.tell(message, selfActor);
    }

    /** Handles a task status update from Mesos. */
    private void statusUpdate(StatusUpdate message) {
        taskMonitor.tell(message, selfActor);
        reconciliationCoordinator.tell(message, selfActor);
        schedulerDriver.acknowledgeStatusUpdate(message.status());
    }

    /**
     * Accept offers as advised by the launch coordinator.
     *
     * <p>Acceptance is routed through the RM to update the persistent state before forwarding the
     * message to Mesos.
     */
    private void acceptOffers(AcceptOffers msg) {
        try {
            List<TaskMonitor.TaskGoalStateUpdated> toMonitor =
                    new ArrayList<>(msg.operations().size());

            // transition the persistent state of some tasks to Launched
            for (Protos.Offer.Operation op : msg.operations()) {
                if (op.getType() == Protos.Offer.Operation.Type.LAUNCH) {
                    for (Protos.TaskInfo info : op.getLaunch().getTaskInfosList()) {

                        final ResourceID resourceId = extractResourceID(info.getTaskId());

                        MesosWorkerStore.Worker worker = workersInNew.remove(resourceId);
                        assert (worker != null);

                        worker = worker.launchWorker(info.getSlaveId(), msg.hostname());
                        workerStore.putWorker(worker);

                        final CompletableFuture<RegisteredMesosWorkerNode> requestResourceFuture =
                                requestResourceFutures.remove(resourceId);
                        assert (requestResourceFuture != null);

                        requestResourceFuture.complete(new RegisteredMesosWorkerNode(worker));

                        log.info(
                                "Launching Mesos task {} on host {}.",
                                worker.taskID().getValue(),
                                worker.hostname().get());

                        toMonitor.add(
                                new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)));
                    }
                }
            }

            // tell the task monitor about the new plans
            for (TaskMonitor.TaskGoalStateUpdated update : toMonitor) {
                taskMonitor.tell(update, selfActor);
            }

            // send the acceptance message to Mesos
            schedulerDriver.acceptOffers(msg.offerIds(), msg.operations(), msg.filters());
        } catch (Exception ex) {
            getResourceEventHandler()
                    .onError(new ResourceManagerException("unable to accept offers", ex));
        }
    }

    /** Handles a reconciliation request from a task monitor. */
    private void reconcile(ReconciliationCoordinator.Reconcile message) {
        // forward to the reconciliation coordinator
        reconciliationCoordinator.tell(message, selfActor);
    }

    /** Handles a termination notification from a task monitor. */
    private void taskTerminated(TaskMonitor.TaskTerminated message) {
        Protos.TaskID taskID = message.taskID();
        Protos.TaskStatus status = message.status();

        // note: this callback occurs for failed containers and for released containers alike
        final ResourceID id = extractResourceID(taskID);

        boolean existed;
        try {
            existed = workerStore.removeWorker(taskID);
        } catch (Exception ex) {
            getResourceEventHandler()
                    .onError(new ResourceManagerException("unable to remove worker", ex));
            return;
        }

        if (!existed) {
            log.info("Received a termination notice for an unrecognized worker: {}", id);
            return;
        }

        // check if this is a failed task or a released task
        assert (!workersInNew.containsKey(id));

        final String diagnostics = extractTerminatedDiagnostics(id, status);
        getResourceEventHandler().onWorkerTerminated(id, diagnostics);
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    /** Fetches framework/worker information persisted by a prior incarnation of the RM. */
    private CompletableFuture<List<MesosWorkerStore.Worker>> getWorkersAsync() {
        // if this resource manager is recovering from failure,
        // then some worker tasks are most likely still alive and we can re-obtain them
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        final List<MesosWorkerStore.Worker> tasksFromPreviousAttempts =
                                workerStore.recoverWorkers();
                        for (final MesosWorkerStore.Worker worker : tasksFromPreviousAttempts) {
                            if (worker.state() == MesosWorkerStore.WorkerState.New) {
                                // remove new workers because allocation requests are transient
                                workerStore.removeWorker(worker.taskID());
                            }
                        }
                        return tasksFromPreviousAttempts;
                    } catch (final Exception e) {
                        throw new CompletionException(new ResourceManagerException(e));
                    }
                },
                getIoExecutor());
    }

    /**
     * Recovers given framework/worker information.
     *
     * @see #getWorkersAsync()
     */
    private void recoverWorkers(final List<MesosWorkerStore.Worker> tasksFromPreviousAttempts) {
        assertStateCleared();

        if (!tasksFromPreviousAttempts.isEmpty()) {
            log.info(
                    "Retrieved {} TaskManagers from previous attempt",
                    tasksFromPreviousAttempts.size());

            List<MesosWorkerStore.Worker> launchedWorkers =
                    tasksFromPreviousAttempts.stream()
                            .peek(
                                    worker ->
                                            taskMonitor.tell(
                                                    new TaskMonitor.TaskGoalStateUpdated(
                                                            extractGoalState(worker)),
                                                    selfActor))
                            .filter(
                                    worker ->
                                            worker.state() == MesosWorkerStore.WorkerState.Launched)
                            .collect(Collectors.toList());

            // tell the launch coordinator about prior assignments
            List<Tuple2<TaskRequest, String>> toAssign =
                    launchedWorkers.stream()
                            .map(
                                    worker ->
                                            new Tuple2<>(
                                                    createLaunchableMesosWorker(worker.taskID())
                                                            .taskRequest(),
                                                    worker.hostname().get()))
                            .collect(Collectors.toList());
            launchCoordinator.tell(new LaunchCoordinator.Assign(toAssign), selfActor);

            // notify resource event handler about recovered workers
            getResourceEventHandler()
                    .onPreviousAttemptWorkersRecovered(
                            launchedWorkers.stream()
                                    .map(RegisteredMesosWorkerNode::new)
                                    .collect(Collectors.toList()));
        }
    }

    private CompletableFuture<Void> stopSupportingActorsAsync() {
        FiniteDuration stopTimeout = new FiniteDuration(5L, TimeUnit.SECONDS);

        CompletableFuture<Boolean> stopTaskMonitorFuture =
                actorFactory.stopActor(taskMonitor, stopTimeout);
        taskMonitor = null;

        CompletableFuture<Boolean> stopConnectionMonitorFuture =
                actorFactory.stopActor(connectionMonitor, stopTimeout);
        connectionMonitor = null;

        CompletableFuture<Boolean> stopLaunchCoordinatorFuture =
                actorFactory.stopActor(launchCoordinator, stopTimeout);
        launchCoordinator = null;

        CompletableFuture<Boolean> stopReconciliationCoordinatorFuture =
                actorFactory.stopActor(reconciliationCoordinator, stopTimeout);
        reconciliationCoordinator = null;

        return CompletableFuture.allOf(
                stopTaskMonitorFuture,
                stopConnectionMonitorFuture,
                stopLaunchCoordinatorFuture,
                stopReconciliationCoordinatorFuture);
    }

    /** Creates a launchable task for Fenzo to process. */
    private LaunchableMesosWorker createLaunchableMesosWorker(Protos.TaskID taskID) {
        log.debug("LaunchableMesosWorker parameters: {}", taskManagerParameters);

        return new LaunchableMesosWorker(
                artifactServer,
                taskManagerParameters,
                taskManagerContainerSpec,
                taskID,
                mesosConfig);
    }

    /**
     * Extracts a unique ResourceID from the Mesos task.
     *
     * @param taskId the Mesos TaskID
     * @return The ResourceID for the container
     */
    static ResourceID extractResourceID(Protos.TaskID taskId) {
        return new ResourceID(taskId.getValue());
    }

    /**
     * Extracts the Mesos task goal state from the worker information.
     *
     * @param worker the persistent worker information.
     * @return goal state information for the {@link TaskMonitor}.
     */
    private static TaskMonitor.TaskGoalState extractGoalState(MesosWorkerStore.Worker worker) {
        switch (worker.state()) {
            case New:
                return new TaskMonitor.New(worker.taskID());
            case Launched:
                return new TaskMonitor.Launched(worker.taskID(), worker.slaveID().get());
            case Released:
                return new TaskMonitor.Released(worker.taskID(), worker.slaveID().get());
            default:
                throw new IllegalArgumentException("unsupported worker state");
        }
    }

    private static String extractTerminatedDiagnostics(ResourceID id, Protos.TaskStatus status) {
        return String.format(
                "Worker %s terminated with status: %s, reason: %s, message: %s.",
                id, status.getState(), status.getReason(), status.getMessage());
    }

    private static TaskSchedulerBuilder createOptimizer() {
        return new TaskSchedulerBuilderImpl();
    }

    @VisibleForTesting
    void assertStateCleared() {
        assert (workersInNew.isEmpty());
        assert (requestResourceFutures.isEmpty());
    }
    // ------------------------------------------------------------------------
    //  Internal Classes
    // ------------------------------------------------------------------------

    private class MesosResourceManagerSchedulerCallback implements Scheduler {

        @Override
        public void registered(
                SchedulerDriver driver,
                final Protos.FrameworkID frameworkId,
                final Protos.MasterInfo masterInfo) {
            getMainThreadExecutor()
                    .execute(
                            () ->
                                    MesosResourceManagerDriver.this.registered(
                                            new Registered(frameworkId, masterInfo)));
        }

        @Override
        public void reregistered(SchedulerDriver driver, final Protos.MasterInfo masterInfo) {
            getMainThreadExecutor()
                    .execute(
                            () ->
                                    MesosResourceManagerDriver.this.reregistered(
                                            new ReRegistered(masterInfo)));
        }

        @Override
        public void resourceOffers(SchedulerDriver driver, final List<Protos.Offer> offers) {
            getMainThreadExecutor()
                    .execute(
                            () ->
                                    MesosResourceManagerDriver.this.resourceOffers(
                                            new ResourceOffers(offers)));
        }

        @Override
        public void offerRescinded(SchedulerDriver driver, final Protos.OfferID offerId) {
            getMainThreadExecutor()
                    .execute(
                            () ->
                                    MesosResourceManagerDriver.this.offerRescinded(
                                            new OfferRescinded(offerId)));
        }

        @Override
        public void statusUpdate(SchedulerDriver driver, final Protos.TaskStatus status) {
            getMainThreadExecutor()
                    .execute(
                            () ->
                                    MesosResourceManagerDriver.this.statusUpdate(
                                            new StatusUpdate(status)));
        }

        @Override
        public void frameworkMessage(
                SchedulerDriver driver,
                final Protos.ExecutorID executorId,
                final Protos.SlaveID slaveId,
                final byte[] data) {
            // noop
        }

        @Override
        public void disconnected(SchedulerDriver driver) {
            getMainThreadExecutor()
                    .execute(
                            () -> MesosResourceManagerDriver.this.disconnected(new Disconnected()));
        }

        @Override
        public void slaveLost(SchedulerDriver driver, final Protos.SlaveID slaveId) {
            // noop
        }

        @Override
        public void executorLost(
                SchedulerDriver driver,
                final Protos.ExecutorID executorId,
                final Protos.SlaveID slaveId,
                final int status) {
            // noop
        }

        @Override
        public void error(SchedulerDriver driver, final String message) {
            getResourceEventHandler().onError(new ResourceManagerException(message));
        }
    }

    /** Adapts incoming Akka messages as RPC calls to the resource manager. */
    class AkkaAdapter extends UntypedAbstractActor {
        @Override
        public void onReceive(final Object message) {
            if (message instanceof ReconciliationCoordinator.Reconcile) {
                getMainThreadExecutor()
                        .execute(() -> reconcile((ReconciliationCoordinator.Reconcile) message));
            } else if (message instanceof TaskMonitor.TaskTerminated) {
                getMainThreadExecutor()
                        .execute(() -> taskTerminated((TaskMonitor.TaskTerminated) message));
            } else if (message instanceof AcceptOffers) {
                getMainThreadExecutor().execute(() -> acceptOffers((AcceptOffers) message));
            } else {
                log.error("unrecognized message: " + message);
            }
        }
    }

    /**
     * Creates the Fenzo optimizer (builder). The builder is an indirection to facilitate unit
     * testing of the Launch Coordinator.
     */
    private static class TaskSchedulerBuilderImpl implements TaskSchedulerBuilder {
        private final TaskScheduler.Builder builder = new TaskScheduler.Builder();

        @Override
        public TaskSchedulerBuilder withLeaseRejectAction(Action1<VirtualMachineLease> action) {
            builder.withLeaseRejectAction(action);
            return this;
        }

        @Override
        public TaskSchedulerBuilder withRejectAllExpiredOffers() {
            builder.withRejectAllExpiredOffers();
            return this;
        }

        @Override
        public TaskSchedulerBuilder withLeaseOfferExpirySecs(long leaseOfferExpirySecs) {
            builder.withLeaseOfferExpirySecs(leaseOfferExpirySecs);
            return this;
        }

        @Override
        public TaskScheduler build() {
            return builder.build();
        }
    }
}
