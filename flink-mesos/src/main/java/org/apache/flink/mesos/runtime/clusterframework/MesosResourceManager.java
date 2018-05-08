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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.scheduler.ConnectionMonitor;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.TaskSchedulerBuilder;
import org.apache.flink.mesos.scheduler.Tasks;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;
import org.apache.flink.mesos.scheduler.messages.Disconnected;
import org.apache.flink.mesos.scheduler.messages.ExecutorLost;
import org.apache.flink.mesos.scheduler.messages.FrameworkMessage;
import org.apache.flink.mesos.scheduler.messages.OfferRescinded;
import org.apache.flink.mesos.scheduler.messages.ReRegistered;
import org.apache.flink.mesos.scheduler.messages.Registered;
import org.apache.flink.mesos.scheduler.messages.ResourceOffers;
import org.apache.flink.mesos.scheduler.messages.SlaveLost;
import org.apache.flink.mesos.scheduler.messages.StatusUpdate;
import org.apache.flink.mesos.util.MesosArtifactServer;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.pattern.Patterns;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.duration.FiniteDuration;

/**
 * The Mesos implementation of the resource manager.
 */
public class MesosResourceManager extends ResourceManager<RegisteredMesosWorkerNode> {
	protected static final Logger LOG = LoggerFactory.getLogger(MesosResourceManager.class);

	/** The Flink configuration. */
	private final Configuration flinkConfig;

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

	/** A local actor system for using the helper actors. */
	private final ActorSystem actorSystem;

	/** Mesos scheduler driver. */
	private SchedulerDriver schedulerDriver;

	/** an adapter to receive messages from Akka actors. */
	private ActorRef selfActor;

	private ActorRef connectionMonitor;

	private ActorRef taskMonitor;

	private ActorRef launchCoordinator;

	private ActorRef reconciliationCoordinator;

	/** planning state related to workers - package private for unit test purposes. */
	final Map<ResourceID, MesosWorkerStore.Worker> workersInNew;
	final Map<ResourceID, MesosWorkerStore.Worker> workersInLaunch;
	final Map<ResourceID, MesosWorkerStore.Worker> workersBeingReturned;

	public MesosResourceManager(
			// base class
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			// Mesos specifics
			Configuration flinkConfig,
			MesosServices mesosServices,
			MesosConfiguration mesosConfig,
			MesosTaskManagerParameters taskManagerParameters,
			ContainerSpecification taskManagerContainerSpec) {
		super(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler);

		this.mesosServices = Preconditions.checkNotNull(mesosServices);
		this.actorSystem = Preconditions.checkNotNull(mesosServices.getLocalActorSystem());

		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.mesosConfig = Preconditions.checkNotNull(mesosConfig);

		this.artifactServer = Preconditions.checkNotNull(mesosServices.getArtifactServer());

		this.taskManagerParameters = Preconditions.checkNotNull(taskManagerParameters);
		this.taskManagerContainerSpec = Preconditions.checkNotNull(taskManagerContainerSpec);

		this.workersInNew = new HashMap<>(8);
		this.workersInLaunch = new HashMap<>(8);
		this.workersBeingReturned = new HashMap<>(8);
	}

	protected ActorRef createSelfActor() {
		return actorSystem.actorOf(
			Props.create(AkkaAdapter.class, this),
			"ResourceManager");
	}

	protected ActorRef createConnectionMonitor() {
		return actorSystem.actorOf(
			ConnectionMonitor.createActorProps(ConnectionMonitor.class, flinkConfig),
			"connectionMonitor");
	}

	protected ActorRef createTaskMonitor(SchedulerDriver schedulerDriver) {
		return actorSystem.actorOf(
			Tasks.createActorProps(Tasks.class, selfActor, flinkConfig, schedulerDriver, TaskMonitor.class),
			"tasks");
	}

	protected ActorRef createLaunchCoordinator(
			SchedulerDriver schedulerDriver,
			ActorRef selfActor) {
		return actorSystem.actorOf(
			LaunchCoordinator.createActorProps(LaunchCoordinator.class, selfActor, flinkConfig, schedulerDriver, createOptimizer()),
			"launchCoordinator");
	}

	protected ActorRef createReconciliationCoordinator(SchedulerDriver schedulerDriver) {
		return actorSystem.actorOf(
			ReconciliationCoordinator.createActorProps(ReconciliationCoordinator.class, flinkConfig, schedulerDriver),
			"reconciliationCoordinator");
	}

	// ------------------------------------------------------------------------
	//  Resource Manager overrides
	// ------------------------------------------------------------------------

	/**
	 * Starts the Mesos-specifics.
	 */
	@Override
	protected void initialize() throws ResourceManagerException {
		// create and start the worker store
		try {
			this.workerStore = mesosServices.createMesosWorkerStore(flinkConfig, getRpcService().getExecutor());
			workerStore.start();
		} catch (Exception e) {
			throw new ResourceManagerException("Unable to initialize the worker store.", e);
		}

		// register with Mesos
		// TODO : defer connection until RM acquires leadership

		Protos.FrameworkInfo.Builder frameworkInfo = mesosConfig.frameworkInfo()
			.clone()
			.setCheckpoint(true);
		try {
			Option<Protos.FrameworkID> frameworkID = workerStore.getFrameworkID();
			if (frameworkID.isEmpty()) {
				LOG.info("Registering as new framework.");
			} else {
				LOG.info("Recovery scenario: re-registering using framework ID {}.", frameworkID.get().getValue());
				frameworkInfo.setId(frameworkID.get());
			}
		} catch (Exception e) {
			throw new ResourceManagerException("Unable to recover the framework ID.", e);
		}

		MesosConfiguration initializedMesosConfig = mesosConfig.withFrameworkInfo(frameworkInfo);
		MesosConfiguration.logMesosConfig(LOG, initializedMesosConfig);
		schedulerDriver = initializedMesosConfig.createDriver(
			new MesosResourceManagerSchedulerCallback(),
			false);

		// create supporting actors
		selfActor = createSelfActor();
		connectionMonitor = createConnectionMonitor();
		launchCoordinator = createLaunchCoordinator(schedulerDriver, selfActor);
		reconciliationCoordinator = createReconciliationCoordinator(schedulerDriver);
		taskMonitor = createTaskMonitor(schedulerDriver);

		// recover state
		try {
			recoverWorkers();
		} catch (Exception e) {
			throw new ResourceManagerException("Unable to recover Mesos worker state.", e);
		}

		// configure the artifact server to serve the TM container artifacts
		try {
			LaunchableMesosWorker.configureArtifactServer(artifactServer, taskManagerContainerSpec);
		}
		catch (IOException e) {
			throw new ResourceManagerException("Unable to configure the artifact server with TaskManager artifacts.", e);
		}

		// begin scheduling
		connectionMonitor.tell(new ConnectionMonitor.Start(), selfActor);
		schedulerDriver.start();

		LOG.info("Mesos resource manager initialized.");
	}

	/**
	 * Recover framework/worker information persisted by a prior incarnation of the RM.
	 */
	private void recoverWorkers() throws Exception {
		// if this resource manager is recovering from failure,
		// then some worker tasks are most likely still alive and we can re-obtain them
		final List<MesosWorkerStore.Worker> tasksFromPreviousAttempts = workerStore.recoverWorkers();

		assert(workersInNew.isEmpty());
		assert(workersInLaunch.isEmpty());
		assert(workersBeingReturned.isEmpty());

		if (!tasksFromPreviousAttempts.isEmpty()) {
			LOG.info("Retrieved {} TaskManagers from previous attempt", tasksFromPreviousAttempts.size());

			List<Tuple2<TaskRequest, String>> toAssign = new ArrayList<>(tasksFromPreviousAttempts.size());

			for (final MesosWorkerStore.Worker worker : tasksFromPreviousAttempts) {
				LaunchableMesosWorker launchable = createLaunchableMesosWorker(worker.taskID(), worker.profile());

				switch(worker.state()) {
					case New:
						// remove new workers because allocation requests are transient
						workerStore.removeWorker(worker.taskID());
						break;
					case Launched:
						workersInLaunch.put(extractResourceID(worker.taskID()), worker);
						toAssign.add(new Tuple2<>(launchable.taskRequest(), worker.hostname().get()));
						break;
					case Released:
						workersBeingReturned.put(extractResourceID(worker.taskID()), worker);
						break;
				}
				taskMonitor.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)), selfActor);
			}

			// tell the launch coordinator about prior assignments
			if (toAssign.size() >= 1) {
				launchCoordinator.tell(new LaunchCoordinator.Assign(toAssign), selfActor);
			}
		}
	}

	@Override
	public CompletableFuture<Void> postStop() {
		FiniteDuration stopTimeout = new FiniteDuration(5L, TimeUnit.SECONDS);

		CompletableFuture<Boolean> stopTaskMonitorFuture = stopActor(taskMonitor, stopTimeout);
		taskMonitor = null;

		CompletableFuture<Boolean> stopConnectionMonitorFuture = stopActor(connectionMonitor, stopTimeout);
		connectionMonitor = null;

		CompletableFuture<Boolean> stopLaunchCoordinatorFuture = stopActor(launchCoordinator, stopTimeout);
		launchCoordinator = null;

		CompletableFuture<Boolean> stopReconciliationCoordinatorFuture = stopActor(reconciliationCoordinator, stopTimeout);
		reconciliationCoordinator = null;

		CompletableFuture<Void> stopFuture = CompletableFuture.allOf(
			stopTaskMonitorFuture,
			stopConnectionMonitorFuture,
			stopLaunchCoordinatorFuture,
			stopReconciliationCoordinatorFuture);

		final CompletableFuture<Void> terminationFuture = super.postStop();

		return stopFuture.thenCombine(
			terminationFuture,
			(Void voidA, Void voidB) -> null);
	}

	@Override
	protected void internalDeregisterApplication(
			ApplicationStatus finalStatus,
			@Nullable String diagnostics) throws ResourceManagerException {
		LOG.info("Shutting down and unregistering as a Mesos framework.");

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
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not stop the Mesos worker store.", ex),
				exception);
		}

		if (exception != null) {
			throw new ResourceManagerException("Could not properly shut down the Mesos application.", exception);
		}
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		LOG.info("Starting a new worker.");
		try {
			// generate new workers into persistent state and launch associated actors
			MesosWorkerStore.Worker worker = MesosWorkerStore.Worker.newWorker(workerStore.newTaskID(), resourceProfile);
			workerStore.putWorker(worker);
			workersInNew.put(extractResourceID(worker.taskID()), worker);

			LaunchableMesosWorker launchable = createLaunchableMesosWorker(worker.taskID(), resourceProfile);

			LOG.info("Scheduling Mesos task {} with ({} MB, {} cpus).",
				launchable.taskID().getValue(), launchable.taskRequest().getMemory(), launchable.taskRequest().getCPUs());

			// tell the task monitor about the new plans
			taskMonitor.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)), selfActor);

			// tell the launch coordinator to launch the new tasks
			launchCoordinator.tell(new LaunchCoordinator.Launch(Collections.singletonList((LaunchableTask) launchable)), selfActor);
		} catch (Exception ex) {
			onFatalError(new ResourceManagerException("Unable to request new workers.", ex));
		}
	}

	@Override
	public boolean stopWorker(RegisteredMesosWorkerNode workerNode) {
		LOG.info("Stopping worker {}.", workerNode.getResourceID());
		try {

			if (workersInLaunch.containsKey(workerNode.getResourceID())) {
				// update persistent state of worker to Released
				MesosWorkerStore.Worker worker = workersInLaunch.remove(workerNode.getResourceID());
				worker = worker.releaseWorker();
				workerStore.putWorker(worker);
				workersBeingReturned.put(extractResourceID(worker.taskID()), worker);

				taskMonitor.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)), selfActor);

				if (worker.hostname().isDefined()) {
					// tell the launch coordinator that the task is being unassigned from the host, for planning purposes
					launchCoordinator.tell(new LaunchCoordinator.Unassign(worker.taskID(), worker.hostname().get()), selfActor);
				}
			}
			else if (workersBeingReturned.containsKey(workerNode.getResourceID())) {
				LOG.info("Ignoring request to stop worker {} because it is already being stopped.", workerNode.getResourceID());
			}
			else {
				LOG.warn("Unrecognized worker {}.", workerNode.getResourceID());
			}
		}
		catch (Exception e) {
			onFatalError(new ResourceManagerException("Unable to release a worker.", e));
		}

		return true;
	}

	/**
	 * Callback when a worker was started.
	 *
	 * @param resourceID The worker resource id (as provided by the TaskExecutor)
	 */
	@Override
	protected RegisteredMesosWorkerNode workerStarted(ResourceID resourceID) {

		// note: this may occur more than once for a given worker.
		MesosWorkerStore.Worker inLaunch = workersInLaunch.get(resourceID);
		if (inLaunch != null) {
			return new RegisteredMesosWorkerNode(inLaunch);
		} else {
			// the worker is unrecognized or was already released
			// return null to indicate that TaskExecutor registration should be declined
			return null;
		}
	}

	// ------------------------------------------------------------------------
	//  Mesos specific methods
	// ------------------------------------------------------------------------

	protected void registered(Registered message) {
		connectionMonitor.tell(message, selfActor);
		try {
			workerStore.setFrameworkID(Option.apply(message.frameworkId()));
		} catch (Exception ex) {
			onFatalError(new ResourceManagerException("Unable to store the assigned framework ID.", ex));
			return;
		}

		launchCoordinator.tell(message, selfActor);
		reconciliationCoordinator.tell(message, selfActor);
		taskMonitor.tell(message, selfActor);
	}

	/**
	 * Called when reconnected to Mesos following a failover event.
	 */
	protected void reregistered(ReRegistered message) {
		connectionMonitor.tell(message, selfActor);
		launchCoordinator.tell(message, selfActor);
		reconciliationCoordinator.tell(message, selfActor);
		taskMonitor.tell(message, selfActor);
	}

	/**
	 * Called when disconnected from Mesos.
	 */
	protected void disconnected(Disconnected message) {
		connectionMonitor.tell(message, selfActor);
		launchCoordinator.tell(message, selfActor);
		reconciliationCoordinator.tell(message, selfActor);
		taskMonitor.tell(message, selfActor);
	}

	/**
	 * Called when resource offers are made to the framework.
	 */
	protected void resourceOffers(ResourceOffers message) {
		launchCoordinator.tell(message, selfActor);
	}

	/**
	 * Called when resource offers are rescinded.
	 */
	protected void offerRescinded(OfferRescinded message) {
		launchCoordinator.tell(message, selfActor);
	}

	/**
	 * Handles a task status update from Mesos.
	 */
	protected void statusUpdate(StatusUpdate message) {
		taskMonitor.tell(message, selfActor);
		reconciliationCoordinator.tell(message, selfActor);
		schedulerDriver.acknowledgeStatusUpdate(message.status());
	}

	protected void frameworkMessage(FrameworkMessage message) {}

	protected void slaveLost(SlaveLost message) {}

	protected void executorLost(ExecutorLost message) {}

	/**
	 * Accept offers as advised by the launch coordinator.
	 *
	 * <p>Acceptance is routed through the RM to update the persistent state before
	 * forwarding the message to Mesos.
	 */
	public void acceptOffers(AcceptOffers msg) {
		try {
			List<TaskMonitor.TaskGoalStateUpdated> toMonitor = new ArrayList<>(msg.operations().size());

			// transition the persistent state of some tasks to Launched
			for (Protos.Offer.Operation op : msg.operations()) {
				if (op.getType() == Protos.Offer.Operation.Type.LAUNCH) {
					for (Protos.TaskInfo info : op.getLaunch().getTaskInfosList()) {
						MesosWorkerStore.Worker worker = workersInNew.remove(extractResourceID(info.getTaskId()));
						assert (worker != null);

						worker = worker.launchWorker(info.getSlaveId(), msg.hostname());
						workerStore.putWorker(worker);
						workersInLaunch.put(extractResourceID(worker.taskID()), worker);

						LOG.info("Launching Mesos task {} on host {}.",
							worker.taskID().getValue(), worker.hostname().get());

						toMonitor.add(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)));
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
			onFatalError(new ResourceManagerException("unable to accept offers", ex));
		}
	}

	/**
	 * Handles a reconciliation request from a task monitor.
	 */
	public void reconcile(ReconciliationCoordinator.Reconcile message) {
		// forward to the reconciliation coordinator
		reconciliationCoordinator.tell(message, selfActor);
	}

	/**
	 * Handles a termination notification from a task monitor.
	 */
	public void taskTerminated(TaskMonitor.TaskTerminated message) {
		Protos.TaskID taskID = message.taskID();
		Protos.TaskStatus status = message.status();

		// note: this callback occurs for failed containers and for released containers alike
		final ResourceID id = extractResourceID(taskID);

		boolean existed;
		try {
			existed = workerStore.removeWorker(taskID);
		} catch (Exception ex) {
			onFatalError(new ResourceManagerException("unable to remove worker", ex));
			return;
		}

		if (!existed) {
			LOG.info("Received a termination notice for an unrecognized worker: {}", id);
			return;
		}

		// check if this is a failed task or a released task
		assert(!workersInNew.containsKey(id));
		if (workersBeingReturned.remove(id) != null) {
			// regular finished worker that we released
			LOG.info("Worker {} finished successfully with message: {}",
				id, status.getMessage());
		} else {
			// failed worker, either at startup, or running
			final MesosWorkerStore.Worker launched = workersInLaunch.remove(id);
			assert(launched != null);
			LOG.info("Worker {} failed with status: {}, reason: {}, message: {}.",
				id, status.getState(), status.getReason(), status.getMessage());
		}

		closeTaskManagerConnection(id, new Exception(status.getMessage()));
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Tries to shut down the given actor gracefully.
	 *
	 * @param actorRef specifying the actor to shut down
	 * @param timeout for the graceful shut down
	 * @return Future containing the result of the graceful shut down
	 */
	private CompletableFuture<Boolean> stopActor(final ActorRef actorRef, FiniteDuration timeout) {
		return FutureUtils.toJava(Patterns.gracefulStop(actorRef, timeout))
			.exceptionally(
				(Throwable throwable) -> {
					// The actor did not stop gracefully in time, try to directly stop it
					actorSystem.stop(actorRef);

					log.warn("Could not stop actor {} gracefully.", actorRef.path(), throwable);

					return true;
				}
			);
	}

	/**
	 * Creates a launchable task for Fenzo to process.
	 */
	private LaunchableMesosWorker createLaunchableMesosWorker(Protos.TaskID taskID, ResourceProfile resourceProfile) {

		// create the specific TM parameters from the resource profile and some defaults
		MesosTaskManagerParameters params = new MesosTaskManagerParameters(
			resourceProfile.getCpuCores() < 1.0 ? taskManagerParameters.cpus() : resourceProfile.getCpuCores(),
			taskManagerParameters.gpus(),
			taskManagerParameters.containerType(),
			taskManagerParameters.containerImageName(),
			new ContaineredTaskManagerParameters(
				ResourceProfile.UNKNOWN.equals(resourceProfile) ? taskManagerParameters.containeredParameters().taskManagerTotalMemoryMB() : resourceProfile.getMemoryInMB(),
				ResourceProfile.UNKNOWN.equals(resourceProfile) ? taskManagerParameters.containeredParameters().taskManagerHeapSizeMB() : resourceProfile.getHeapMemoryInMB(),
				ResourceProfile.UNKNOWN.equals(resourceProfile) ? taskManagerParameters.containeredParameters().taskManagerDirectMemoryLimitMB() : resourceProfile.getDirectMemoryInMB(),
				1,
				new HashMap<>(taskManagerParameters.containeredParameters().taskManagerEnv())),
			taskManagerParameters.containerVolumes(),
			taskManagerParameters.dockerParameters(),
			taskManagerParameters.constraints(),
			taskManagerParameters.command(),
			taskManagerParameters.bootstrapCommand(),
			taskManagerParameters.getTaskManagerHostname()
		);

		LOG.debug("LaunchableMesosWorker parameters: {}", params);

		LaunchableMesosWorker launchable =
			new LaunchableMesosWorker(
				artifactServer,
				params,
				taskManagerContainerSpec,
				taskID,
				mesosConfig);

		return launchable;
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
	 * @return goal state information for the {@Link TaskMonitor}.
	 */
	static TaskMonitor.TaskGoalState extractGoalState(MesosWorkerStore.Worker worker) {
		switch(worker.state()) {
			case New: return new TaskMonitor.New(worker.taskID());
			case Launched: return new TaskMonitor.Launched(worker.taskID(), worker.slaveID().get());
			case Released: return new TaskMonitor.Released(worker.taskID(), worker.slaveID().get());
			default: throw new IllegalArgumentException("unsupported worker state");
		}
	}

	/**
	 * Creates the Fenzo optimizer (builder).
	 * The builder is an indirection to facilitate unit testing of the Launch Coordinator.
	 */
	private static TaskSchedulerBuilder createOptimizer() {
		return new TaskSchedulerBuilder() {
			TaskScheduler.Builder builder = new TaskScheduler.Builder();

			@Override
			public TaskSchedulerBuilder withLeaseRejectAction(Action1<VirtualMachineLease> action) {
				builder.withLeaseRejectAction(action);
				return this;
			}

			@Override
			public TaskScheduler build() {
				return builder.build();
			}
		};
	}

	private class MesosResourceManagerSchedulerCallback implements Scheduler {

		@Override
		public void registered(SchedulerDriver driver, final Protos.FrameworkID frameworkId, final Protos.MasterInfo masterInfo) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.registered(new Registered(frameworkId, masterInfo));
				}
			});
		}

		@Override
		public void reregistered(SchedulerDriver driver, final Protos.MasterInfo masterInfo) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.reregistered(new ReRegistered(masterInfo));
				}
			});
		}

		@Override
		public void resourceOffers(SchedulerDriver driver, final List<Protos.Offer> offers) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.resourceOffers(new ResourceOffers(offers));
				}
			});
		}

		@Override
		public void offerRescinded(SchedulerDriver driver, final Protos.OfferID offerId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.offerRescinded(new OfferRescinded(offerId));
				}
			});
		}

		@Override
		public void statusUpdate(SchedulerDriver driver, final Protos.TaskStatus status) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.statusUpdate(new StatusUpdate(status));
				}
			});
		}

		@Override
		public void frameworkMessage(SchedulerDriver driver, final Protos.ExecutorID executorId, final Protos.SlaveID slaveId, final byte[] data) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.frameworkMessage(new FrameworkMessage(executorId, slaveId, data));
				}
			});
		}

		@Override
		public void disconnected(SchedulerDriver driver) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.disconnected(new Disconnected());
				}
			});
		}

		@Override
		public void slaveLost(SchedulerDriver driver, final Protos.SlaveID slaveId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.slaveLost(new SlaveLost(slaveId));
				}
			});
		}

		@Override
		public void executorLost(SchedulerDriver driver, final Protos.ExecutorID executorId, final Protos.SlaveID slaveId, final int status) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					MesosResourceManager.this.executorLost(new ExecutorLost(executorId, slaveId, status));
				}
			});
		}

		@Override
		public void error(SchedulerDriver driver, final String message) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					onFatalError(new ResourceManagerException(message));
				}
			});
		}
	}

	/**
	 * Adapts incoming Akka messages as RPC calls to the resource manager.
	 */
	private class AkkaAdapter extends UntypedActor {
		@Override
		public void onReceive(final Object message) throws Exception {
			if (message instanceof ReconciliationCoordinator.Reconcile) {
				runAsync(new Runnable() {
					@Override
					public void run() {
						reconcile((ReconciliationCoordinator.Reconcile) message);
					}
				});
			} else if (message instanceof TaskMonitor.TaskTerminated) {
				runAsync(new Runnable() {
					@Override
					public void run() {
						taskTerminated((TaskMonitor.TaskTerminated) message);
					}
				});
			} else if (message instanceof AcceptOffers) {
				runAsync(new Runnable() {
					@Override
					public void run() {
						acceptOffers((AcceptOffers) message);
					}
				});
			} else {
				MesosResourceManager.LOG.error("unrecognized message: " + message);
			}
		}
	}
}
