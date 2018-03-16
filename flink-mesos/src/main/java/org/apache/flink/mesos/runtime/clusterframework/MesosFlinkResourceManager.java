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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.mesos.configuration.MesosOptions;
import org.apache.flink.mesos.runtime.clusterframework.store.MesosWorkerStore;
import org.apache.flink.mesos.scheduler.ConnectionMonitor;
import org.apache.flink.mesos.scheduler.LaunchCoordinator;
import org.apache.flink.mesos.scheduler.LaunchableTask;
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator;
import org.apache.flink.mesos.scheduler.SchedulerProxy;
import org.apache.flink.mesos.scheduler.TaskMonitor;
import org.apache.flink.mesos.scheduler.TaskSchedulerBuilder;
import org.apache.flink.mesos.scheduler.Tasks;
import org.apache.flink.mesos.scheduler.messages.AcceptOffers;
import org.apache.flink.mesos.scheduler.messages.Disconnected;
import org.apache.flink.mesos.scheduler.messages.Error;
import org.apache.flink.mesos.scheduler.messages.OfferRescinded;
import org.apache.flink.mesos.scheduler.messages.ReRegistered;
import org.apache.flink.mesos.scheduler.messages.Registered;
import org.apache.flink.mesos.scheduler.messages.ResourceOffers;
import org.apache.flink.mesos.scheduler.messages.StatusUpdate;
import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.FrameworkInfo.Capability;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

import static java.util.Objects.requireNonNull;

/**
 * Flink Resource Manager for Apache Mesos.
 */
public class MesosFlinkResourceManager extends FlinkResourceManager<RegisteredMesosWorkerNode> {

	/** The Mesos configuration (master and framework info). */
	private final MesosConfiguration mesosConfig;

	/** The TaskManager container parameters (like container memory size). */
	private final MesosTaskManagerParameters taskManagerParameters;

	/** Container specification for launching a TM. */
	private final ContainerSpecification taskManagerContainerSpec;

	/** Resolver for HTTP artifacts. **/
	private final MesosArtifactResolver artifactResolver;

	/** Number of failed Mesos tasks before stopping the application. -1 means infinite. */
	private final int maxFailedTasks;

	/** Callback handler for the asynchronous Mesos scheduler. */
	private SchedulerProxy schedulerCallbackHandler;

	/** Mesos scheduler driver. */
	private SchedulerDriver schedulerDriver;

	private ActorRef connectionMonitor;

	private ActorRef taskRouter;

	private ActorRef launchCoordinator;

	private ActorRef reconciliationCoordinator;

	private final MesosWorkerStore workerStore;

	/** planning state related to workers - package private for unit test purposes. */
	final Map<ResourceID, MesosWorkerStore.Worker> workersInNew;
	final Map<ResourceID, MesosWorkerStore.Worker> workersInLaunch;
	final Map<ResourceID, MesosWorkerStore.Worker> workersBeingReturned;

	/** The number of failed tasks since the master became active. */
	private int failedTasksSoFar;

	public MesosFlinkResourceManager(
		Configuration flinkConfig,
		MesosConfiguration mesosConfig,
		MesosWorkerStore workerStore,
		LeaderRetrievalService leaderRetrievalService,
		MesosTaskManagerParameters taskManagerParameters,
		ContainerSpecification taskManagerContainerSpec,
		MesosArtifactResolver artifactResolver,
		int maxFailedTasks,
		int numInitialTaskManagers) {

		super(numInitialTaskManagers, flinkConfig, leaderRetrievalService);

		this.mesosConfig = requireNonNull(mesosConfig);

		this.workerStore = requireNonNull(workerStore);
		this.artifactResolver = requireNonNull(artifactResolver);

		this.taskManagerParameters = requireNonNull(taskManagerParameters);
		this.taskManagerContainerSpec = requireNonNull(taskManagerContainerSpec);
		this.maxFailedTasks = maxFailedTasks;

		this.workersInNew = new HashMap<>();
		this.workersInLaunch = new HashMap<>();
		this.workersBeingReturned = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  Mesos-specific behavior
	// ------------------------------------------------------------------------

	@Override
	protected void initialize() throws Exception {
		LOG.info("Initializing Mesos resource master");

		workerStore.start();

		// create the scheduler driver to communicate with Mesos
		schedulerCallbackHandler = new SchedulerProxy(self());

		// register with Mesos
		FrameworkInfo.Builder frameworkInfo = mesosConfig.frameworkInfo()
			.clone()
			.setCheckpoint(true);

		Option<Protos.FrameworkID> frameworkID = workerStore.getFrameworkID();
		if (frameworkID.isEmpty()) {
			LOG.info("Registering as new framework.");
		}
		else {
			LOG.info("Recovery scenario: re-registering using framework ID {}.", frameworkID.get().getValue());
			frameworkInfo.setId(frameworkID.get());
		}

		if (taskManagerParameters.gpus() > 0) {
			LOG.info("Add GPU_RESOURCES capability to framework");
			frameworkInfo.addCapabilities(Capability.newBuilder().setType(Capability.Type.GPU_RESOURCES));
		}

		MesosConfiguration initializedMesosConfig = mesosConfig.withFrameworkInfo(frameworkInfo);
		MesosConfiguration.logMesosConfig(LOG, initializedMesosConfig);
		schedulerDriver = initializedMesosConfig.createDriver(schedulerCallbackHandler, false);

		// create supporting actors
		connectionMonitor = createConnectionMonitor();
		launchCoordinator = createLaunchCoordinator();
		reconciliationCoordinator = createReconciliationCoordinator();
		taskRouter = createTaskRouter();

		recoverWorkers();

		connectionMonitor.tell(new ConnectionMonitor.Start(), self());
		schedulerDriver.start();
	}

	protected ActorRef createConnectionMonitor() {
		return context().actorOf(
			ConnectionMonitor.createActorProps(ConnectionMonitor.class, config),
			"connectionMonitor");
	}

	protected ActorRef createTaskRouter() {
		return context().actorOf(
			Tasks.createActorProps(Tasks.class, self(), config, schedulerDriver, TaskMonitor.class),
			"tasks");
	}

	protected ActorRef createLaunchCoordinator() {
		return context().actorOf(
			LaunchCoordinator.createActorProps(LaunchCoordinator.class, self(), config, schedulerDriver, createOptimizer()),
			"launchCoordinator");
	}

	protected ActorRef createReconciliationCoordinator() {
		return context().actorOf(
			ReconciliationCoordinator.createActorProps(ReconciliationCoordinator.class, config, schedulerDriver),
			"reconciliationCoordinator");
	}

	@Override
	public void postStop() {
		LOG.info("Stopping Mesos resource master");
		super.postStop();
	}

	// ------------------------------------------------------------------------
	//  Actor messages
	// ------------------------------------------------------------------------

	@Override
	protected void handleMessage(Object message) {

		// check for Mesos-specific actor messages first

		// --- messages about Mesos connection
		if (message instanceof Registered) {
			registered((Registered) message);
		} else if (message instanceof ReRegistered) {
			reregistered((ReRegistered) message);
		} else if (message instanceof Disconnected) {
			disconnected((Disconnected) message);
		} else if (message instanceof Error) {
			error(((Error) message).message());

		// --- messages about offers
		} else if (message instanceof ResourceOffers || message instanceof OfferRescinded) {
			launchCoordinator.tell(message, self());
		} else if (message instanceof AcceptOffers) {
			acceptOffers((AcceptOffers) message);

		// --- messages about tasks
		} else if (message instanceof StatusUpdate) {
			taskStatusUpdated((StatusUpdate) message);
		} else if (message instanceof ReconciliationCoordinator.Reconcile) {
			// a reconciliation request from a task
			reconciliationCoordinator.tell(message, self());
		} else if (message instanceof TaskMonitor.TaskTerminated) {
			// a termination message from a task
			TaskMonitor.TaskTerminated msg = (TaskMonitor.TaskTerminated) message;
			taskTerminated(msg.taskID(), msg.status());

		} else {
			// message handled by the generic resource master code
			super.handleMessage(message);
		}
	}

	/**
	 * Called to shut down the cluster (not a failover situation).
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics An optional diagnostics message.
	 */
	@Override
	protected void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {

		LOG.info("Shutting down and unregistering as a Mesos framework.");
		try {
			// unregister the framework, which implicitly removes all tasks.
			schedulerDriver.stop(false);
		} catch (Exception ex) {
			LOG.warn("unable to unregister the framework", ex);
		}

		try {
			workerStore.stop(true);
		} catch (Exception ex) {
			LOG.warn("unable to stop the worker state store", ex);
		}

		context().stop(self());
	}

	@Override
	protected void fatalError(String message, Throwable error) {
		// we do not unregister, but cause a hard fail of this process, to have it
		// restarted by the dispatcher
		LOG.error("FATAL ERROR IN MESOS APPLICATION MASTER: " + message, error);
		LOG.error("Shutting down process");

		// kill this process, this will make an external supervisor (the dispatcher) restart the process
		System.exit(EXIT_CODE_FATAL_ERROR);
	}

	// ------------------------------------------------------------------------
	//  Worker Management
	// ------------------------------------------------------------------------

	/**
	 * Recover framework/worker information persisted by a prior incarnation of the RM.
	 */
	private void recoverWorkers() throws Exception {
		// if this application master starts as part of an ApplicationMaster/JobManager recovery,
		// then some worker tasks are most likely still alive and we can re-obtain them
		final List<MesosWorkerStore.Worker> tasksFromPreviousAttempts = workerStore.recoverWorkers();

		if (!tasksFromPreviousAttempts.isEmpty()) {
			LOG.info("Retrieved {} TaskManagers from previous attempt", tasksFromPreviousAttempts.size());

			List<Tuple2<TaskRequest, String>> toAssign = new ArrayList<>(tasksFromPreviousAttempts.size());
			List<LaunchableTask> toLaunch = new ArrayList<>(tasksFromPreviousAttempts.size());

			for (final MesosWorkerStore.Worker worker : tasksFromPreviousAttempts) {
				LaunchableMesosWorker launchable = createLaunchableMesosWorker(worker.taskID());

				switch (worker.state()) {
					case New:
						workersInNew.put(extractResourceID(worker.taskID()), worker);
						toLaunch.add(launchable);
						break;
					case Launched:
						workersInLaunch.put(extractResourceID(worker.taskID()), worker);
						toAssign.add(new Tuple2<>(launchable.taskRequest(), worker.hostname().get()));
						break;
					case Released:
						workersBeingReturned.put(extractResourceID(worker.taskID()), worker);
						break;
				}
				taskRouter.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)), self());
			}

			// tell the launch coordinator about prior assignments
			if (toAssign.size() >= 1) {
				launchCoordinator.tell(new LaunchCoordinator.Assign(toAssign), self());
			}
			// tell the launch coordinator to launch any new tasks
			if (toLaunch.size() >= 1) {
				launchCoordinator.tell(new LaunchCoordinator.Launch(toLaunch), self());
			}
		}
	}

	/**
	 * Plan for some additional workers to be launched.
	 *
	 * @param numWorkers The number of workers to allocate.
	 */
	@Override
	protected void requestNewWorkers(int numWorkers) {

		try {
			List<TaskMonitor.TaskGoalStateUpdated> toMonitor = new ArrayList<>(numWorkers);
			List<LaunchableTask> toLaunch = new ArrayList<>(numWorkers);

			// generate new workers into persistent state and launch associated actors
			for (int i = 0; i < numWorkers; i++) {
				MesosWorkerStore.Worker worker = MesosWorkerStore.Worker.newWorker(workerStore.newTaskID());
				workerStore.putWorker(worker);
				workersInNew.put(extractResourceID(worker.taskID()), worker);

				LaunchableMesosWorker launchable = createLaunchableMesosWorker(worker.taskID());

				LOG.info("Scheduling Mesos task {} with ({} MB, {} cpus, {} gpus).",
					launchable.taskID().getValue(), launchable.taskRequest().getMemory(), launchable.taskRequest().getCPUs(),
					launchable.taskRequest().getScalarRequests().get("gpus"));

				toMonitor.add(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)));
				toLaunch.add(launchable);
			}

			// tell the task router about the new plans
			for (TaskMonitor.TaskGoalStateUpdated update : toMonitor) {
				taskRouter.tell(update, self());
			}

			// tell the launch coordinator to launch the new tasks
			if (toLaunch.size() >= 1) {
				launchCoordinator.tell(new LaunchCoordinator.Launch(toLaunch), self());
			}
		} catch (Exception ex) {
			fatalError("unable to request new workers", ex);
		}
	}

	/**
	 * Accept offers as advised by the launch coordinator.
	 *
	 * <p>Acceptance is routed through the RM to update the persistent state before
	 * forwarding the message to Mesos.
	 */
	private void acceptOffers(AcceptOffers msg) {

		try {
			List<TaskMonitor.TaskGoalStateUpdated> toMonitor = new ArrayList<>(msg.operations().size());

			// transition the persistent state of some tasks to Launched
			for (Protos.Offer.Operation op : msg.operations()) {
				if (op.getType() != Protos.Offer.Operation.Type.LAUNCH) {
					continue;
				}
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

			// tell the task router about the new plans
			for (TaskMonitor.TaskGoalStateUpdated update : toMonitor) {
				taskRouter.tell(update, self());
			}

			// send the acceptance message to Mesos
			schedulerDriver.acceptOffers(msg.offerIds(), msg.operations(), msg.filters());
		} catch (Exception ex) {
			fatalError("unable to accept offers", ex);
		}
	}

	/**
	 * Handle a task status change.
	 */
	private void taskStatusUpdated(StatusUpdate message) {
		taskRouter.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		schedulerDriver.acknowledgeStatusUpdate(message.status());
	}

	/**
	 * Accept the given started worker into the internal state.
	 *
	 * @param resourceID The worker resource id
	 * @return A registered worker node record.
	 */
	@Override
	protected RegisteredMesosWorkerNode workerStarted(ResourceID resourceID) {
		MesosWorkerStore.Worker inLaunch = workersInLaunch.remove(resourceID);
		if (inLaunch == null) {
			// Worker was not in state "being launched", this can indicate that the TaskManager
			// in this worker was already registered or that the container was not started
			// by this resource manager. Simply ignore this resourceID.
			return null;
		}
		return new RegisteredMesosWorkerNode(inLaunch);
	}

	/**
	 * Accept the given registered workers into the internal state.
	 *
	 * @param toConsolidate The worker IDs known previously to the JobManager.
	 * @return A collection of registered worker node records.
	 */
	@Override
	protected Collection<RegisteredMesosWorkerNode> reacceptRegisteredWorkers(Collection<ResourceID> toConsolidate) {

		// we check for each task manager if we recognize its Mesos task ID
		List<RegisteredMesosWorkerNode> accepted = new ArrayList<>(toConsolidate.size());
		for (ResourceID resourceID : toConsolidate) {
			MesosWorkerStore.Worker worker = workersInLaunch.remove(resourceID);
			if (worker != null) {
				LOG.info("Mesos worker consolidation recognizes TaskManager {}.", resourceID);
				accepted.add(new RegisteredMesosWorkerNode(worker));
			} else {
				if (isStarted(resourceID)) {
					LOG.info("TaskManager {} has already been registered at the resource manager.", resourceID);
				}
				else {
					LOG.info("Mesos worker consolidation does not recognize TaskManager {}.", resourceID);
				}
			}
		}
		return accepted;
	}

	/**
	 * Release the given pending worker.
	 */
	@Override
	protected void releasePendingWorker(ResourceID id) {
		MesosWorkerStore.Worker worker = workersInLaunch.remove(id);
		if (worker != null) {
			releaseWorker(worker);
		} else {
			LOG.error("Cannot find worker {} to release. Ignoring request.", id);
		}
	}

	/**
	 * Release the given started worker.
	 */
	@Override
	protected void releaseStartedWorker(RegisteredMesosWorkerNode worker) {
		releaseWorker(worker.getWorker());
	}

	/**
	 * Plan for the removal of the given worker.
	 */
	private void releaseWorker(MesosWorkerStore.Worker worker) {
		try {
			LOG.info("Releasing worker {}", worker.taskID());

			// update persistent state of worker to Released
			worker = worker.releaseWorker();
			workerStore.putWorker(worker);
			workersBeingReturned.put(extractResourceID(worker.taskID()), worker);
			taskRouter.tell(new TaskMonitor.TaskGoalStateUpdated(extractGoalState(worker)), self());

			if (worker.hostname().isDefined()) {
				// tell the launch coordinator that the task is being unassigned from the host, for planning purposes
				launchCoordinator.tell(new LaunchCoordinator.Unassign(worker.taskID(), worker.hostname().get()), self());
			}
		}
		catch (Exception ex) {
			fatalError("unable to release worker", ex);
		}
	}

	@Override
	protected int getNumWorkerRequestsPending() {
		return workersInNew.size();
	}

	@Override
	protected int getNumWorkersPendingRegistration() {
		return workersInLaunch.size();
	}

	// ------------------------------------------------------------------------
	//  Callbacks from the Mesos Master
	// ------------------------------------------------------------------------

	/**
	 * Called when connected to Mesos as a new framework.
	 */
	private void registered(Registered message) {
		connectionMonitor.tell(message, self());

		try {
			workerStore.setFrameworkID(Option.apply(message.frameworkId()));
		} catch (Exception ex) {
			fatalError("unable to store the assigned framework ID", ex);
			return;
		}

		launchCoordinator.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		taskRouter.tell(message, self());
	}

	/**
	 * Called when reconnected to Mesos following a failover event.
	 */
	private void reregistered(ReRegistered message) {
		connectionMonitor.tell(message, self());
		launchCoordinator.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		taskRouter.tell(message, self());
	}

	/**
	 * Called when disconnected from Mesos.
	 */
	private void disconnected(Disconnected message) {
		connectionMonitor.tell(message, self());
		launchCoordinator.tell(message, self());
		reconciliationCoordinator.tell(message, self());
		taskRouter.tell(message, self());
	}

	/**
	 * Called when an error is reported by the scheduler callback.
	 */
	private void error(String message) {
		self().tell(new FatalErrorOccurred("Connection to Mesos failed", new Exception(message)), self());
	}

	/**
	 * Invoked when a Mesos task reaches a terminal status.
	 */
	private void taskTerminated(Protos.TaskID taskID, Protos.TaskStatus status) {
		// this callback occurs for failed containers and for released containers alike

		final ResourceID id = extractResourceID(taskID);

		boolean existed;
		try {
			existed = workerStore.removeWorker(taskID);
		} catch (Exception ex) {
			fatalError("unable to remove worker", ex);
			return;
		}

		if (!existed) {
			LOG.info("Received a termination notice for an unrecognized worker: {}", id);
			return;
		}

		// check if this is a failed task or a released task
		if (workersBeingReturned.remove(id) != null) {
			// regular finished worker that we released
			LOG.info("Worker {} finished successfully with diagnostics: {}",
				id, status.getMessage());
		} else {
			// failed worker, either at startup, or running
			final MesosWorkerStore.Worker launched = workersInLaunch.remove(id);
			if (launched != null) {
				LOG.info("Mesos task {} failed, with a TaskManager in launch or registration. " +
					"State: {} Reason: {} ({})", id, status.getState(), status.getReason(), status.getMessage());
				// we will trigger re-acquiring new workers at the end
			} else {
				// failed registered worker
				LOG.info("Mesos task {} failed, with a registered TaskManager. " +
					"State: {} Reason: {} ({})", id, status.getState(), status.getReason(), status.getMessage());

				// notify the generic logic, which notifies the JobManager, etc.
				notifyWorkerFailed(id, "Mesos task " + id + " failed.  State: " + status.getState());
			}

			// general failure logging
			failedTasksSoFar++;

			String diagMessage = String.format("Diagnostics for task %s in state %s : " +
					"reason=%s message=%s",
				id, status.getState(), status.getReason(), status.getMessage());
			sendInfoMessage(diagMessage);

			LOG.info(diagMessage);
			LOG.info("Total number of failed tasks so far: {}", failedTasksSoFar);

			// maxFailedTasks == -1 is infinite number of retries.
			if (maxFailedTasks >= 0 && failedTasksSoFar > maxFailedTasks) {
				String msg = "Stopping Mesos session because the number of failed tasks ("
					+ failedTasksSoFar + ") exceeded the maximum failed tasks ("
					+ maxFailedTasks + "). This number is controlled by the '"
					+ MesosOptions.MAX_FAILED_TASKS.key() + "' configuration setting. "
					+ "By default its the number of requested tasks.";

				LOG.error(msg);
				self().tell(decorateMessage(new StopCluster(ApplicationStatus.FAILED, msg)),
					ActorRef.noSender());

				// no need to do anything else
				return;
			}
		}

		// in case failed containers were among the finished containers, make
		// sure we re-examine and request new ones
		triggerCheckWorkers();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private LaunchableMesosWorker createLaunchableMesosWorker(Protos.TaskID taskID) {
		LaunchableMesosWorker launchable =
			new LaunchableMesosWorker(
				artifactResolver,
				taskManagerParameters,
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
	 * @param worker the persistent worker information.
	 * @return goal state information for the {@Link TaskMonitor}.
	 */
	static TaskMonitor.TaskGoalState extractGoalState(MesosWorkerStore.Worker worker) {
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

	/**
	 * Creates the props needed to instantiate this actor.
	 *
	 * <p>Rather than extracting and validating parameters in the constructor, this factory method takes
	 * care of that. That way, errors occur synchronously, and are not swallowed simply in a
	 * failed asynchronous attempt to start the actor.

	 * @param actorClass
	 *             The actor class, to allow overriding this actor with subclasses for testing.
	 * @param flinkConfig
	 *             The Flink configuration object.
	 * @param taskManagerParameters
	 *             The parameters for launching TaskManager containers.
	 * @param taskManagerContainerSpec
	 *             The container specification.
	 * @param artifactResolver
	 *             The artifact resolver to locate artifacts
	 * @param log
	 *             The logger to log to.
	 *
	 * @return The Props object to instantiate the MesosFlinkResourceManager actor.
	 */
	public static Props createActorProps(
			Class<? extends MesosFlinkResourceManager> actorClass,
			Configuration flinkConfig,
			MesosConfiguration mesosConfig,
			MesosWorkerStore workerStore,
			LeaderRetrievalService leaderRetrievalService,
			MesosTaskManagerParameters taskManagerParameters,
			ContainerSpecification taskManagerContainerSpec,
			MesosArtifactResolver artifactResolver,
			Logger log) {

		final int numInitialTaskManagers = flinkConfig.getInteger(
			MesosOptions.INITIAL_TASKS);
		if (numInitialTaskManagers >= 0) {
			log.info("Mesos framework to allocate {} initial tasks",
				numInitialTaskManagers);
		}
		else {
			throw new IllegalConfigurationException("Invalid value for " +
				MesosOptions.INITIAL_TASKS.key() + ", which must be at least zero.");
		}

		final int maxFailedTasks = flinkConfig.getInteger(
			MesosOptions.MAX_FAILED_TASKS.key(), numInitialTaskManagers);
		if (maxFailedTasks >= 0) {
			log.info("Mesos framework tolerates {} failed tasks before giving up",
				maxFailedTasks);
		}

		return Props.create(actorClass,
			flinkConfig,
			mesosConfig,
			workerStore,
			leaderRetrievalService,
			taskManagerParameters,
			taskManagerContainerSpec,
			artifactResolver,
			maxFailedTasks,
			numInitialTaskManagers);
	}
}
