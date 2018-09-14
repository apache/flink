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

package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.util.Timeout;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.clusterframework.messages.CheckAndAllocateContainers;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.messages.NewLeaderAvailable;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterInfoMessageListener;
import org.apache.flink.runtime.clusterframework.messages.RegisterInfoMessageListenerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RemoveResource;
import org.apache.flink.runtime.clusterframework.messages.ResourceRemoved;
import org.apache.flink.runtime.clusterframework.messages.SetWorkerPoolSize;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.messages.StopClusterSuccessful;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.messages.UnRegisterInfoMessageListener;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;

import org.apache.flink.util.Preconditions;

import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 *
 * <h1>Worker allocation steps</h1>
 *
 * <ol>
 *     <li>The resource manager decides to request more workers. This can happen in order
 *         to fill the initial pool, or as a result of the JobManager requesting more workers.</li>
 *
 *     <li>The resource master calls {@link #requestNewWorkers(int)}, which triggers requests
 *         for more containers. After that, the {@link #getNumWorkerRequestsPending()}
 *         should reflect the pending requests.</li>
 *
 *     <li>The concrete framework may acquire containers and then trigger to start TaskManagers
 *         in those containers. That should be reflected in {@link #getNumWorkersPendingRegistration()}.</li>
 *
 *     <li>At some point, the TaskManager processes will have started and send a registration
 *         message to the JobManager. The JobManager will perform
 *         a lookup with the ResourceManager to check if it really started this TaskManager.
 *         The method {@link #workerStarted(ResourceID)} will be called
 *         to inform about a registered worker.</li>
 * </ol>
 *
 */
public abstract class FlinkResourceManager<WorkerType extends ResourceIDRetrievable> extends FlinkUntypedActor {

	/** The exit code with which the process is stopped in case of a fatal error */
	protected static final int EXIT_CODE_FATAL_ERROR = -13;

	/** The default name of the resource manager actor */
	public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

	// ------------------------------------------------------------------------

	/** The Flink configuration object */
	protected final Configuration config;

	/** The timeout for actor messages sent to the JobManager / TaskManagers */
	private final FiniteDuration messageTimeout;

	/** The service to find the right leader JobManager (to support high availability) */
	private final LeaderRetrievalService leaderRetriever;

	/** Map which contains the workers from which we know that they have been successfully started
	 * in a container. This notification is sent by the JM when a TM tries to register at it. */
	private final Map<ResourceID, WorkerType> startedWorkers;

	/** List of listeners for info messages */
	private final Set<ActorRef> infoMessageListeners;

	/** The JobManager that the framework master manages resources for */
	private ActorRef jobManager;

	/** Our JobManager's leader session */
	private UUID leaderSessionID;

	/** The size of the worker pool that the resource master strives to maintain */
	private int designatedPoolSize;

	// ------------------------------------------------------------------------

	/**
	 * Creates a AbstractFrameworkMaster actor.
	 *
	 * @param flinkConfig The Flink configuration object.
	 */
	protected FlinkResourceManager(
			int numInitialTaskManagers,
			Configuration flinkConfig,
			LeaderRetrievalService leaderRetriever) {
		this.config = requireNonNull(flinkConfig);
		this.leaderRetriever = requireNonNull(leaderRetriever);
		this.startedWorkers = new HashMap<>();

		FiniteDuration lt;
		try {
			lt = AkkaUtils.getLookupTimeout(config);
		}
		catch (Exception e) {
			lt = new FiniteDuration(
				Duration.apply(AkkaOptions.LOOKUP_TIMEOUT.defaultValue()).toMillis(),
				TimeUnit.MILLISECONDS);
		}
		this.messageTimeout = lt;
		this.designatedPoolSize = numInitialTaskManagers;
		this.infoMessageListeners = new HashSet<>();
	}

	// ------------------------------------------------------------------------
	//  Actor Behavior
	// ------------------------------------------------------------------------

	@Override
	public void preStart() {
		try {
			// we start our leader retrieval service to make sure we get informed
			// about JobManager leader changes
			leaderRetriever.start(new LeaderRetrievalListener() {

				@Override
				public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
					self().tell(
						new NewLeaderAvailable(leaderAddress, leaderSessionID),
						ActorRef.noSender());
				}

				@Override
				public void handleError(Exception e) {
					self().tell(
						new FatalErrorOccurred("Leader retrieval service failed", e),
						ActorRef.noSender());
				}
			});

			// framework specific initialization
			initialize();

		}
		catch (Throwable t) {
			self().tell(
				new FatalErrorOccurred("Error during startup of ResourceManager actor", t),
				ActorRef.noSender());
		}
	}

	@Override
	public void postStop() {
		try {
			leaderRetriever.stop();
		}
		catch (Throwable t) {
			LOG.error("Could not cleanly shut down leader retrieval service", t);
		}
	}

	/**
	 *
	 * This method receives the actor messages after they have been filtered for
	 * a match with the leader session.
	 *
	 * @param message The incoming actor message.
	 */
	@Override
	protected void handleMessage(Object message) {
		try {
			// --- messages about worker allocation and pool sizes

			if (message instanceof CheckAndAllocateContainers) {
				checkWorkersPool();
			}
			else if (message instanceof SetWorkerPoolSize) {
				SetWorkerPoolSize msg = (SetWorkerPoolSize) message;
				adjustDesignatedNumberOfWorkers(msg.numberOfWorkers());
			}
			else if (message instanceof RemoveResource) {
				RemoveResource msg = (RemoveResource) message;
				removeRegisteredResource(msg.resourceId());
			}

			// --- lookup of registered resources

			else if (message instanceof NotifyResourceStarted) {
				NotifyResourceStarted msg = (NotifyResourceStarted) message;
				handleResourceStarted(sender(), msg.getResourceID());
			}

			// --- messages about JobManager leader status and registration

			else if (message instanceof NewLeaderAvailable) {
				NewLeaderAvailable msg = (NewLeaderAvailable) message;
				newJobManagerLeaderAvailable(msg.leaderAddress(), msg.leaderSessionId());
			}
			else if (message instanceof TriggerRegistrationAtJobManager) {
				TriggerRegistrationAtJobManager msg = (TriggerRegistrationAtJobManager) message;
				triggerConnectingToJobManager(msg.jobManagerAddress());
			}
			else if (message instanceof RegisterResourceManagerSuccessful) {
				RegisterResourceManagerSuccessful msg = (RegisterResourceManagerSuccessful) message;
				jobManagerLeaderConnected(msg.jobManager(), msg.currentlyRegisteredTaskManagers());
			}

			// --- end of application

			else if (message instanceof StopCluster) {
				StopCluster msg = (StopCluster) message;
				shutdownCluster(msg.finalStatus(), msg.message());
				sender().tell(decorateMessage(StopClusterSuccessful.getInstance()), ActorRef.noSender());
			}

			// --- miscellaneous messages

			else if (message instanceof RegisterInfoMessageListener) {
				if (jobManager != null) {
					infoMessageListeners.add(sender());
					sender().tell(decorateMessage(
						RegisterInfoMessageListenerSuccessful.get()),
						// answer as the JobManager
						jobManager);
				}
			}

			else if (message instanceof UnRegisterInfoMessageListener) {
				infoMessageListeners.remove(sender());
			}

			else if (message instanceof FatalErrorOccurred) {
				FatalErrorOccurred fatalErrorOccurred = (FatalErrorOccurred) message;
				fatalError(fatalErrorOccurred.message(), fatalErrorOccurred.error());
			}

			// --- unknown messages

			else {
				LOG.error("Discarding unknown message: {}", message);
			}
		}
		catch (Throwable t) {
			// fatal error, needs master recovery
			fatalError("Error processing actor message", t);
		}
	}

	@Override
	protected final UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	// ------------------------------------------------------------------------
	//  Status
	// ------------------------------------------------------------------------

	/**
	 * Gets the current designated worker pool size, meaning the number of workers
	 * that the resource master strives to maintain. The actual number of workers
	 * may be lower (if worker requests are still pending) or higher (if workers have
	 * not yet been released).
	 *
	 * @return The designated worker pool size.
	 */
	public int getDesignatedWorkerPoolSize() {
		return designatedPoolSize;
	}

	/**
	 * Gets the number of currently started TaskManagers.
	 *
	 * @return The number of currently started TaskManagers.
	 */
	public int getNumberOfStartedTaskManagers() {
		return startedWorkers.size();
	}

	/**
	 * Gets the currently registered resources.
	 * @return
	 */
	public Collection<WorkerType> getStartedTaskManagers() {
		return startedWorkers.values();
	}

	/**
	 * Gets the started worker for a given resource ID, if one is available.
	 *
	 * @param resourceId The resource ID for the worker.
	 * @return True if already registered, otherwise false
	 */
	public boolean isStarted(ResourceID resourceId) {
		return startedWorkers.containsKey(resourceId);
	}

	/**
	 * Gets an iterable for all currently started TaskManagers.
	 *
	 * @return All currently started TaskManagers.
	 */
	public Collection<WorkerType> allStartedWorkers() {
		return startedWorkers.values();
	}

	/**
	 * Tells the ResourceManager that a TaskManager had been started in a container with the given
	 * resource id.
	 *
	 * @param jobManager The sender (JobManager) of the message
	 * @param resourceID The resource id of the started TaskManager
	 */
	private void handleResourceStarted(ActorRef jobManager, ResourceID resourceID) {
		if (resourceID != null) {
			// check if resourceID is already registered (TaskManager may send duplicate register messages)
			WorkerType oldWorker = startedWorkers.get(resourceID);
			if (oldWorker != null) {
				LOG.debug("Notification that TaskManager {} had been started was sent before.", resourceID);
			} else {
				WorkerType newWorker = workerStarted(resourceID);

				if (newWorker != null) {
					startedWorkers.put(resourceID, newWorker);
					LOG.info("TaskManager {} has started.", resourceID);
				} else {
					LOG.info("TaskManager {} has not been started by this resource manager.", resourceID);
				}
			}
		}

		// Acknowledge the resource registration
		jobManager.tell(decorateMessage(Acknowledge.get()), self());
	}

	/**
	 * Releases the given resource. Note that this does not automatically shrink
	 * the designated worker pool size.
	 *
	 * @param resourceId The TaskManager's resource id.
	 */
	private void removeRegisteredResource(ResourceID resourceId) {

		WorkerType worker = startedWorkers.remove(resourceId);
		if (worker != null) {
			releaseStartedWorker(worker);
		} else {
			LOG.warn("Resource {} could not be released", resourceId);
		}
	}


	// ------------------------------------------------------------------------
	//  Registration and consolidation with JobManager Leader
	// ------------------------------------------------------------------------

	/**
	 * Called as soon as we discover (via leader election) that a JobManager lost leadership
	 * or a different one gained leadership.
	 *
	 * @param leaderAddress The address (Akka URL) of the new leader. Null if there is currently no leader.
	 * @param leaderSessionID The unique session ID marking the leadership session.
	 */
	private void newJobManagerLeaderAvailable(String leaderAddress, UUID leaderSessionID) {
		LOG.debug("Received new leading JobManager {}. Connecting.", leaderAddress);

		// disconnect from the current leader (no-op if no leader yet)
		jobManagerLostLeadership();

		// a null leader session id means that only a leader disconnect
		// happened, without a new leader yet
		if (leaderSessionID != null && leaderAddress != null) {
			// the leaderSessionID implicitly filters out success and failure messages
			// that come after leadership changed again
			this.leaderSessionID = leaderSessionID;
			triggerConnectingToJobManager(leaderAddress);
		}
	}

	/**
	 * Causes the resource manager to announce itself at the new leader JobManager and
	 * obtains its connection information and currently known TaskManagers.
	 *
	 * @param leaderAddress The akka actor URL of the new leader JobManager.
	 */
	protected void triggerConnectingToJobManager(String leaderAddress) {

		LOG.info("Trying to associate with JobManager leader " + leaderAddress);

		final Object registerMessage = decorateMessage(new RegisterResourceManager(self()));
		final Object retryMessage = decorateMessage(new TriggerRegistrationAtJobManager(leaderAddress));

		// send the registration message to the JobManager
		ActorSelection jobManagerSel = context().actorSelection(leaderAddress);
		Future<Object> future = Patterns.ask(jobManagerSel, registerMessage, new Timeout(messageTimeout));

		future.onComplete(new OnComplete<Object>() {

			@Override
			public void onComplete(Throwable failure, Object msg) {
				// only process if we haven't been connected in the meantime
				if (jobManager == null) {
					if (msg != null) {
						if (msg instanceof LeaderSessionMessage &&
							((LeaderSessionMessage) msg).message() instanceof RegisterResourceManagerSuccessful) {
							self().tell(msg, ActorRef.noSender());
						} else {
							LOG.error("Invalid response type to registration at JobManager: {}", msg);
							self().tell(retryMessage, ActorRef.noSender());
						}
					} else {
						// no success
						LOG.error("Resource manager could not register at JobManager", failure);
						self().tell(retryMessage, ActorRef.noSender());
					}
				}
			}

		}, context().dispatcher());
	}

	/**
	 * This method disassociates from the current leader JobManager.
	 */
	private void jobManagerLostLeadership() {
		if (jobManager != null) {
			LOG.info("Associated JobManager {} lost leader status", jobManager);

			jobManager = null;
			leaderSessionID = null;

			infoMessageListeners.clear();
		}
	}

	/**
	 * Callback when we're informed about a new leading JobManager.
	 * @param newJobManagerLeader The ActorRef of the new jobManager
	 * @param workers The existing workers the JobManager has registered.
	 */
	private void jobManagerLeaderConnected(
						ActorRef newJobManagerLeader,
						Collection<ResourceID> workers) {

		if (jobManager == null) {
			LOG.info("Resource Manager associating with leading JobManager {} - leader session {}",
						newJobManagerLeader, leaderSessionID);

			jobManager = newJobManagerLeader;

			if (workers.size() > 0) {
				LOG.info("Received TaskManagers that were registered at the leader JobManager. " +
						"Trying to consolidate.");

				// keep track of which TaskManagers are not handled
				Set<ResourceID> toHandle = new HashSet<>(workers.size());
				toHandle.addAll(workers);

				try {
					// ask the framework to tell us which ones we should keep for now
					Collection<WorkerType> consolidated = reacceptRegisteredWorkers(workers);
					LOG.info("Consolidated {} TaskManagers", consolidated.size());

					// put the consolidated TaskManagers into our bookkeeping
					for (WorkerType worker : consolidated) {
						ResourceID resourceID = worker.getResourceID();
						startedWorkers.put(resourceID, worker);
						toHandle.remove(resourceID);
					}
				}
				catch (Throwable t) {
					LOG.error("Error during consolidation of known TaskManagers", t);
					// the framework should release the remaining unclear resources
					for (ResourceID id : toHandle) {
						releasePendingWorker(id);
					}
				}

			}

			// trigger initial check for requesting new workers
			checkWorkersPool();

		} else {
			String msg = "Attempting to associate with new JobManager leader " + newJobManagerLeader
				+ " without previously disassociating from current leader " + jobManager;
			fatalError(msg, new Exception(msg));
		}
	}

	// ------------------------------------------------------------------------
	//  ClusterClient Shutdown
	// ------------------------------------------------------------------------

	private void shutdownCluster(ApplicationStatus status, String diagnostics) {
		LOG.info("Shutting down cluster with status {} : {}", status, diagnostics);

		shutdownApplication(status, diagnostics);
	}

	// ------------------------------------------------------------------------
	//  Worker pool size management
	// ------------------------------------------------------------------------

	/**
	 * This method causes the resource framework master to <b>synchronously</b>re-examine
	 * the set of available and pending workers containers, and allocate containers
	 * if needed.
	 *
	 * This method does not automatically release workers, because it is not visible to
	 * this resource master which workers can be released. Instead, the JobManager must
	 * explicitly release individual workers.
	 */
	private void checkWorkersPool() {
		int numWorkersPending = getNumWorkerRequestsPending();
		int numWorkersPendingRegistration = getNumWorkersPendingRegistration();

		// sanity checks
		Preconditions.checkState(numWorkersPending >= 0,
			"Number of pending workers should never be below 0.");
		Preconditions.checkState(numWorkersPendingRegistration >= 0,
			"Number of pending workers pending registration should never be below 0.");

		// see how many workers we want, and whether we have enough
		int allAvailableAndPending = startedWorkers.size() +
			numWorkersPending + numWorkersPendingRegistration;

		int missing = designatedPoolSize - allAvailableAndPending;

		if (missing > 0) {
			requestNewWorkers(missing);
		}
	}

	/**
	 * Sets the designated worker pool size. If this size is larger than the current pool
	 * size, then the resource manager will try to acquire more TaskManagers.
	 *
	 * @param num The number of workers in the pool.
	 */
	private void adjustDesignatedNumberOfWorkers(int num) {
		if (num >= 0) {
			LOG.info("Adjusting designated worker pool size to {}", num);
			designatedPoolSize = num;
			checkWorkersPool();
		} else {
			LOG.warn("Ignoring invalid designated worker pool size: " + num);
		}
	}

	// ------------------------------------------------------------------------
	//  Callbacks
	// ------------------------------------------------------------------------

	/**
	 * This method causes the resource framework master to <b>asynchronously</b>re-examine
	 * the set of available and pending workers containers, and release or allocate
	 * containers if needed. The method sends an actor message which will trigger the
	 * re-examination.
	 */
	public void triggerCheckWorkers() {
		self().tell(
			decorateMessage(
				CheckAndAllocateContainers.get()),
			ActorRef.noSender());
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * worker has failed.
	 *
	 * @param resourceID Id of the worker that has failed.
	 * @param message An informational message that explains why the worker failed.
	 */
	public void notifyWorkerFailed(ResourceID resourceID, String message) {
		WorkerType worker = startedWorkers.remove(resourceID);
		if (worker != null) {
			jobManager.tell(
				decorateMessage(
					new ResourceRemoved(resourceID, message)),
				self());
		}
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	/**
	 * Initializes the framework specific components.
	 *
	 * @throws Exception Exceptions during initialization cause the resource manager to fail.
	 *                   If the framework is able to recover this resource manager, it will be
	 *                   restarted.
	 */
	protected abstract void initialize() throws Exception;

	/**
	 * The framework specific code for shutting down the application. This should report the
	 * application's final status and shut down the resource manager cleanly.
	 *
	 * This method also needs to make sure all pending containers that are not registered
	 * yet are returned.
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics An optional diagnostics message.
	 */
	protected abstract void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics);

	/**
	 * Notifies the resource master of a fatal error.
	 *
	 * <p><b>IMPORTANT:</b> This should not cleanly shut down this master, but exit it in
	 * such a way that a high-availability setting would restart this or fail over
	 * to another master.
	 */
	protected abstract void fatalError(String message, Throwable error);

	/**
	 * Requests to allocate a certain number of new workers.
	 *
	 * @param numWorkers The number of workers to allocate.
	 */
	protected abstract void requestNewWorkers(int numWorkers);

	/**
	 * Trigger a release of a pending worker.
	 * @param resourceID The worker resource id
	 */
	protected abstract void releasePendingWorker(ResourceID resourceID);

	/**
	 * Trigger a release of a started worker.
	 * @param resourceID The worker resource id
	 */
	protected abstract void releaseStartedWorker(WorkerType resourceID);

	/**
	 * Callback when a worker was started.
	 * @param resourceID The worker resource id
	 */
	protected abstract WorkerType workerStarted(ResourceID resourceID);

	/**
	 * This method is called when the resource manager starts after a failure and reconnects to
	 * the leader JobManager, who still has some workers registered. The method is used to consolidate
	 * the view between resource manager and JobManager. The resource manager gets the list of TaskManagers
	 * that the JobManager considers available and should return a list or nodes that the
	 * resource manager considers available.
	 *
	 * After that, the JobManager is informed of loss of all TaskManagers that are not part of the
	 * returned list.
	 *
	 * It is possible that the resource manager initially confirms some TaskManagers to be alive, even
	 * through they are in an uncertain status, if it later sends necessary failure notifications
	 * via calling {@link #notifyWorkerFailed(ResourceID, String)}.
	 *
	 * @param registered The list of TaskManagers that the JobManager knows.
	 * @return The subset of TaskManagers that the resource manager can confirm to be alive.
	 */
	protected abstract Collection<WorkerType> reacceptRegisteredWorkers(Collection<ResourceID> registered);

	/**
	 * Gets the number of requested workers that have not yet been granted.
	 *
	 * @return The number pending worker requests. Must never be smaller than 0.
	 */
	protected abstract int getNumWorkerRequestsPending();

	/**
	 * Gets the number of containers that have been started, but where the TaskManager
	 * has not yet registered at the job manager.
	 *
	 * @return The number of started containers pending TaskManager registration.
	 * Must never be smaller than 0.
	 */
	protected abstract int getNumWorkersPendingRegistration();

	// ------------------------------------------------------------------------
	//  Info messaging
	// ------------------------------------------------------------------------

	protected void sendInfoMessage(String message) {
		for (ActorRef listener : infoMessageListeners) {
			listener.tell(decorateMessage(new InfoMessage(message)), self());
		}
	}


	// ------------------------------------------------------------------------
	//  Startup
	// ------------------------------------------------------------------------

	/**
	 * Starts the resource manager actors.
	 * @param configuration The configuration for the resource manager
	 * @param actorSystem The actor system to start the resource manager in
	 * @param leaderRetriever The leader retriever service to initialize the resource manager
	 * @param resourceManagerClass The class of the ResourceManager to be started
	 * @return ActorRef of the resource manager
	 */
	public static ActorRef startResourceManagerActors(
			Configuration configuration,
			ActorSystem actorSystem,
			LeaderRetrievalService leaderRetriever,
			Class<? extends FlinkResourceManager<?>> resourceManagerClass) {

		return startResourceManagerActors(
			configuration, actorSystem, leaderRetriever, resourceManagerClass,
			RESOURCE_MANAGER_NAME + "-" + UUID.randomUUID());
	}

	/**
	 * Starts the resource manager actors.
	 * @param configuration The configuration for the resource manager
	 * @param actorSystem The actor system to start the resource manager in
	 * @param leaderRetriever The leader retriever service to initialize the resource manager
	 * @param resourceManagerClass The class of the ResourceManager to be started
	 * @param resourceManagerActorName The name of the resource manager actor.
	 * @return ActorRef of the resource manager
	 */
	public static ActorRef startResourceManagerActors(
			Configuration configuration,
			ActorSystem actorSystem,
			LeaderRetrievalService leaderRetriever,
			Class<? extends FlinkResourceManager<?>> resourceManagerClass,
			String resourceManagerActorName) {

		Props resourceMasterProps = getResourceManagerProps(
			resourceManagerClass,
			configuration,
			leaderRetriever);

		return actorSystem.actorOf(resourceMasterProps, resourceManagerActorName);
	}

	public static Props getResourceManagerProps(
		Class<? extends FlinkResourceManager> resourceManagerClass,
		Configuration configuration,
		LeaderRetrievalService leaderRetrievalService) {

		return Props.create(resourceManagerClass, configuration, leaderRetrievalService);
	}
}
