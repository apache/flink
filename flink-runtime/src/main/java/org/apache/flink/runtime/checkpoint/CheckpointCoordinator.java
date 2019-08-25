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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state.
 * It triggers the checkpoint by sending the messages to the relevant tasks and collects the
 * checkpoint acknowledgements. It also collects and maintains the overview of the state handles
 * reported by the tasks that acknowledge the checkpoint.
 */
public class CheckpointCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	/** The number of recent checkpoints whose IDs are remembered. */
	private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

	// ------------------------------------------------------------------------

	/** Coordinator-wide lock to safeguard the checkpoint updates. */
	private final Object lock = new Object();

	/** Lock specially to make sure that trigger requests do not overtake each other.
	 * This is not done with the coordinator-wide lock, because as part of triggering,
	 * blocking operations may happen (distributed atomic counters).
	 * Using a dedicated lock, we avoid blocking the processing of 'acknowledge/decline'
	 * messages during that phase. */
	private final Object triggerLock = new Object();

	/** The job whose checkpoint this coordinator coordinates. */
	private final JobID job;

	/** Default checkpoint properties. **/
	private final CheckpointProperties checkpointProperties;

	/** The executor used for asynchronous calls, like potentially blocking I/O. */
	private final Executor executor;

	/** Tasks who need to be sent a message when a checkpoint is started. */
	private final ExecutionVertex[] tasksToTrigger;

	/** Tasks who need to acknowledge a checkpoint before it succeeds. */
	private final ExecutionVertex[] tasksToWaitFor;

	/** Tasks who need to be sent a message when a checkpoint is confirmed. */
	private final ExecutionVertex[] tasksToCommitTo;

	/** Map from checkpoint ID to the pending checkpoint. */
	private final Map<Long, PendingCheckpoint> pendingCheckpoints;

	/** Completed checkpoints. Implementations can be blocking. Make sure calls to methods
	 * accessing this don't block the job manager actor and run asynchronously. */
	private final CompletedCheckpointStore completedCheckpointStore;

	/** The root checkpoint state backend, which is responsible for initializing the
	 * checkpoint, storing the metadata, and cleaning up the checkpoint. */
	private final CheckpointStorageCoordinatorView checkpointStorage;

	/** A list of recent checkpoint IDs, to identify late messages (vs invalid ones). */
	private final ArrayDeque<Long> recentPendingCheckpoints;

	/** Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these
	 * need to be ascending across job managers. */
	private final CheckpointIDCounter checkpointIdCounter;

	/** The base checkpoint interval. Actual trigger time may be affected by the
	 * max concurrent checkpoints and minimum-pause values */
	private final long baseInterval;

	/** The max time (in ms) that a checkpoint may take. */
	private final long checkpointTimeout;

	/** The min time(in ns) to delay after a checkpoint could be triggered. Allows to
	 * enforce minimum processing time between checkpoint attempts */
	private final long minPauseBetweenCheckpointsNanos;

	/** The maximum number of checkpoints that may be in progress at the same time. */
	private final int maxConcurrentCheckpointAttempts;

	/** The timer that handles the checkpoint timeouts and triggers periodic checkpoints. */
	private final ScheduledThreadPoolExecutor timer;

	/** The master checkpoint hooks executed by this checkpoint coordinator. */
	private final HashMap<String, MasterTriggerRestoreHook<?>> masterHooks;

	/** Actor that receives status updates from the execution graph this coordinator works for. */
	private JobStatusListener jobStatusListener;

	/** The number of consecutive failed trigger attempts. */
	private final AtomicInteger numUnsuccessfulCheckpointsTriggers = new AtomicInteger(0);

	/** A handle to the current periodic trigger, to cancel it when necessary. */
	private ScheduledFuture<?> currentPeriodicTrigger;

	/** The timestamp (via {@link System#nanoTime()}) when the last checkpoint completed. */
	private long lastCheckpointCompletionNanos;

	/** Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
	 * Non-volatile, because only accessed in synchronized scope */
	private boolean periodicScheduling;

	/** Flag whether a trigger request could not be handled immediately. Non-volatile, because only
	 * accessed in synchronized scope */
	private boolean triggerRequestQueued;

	/** Flag marking the coordinator as shut down (not accepting any messages any more). */
	private volatile boolean shutdown;

	/** Optional tracker for checkpoint statistics. */
	@Nullable
	private CheckpointStatsTracker statsTracker;

	/** A factory for SharedStateRegistry objects. */
	private final SharedStateRegistryFactory sharedStateRegistryFactory;

	/** Registry that tracks state which is shared across (incremental) checkpoints. */
	private SharedStateRegistry sharedStateRegistry;

	private boolean isPreferCheckpointForRecovery;

	private final CheckpointFailureManager failureManager;

	// --------------------------------------------------------------------------------------------

	public CheckpointCoordinator(
			JobID job,
			CheckpointCoordinatorConfiguration chkConfig,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			StateBackend checkpointStateBackend,
			Executor executor,
			SharedStateRegistryFactory sharedStateRegistryFactory,
			CheckpointFailureManager failureManager) {

		// sanity checks
		checkNotNull(checkpointStateBackend);

		// max "in between duration" can be one year - this is to prevent numeric overflows
		long minPauseBetweenCheckpoints = chkConfig.getMinPauseBetweenCheckpoints();
		if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
			minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
		}

		// it does not make sense to schedule checkpoints more often then the desired
		// time between checkpoints
		long baseInterval = chkConfig.getCheckpointInterval();
		if (baseInterval < minPauseBetweenCheckpoints) {
			baseInterval = minPauseBetweenCheckpoints;
		}

		this.job = checkNotNull(job);
		this.baseInterval = baseInterval;
		this.checkpointTimeout = chkConfig.getCheckpointTimeout();
		this.minPauseBetweenCheckpointsNanos = minPauseBetweenCheckpoints * 1_000_000;
		this.maxConcurrentCheckpointAttempts = chkConfig.getMaxConcurrentCheckpoints();
		this.tasksToTrigger = checkNotNull(tasksToTrigger);
		this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
		this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
		this.pendingCheckpoints = new LinkedHashMap<>();
		this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
		this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
		this.executor = checkNotNull(executor);
		this.sharedStateRegistryFactory = checkNotNull(sharedStateRegistryFactory);
		this.sharedStateRegistry = sharedStateRegistryFactory.create(executor);
		this.isPreferCheckpointForRecovery = chkConfig.isPreferCheckpointForRecovery();
		this.failureManager = checkNotNull(failureManager);

		this.recentPendingCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
		this.masterHooks = new HashMap<>();

		this.timer = new ScheduledThreadPoolExecutor(1,
				new DispatcherThreadFactory(Thread.currentThread().getThreadGroup(), "Checkpoint Timer"));

		// make sure the timer internally cleans up and does not hold onto stale scheduled tasks
		this.timer.setRemoveOnCancelPolicy(true);
		this.timer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

		this.checkpointProperties = CheckpointProperties.forCheckpoint(chkConfig.getCheckpointRetentionPolicy());

		try {
			this.checkpointStorage = checkpointStateBackend.createCheckpointStorage(job);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to create checkpoint storage at checkpoint coordinator side.", e);
		}

		try {
			// Make sure the checkpoint ID enumerator is running. Possibly
			// issues a blocking call to ZooKeeper.
			checkpointIDCounter.start();
		} catch (Throwable t) {
			throw new RuntimeException("Failed to start checkpoint ID counter: " + t.getMessage(), t);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * Adds the given master hook to the checkpoint coordinator. This method does nothing, if
	 * the checkpoint coordinator already contained a hook with the same ID (as defined via
	 * {@link MasterTriggerRestoreHook#getIdentifier()}).
	 *
	 * @param hook The hook to add.
	 * @return True, if the hook was added, false if the checkpoint coordinator already
	 *         contained a hook with the same ID.
	 */
	public boolean addMasterHook(MasterTriggerRestoreHook<?> hook) {
		checkNotNull(hook);

		final String id = hook.getIdentifier();
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(id), "The hook has a null or empty id");

		synchronized (lock) {
			if (!masterHooks.containsKey(id)) {
				masterHooks.put(id, hook);
				return true;
			}
			else {
				return false;
			}
		}
	}

	/**
	 * Gets the number of currently register master hooks.
	 */
	public int getNumberOfRegisteredMasterHooks() {
		synchronized (lock) {
			return masterHooks.size();
		}
	}

	/**
	 * Sets the checkpoint stats tracker.
	 *
	 * @param statsTracker The checkpoint stats tracker.
	 */
	public void setCheckpointStatsTracker(@Nullable CheckpointStatsTracker statsTracker) {
		this.statsTracker = statsTracker;
	}

	// --------------------------------------------------------------------------------------------
	//  Clean shutdown
	// --------------------------------------------------------------------------------------------

	/**
	 * Shuts down the checkpoint coordinator.
	 *
	 * <p>After this method has been called, the coordinator does not accept
	 * and further messages and cannot trigger any further checkpoints.
	 */
	public void shutdown(JobStatus jobStatus) throws Exception {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;
				LOG.info("Stopping checkpoint coordinator for job {}.", job);

				periodicScheduling = false;
				triggerRequestQueued = false;

				// shut down the hooks
				MasterHooks.close(masterHooks.values(), LOG);
				masterHooks.clear();

				// shut down the thread that handles the timeouts and pending triggers
				timer.shutdownNow();

				// clear and discard all pending checkpoints
				for (PendingCheckpoint pending : pendingCheckpoints.values()) {
					failPendingCheckpoint(pending, CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
				}
				pendingCheckpoints.clear();

				completedCheckpointStore.shutdown(jobStatus);
				checkpointIdCounter.shutdown(jobStatus);
			}
		}
	}

	public boolean isShutdown() {
		return shutdown;
	}

	// --------------------------------------------------------------------------------------------
	//  Triggering Checkpoints and Savepoints
	// --------------------------------------------------------------------------------------------

	/**
	 * Triggers a savepoint with the given savepoint directory as a target.
	 *
	 * @param timestamp The timestamp for the savepoint.
	 * @param targetLocation Target location for the savepoint, optional. If null, the
	 *                       state backend's configured default will be used.
	 * @return A future to the completed checkpoint
	 * @throws IllegalStateException If no savepoint directory has been
	 *                               specified and no default savepoint directory has been
	 *                               configured
	 */
	public CompletableFuture<CompletedCheckpoint> triggerSavepoint(
			final long timestamp,
			@Nullable final String targetLocation) {

		final CheckpointProperties properties = CheckpointProperties.forSavepoint();
		return triggerSavepointInternal(timestamp, properties, false, targetLocation);
	}

	/**
	 * Triggers a synchronous savepoint with the given savepoint directory as a target.
	 *
	 * @param timestamp The timestamp for the savepoint.
	 * @param advanceToEndOfEventTime Flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 *                              to fire any registered event-time timers.
	 * @param targetLocation Target location for the savepoint, optional. If null, the
	 *                       state backend's configured default will be used.
	 * @return A future to the completed checkpoint
	 * @throws IllegalStateException If no savepoint directory has been
	 *                               specified and no default savepoint directory has been
	 *                               configured
	 */
	public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
			final long timestamp,
			final boolean advanceToEndOfEventTime,
			@Nullable final String targetLocation) {

		final CheckpointProperties properties = CheckpointProperties.forSyncSavepoint();

		return triggerSavepointInternal(timestamp, properties, advanceToEndOfEventTime, targetLocation).handle(
				(completedCheckpoint, throwable) -> {
					if (throwable != null) {
						failureManager.handleSynchronousSavepointFailure(throwable);
						throw new CompletionException(throwable);
					}
					return completedCheckpoint;
				});
	}

	private CompletableFuture<CompletedCheckpoint> triggerSavepointInternal(
			final long timestamp,
			final CheckpointProperties checkpointProperties,
			final boolean advanceToEndOfEventTime,
			@Nullable final String targetLocation) {

		checkNotNull(checkpointProperties);

		try {
			PendingCheckpoint pendingCheckpoint = triggerCheckpoint(
					timestamp,
					checkpointProperties,
					targetLocation,
					false,
					advanceToEndOfEventTime);

			return pendingCheckpoint.getCompletionFuture();
		} catch (CheckpointException e) {
			Throwable cause = new CheckpointException("Failed to trigger savepoint.", e.getCheckpointFailureReason());
			return FutureUtils.completedExceptionally(cause);
		}
	}

	/**
	 * Triggers a new standard checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 *
	 * @param timestamp The timestamp for the checkpoint.
	 * @param isPeriodic Flag indicating whether this triggered checkpoint is
	 * periodic. If this flag is true, but the periodic scheduler is disabled,
	 * the checkpoint will be declined.
	 * @return <code>true</code> if triggering the checkpoint succeeded.
	 */
	public boolean triggerCheckpoint(long timestamp, boolean isPeriodic) {
		try {
			triggerCheckpoint(timestamp, checkpointProperties, null, isPeriodic, false);
			return true;
		} catch (CheckpointException e) {
			long latestGeneratedCheckpointId = getCheckpointIdCounter().get();
			// here we can not get the failed pending checkpoint's id,
			// so we pass the negative latest generated checkpoint id as a special flag
			failureManager.handleJobLevelCheckpointException(e, -1 * latestGeneratedCheckpointId);
			return false;
		}
	}

	@VisibleForTesting
	public PendingCheckpoint triggerCheckpoint(
			long timestamp,
			CheckpointProperties props,
			@Nullable String externalSavepointLocation,
			boolean isPeriodic,
			boolean advanceToEndOfTime) throws CheckpointException {

		if (advanceToEndOfTime && !(props.isSynchronous() && props.isSavepoint())) {
			throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
		}

		// make some eager pre-checks
		synchronized (lock) {
			// abort if the coordinator has been shutdown in the meantime
			if (shutdown) {
				throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
			}

			// Don't allow periodic checkpoint if scheduling has been disabled
			if (isPeriodic && !periodicScheduling) {
				throw new CheckpointException(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
			}

			// validate whether the checkpoint can be triggered, with respect to the limit of
			// concurrent checkpoints, and the minimum time between checkpoints.
			// these checks are not relevant for savepoints
			if (!props.forceCheckpoint()) {
				// sanity check: there should never be more than one trigger request queued
				if (triggerRequestQueued) {
					LOG.warn("Trying to trigger another checkpoint for job {} while one was queued already.", job);
					throw new CheckpointException(CheckpointFailureReason.ALREADY_QUEUED);
				}

				checkConcurrentCheckpoints();

				checkMinPauseBetweenCheckpoints();
			}
		}

		// check if all tasks that we need to trigger are running.
		// if not, abort the checkpoint
		Execution[] executions = new Execution[tasksToTrigger.length];
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee == null) {
				LOG.info("Checkpoint triggering task {} of job {} is not being executed at the moment. Aborting checkpoint.",
						tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
						job);
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			} else if (ee.getState() == ExecutionState.RUNNING) {
				executions[i] = ee;
			} else {
				LOG.info("Checkpoint triggering task {} of job {} is not in state {} but {} instead. Aborting checkpoint.",
						tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
						job,
						ExecutionState.RUNNING,
						ee.getState());
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}

		// next, check if all tasks that need to acknowledge the checkpoint are running.
		// if not, abort the checkpoint
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(tasksToWaitFor.length);

		for (ExecutionVertex ev : tasksToWaitFor) {
			Execution ee = ev.getCurrentExecutionAttempt();
			if (ee != null) {
				ackTasks.put(ee.getAttemptId(), ev);
			} else {
				LOG.info("Checkpoint acknowledging task {} of job {} is not being executed at the moment. Aborting checkpoint.",
						ev.getTaskNameWithSubtaskIndex(),
						job);
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}

		// we will actually trigger this checkpoint!

		// we lock with a special lock to make sure that trigger requests do not overtake each other.
		// this is not done with the coordinator-wide lock, because the 'checkpointIdCounter'
		// may issue blocking operations. Using a different lock than the coordinator-wide lock,
		// we avoid blocking the processing of 'acknowledge/decline' messages during that time.
		synchronized (triggerLock) {

			final CheckpointStorageLocation checkpointStorageLocation;
			final long checkpointID;

			try {
				// this must happen outside the coordinator-wide lock, because it communicates
				// with external services (in HA mode) and may block for a while.
				checkpointID = checkpointIdCounter.getAndIncrement();

				checkpointStorageLocation = props.isSavepoint() ?
						checkpointStorage.initializeLocationForSavepoint(checkpointID, externalSavepointLocation) :
						checkpointStorage.initializeLocationForCheckpoint(checkpointID);
			}
			catch (Throwable t) {
				int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
				LOG.warn("Failed to trigger checkpoint for job {} ({} consecutive failed attempts so far).",
						job,
						numUnsuccessful,
						t);
				throw new CheckpointException(CheckpointFailureReason.EXCEPTION, t);
			}

			final PendingCheckpoint checkpoint = new PendingCheckpoint(
				job,
				checkpointID,
				timestamp,
				ackTasks,
				props,
				checkpointStorageLocation,
				executor);

			if (statsTracker != null) {
				PendingCheckpointStats callback = statsTracker.reportPendingCheckpoint(
					checkpointID,
					timestamp,
					props);

				checkpoint.setStatsCallback(callback);
			}

			// schedule the timer that will clean up the expired checkpoints
			final Runnable canceller = () -> {
				synchronized (lock) {
					// only do the work if the checkpoint is not discarded anyways
					// note that checkpoint completion discards the pending checkpoint object
					if (!checkpoint.isDiscarded()) {
						LOG.info("Checkpoint {} of job {} expired before completing.", checkpointID, job);

						failPendingCheckpoint(checkpoint, CheckpointFailureReason.CHECKPOINT_EXPIRED);
						pendingCheckpoints.remove(checkpointID);
						rememberRecentCheckpointId(checkpointID);

						triggerQueuedRequests();
					}
				}
			};

			try {
				// re-acquire the coordinator-wide lock
				synchronized (lock) {
					// since we released the lock in the meantime, we need to re-check
					// that the conditions still hold.
					if (shutdown) {
						throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
					}
					else if (!props.forceCheckpoint()) {
						if (triggerRequestQueued) {
							LOG.warn("Trying to trigger another checkpoint for job {} while one was queued already.", job);
							throw new CheckpointException(CheckpointFailureReason.ALREADY_QUEUED);
						}

						checkConcurrentCheckpoints();

						checkMinPauseBetweenCheckpoints();
					}

					LOG.info("Triggering checkpoint {} @ {} for job {}.", checkpointID, timestamp, job);

					pendingCheckpoints.put(checkpointID, checkpoint);

					ScheduledFuture<?> cancellerHandle = timer.schedule(
							canceller,
							checkpointTimeout, TimeUnit.MILLISECONDS);

					if (!checkpoint.setCancellerHandle(cancellerHandle)) {
						// checkpoint is already disposed!
						cancellerHandle.cancel(false);
					}

					// trigger the master hooks for the checkpoint
					final List<MasterState> masterStates = MasterHooks.triggerMasterHooks(masterHooks.values(),
							checkpointID, timestamp, executor, Time.milliseconds(checkpointTimeout));
					for (MasterState s : masterStates) {
						checkpoint.addMasterState(s);
					}
				}
				// end of lock scope

				final CheckpointOptions checkpointOptions = new CheckpointOptions(
						props.getCheckpointType(),
						checkpointStorageLocation.getLocationReference());

				// send the messages to the tasks that trigger their checkpoint
				for (Execution execution: executions) {
					if (props.isSynchronous()) {
						execution.triggerSynchronousSavepoint(checkpointID, timestamp, checkpointOptions, advanceToEndOfTime);
					} else {
						execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);
					}
				}

				numUnsuccessfulCheckpointsTriggers.set(0);
				return checkpoint;
			}
			catch (Throwable t) {
				// guard the map against concurrent modifications
				synchronized (lock) {
					pendingCheckpoints.remove(checkpointID);
				}

				int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
				LOG.warn("Failed to trigger checkpoint {} for job {}. ({} consecutive failed attempts so far)",
						checkpointID, job, numUnsuccessful, t);

				if (!checkpoint.isDiscarded()) {
					failPendingCheckpoint(checkpoint, CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, t);
				}

				try {
					checkpointStorageLocation.disposeOnFailure();
				}
				catch (Throwable t2) {
					LOG.warn("Cannot dispose failed checkpoint storage location {}", checkpointStorageLocation, t2);
				}

				throw new CheckpointException(CheckpointFailureReason.EXCEPTION, t);
			}

		} // end trigger lock
	}

	// --------------------------------------------------------------------------------------------
	//  Handling checkpoints and messages
	// --------------------------------------------------------------------------------------------

	/**
	 * Receives a {@link DeclineCheckpoint} message for a pending checkpoint.
	 *
	 * @param message Checkpoint decline from the task manager
	 * @param taskManagerLocationInfo The location info of the decline checkpoint message's sender
	 */
	public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
		if (shutdown || message == null) {
			return;
		}

		if (!job.equals(message.getJob())) {
			throw new IllegalArgumentException("Received DeclineCheckpoint message for job " +
				message.getJob() + " from " + taskManagerLocationInfo + " while this coordinator handles job " + job);
		}

		final long checkpointId = message.getCheckpointId();
		final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");

		PendingCheckpoint checkpoint;

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return;
			}

			checkpoint = pendingCheckpoints.remove(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				LOG.info("Decline checkpoint {} by task {} of job {} at {}.",
					checkpointId,
					message.getTaskExecutionId(),
					job,
					taskManagerLocationInfo);
				discardCheckpoint(checkpoint, message.getReason(), message.getTaskExecutionId());
			}
			else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
						"Received message for discarded but non-removed checkpoint " + checkpointId);
			}
			else if (LOG.isDebugEnabled()) {
				if (recentPendingCheckpoints.contains(checkpointId)) {
					// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
					LOG.debug("Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
							checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
				} else {
					// message is for an unknown checkpoint. might be so old that we don't even remember it any more
					LOG.debug("Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
							checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
				}
			}
		}
	}

	/**
	 * Receives an AcknowledgeCheckpoint message and returns whether the
	 * message was associated with a pending checkpoint.
	 *
	 * @param message Checkpoint ack from the task manager
	 *
	 * @param taskManagerLocationInfo The location of the acknowledge checkpoint message's sender
	 * @return Flag indicating whether the ack'd checkpoint was associated
	 * with a pending checkpoint.
	 *
	 * @throws CheckpointException If the checkpoint cannot be added to the completed checkpoint store.
	 */
	public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message, String taskManagerLocationInfo) throws CheckpointException {
		if (shutdown || message == null) {
			return false;
		}

		if (!job.equals(message.getJob())) {
			LOG.error("Received wrong AcknowledgeCheckpoint message for job {} from {} : {}", job, taskManagerLocationInfo, message);
			return false;
		}

		final long checkpointId = message.getCheckpointId();

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return false;
			}

			final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {

				switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getSubtaskState(), message.getCheckpointMetrics())) {
					case SUCCESS:
						LOG.debug("Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
							checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

						if (checkpoint.isFullyAcknowledged()) {
							completePendingCheckpoint(checkpoint);
						}
						break;
					case DUPLICATE:
						LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
						break;
					case UNKNOWN:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
								"because the task's execution attempt id was unknown. Discarding " +
								"the state handle to avoid lingering state.", message.getCheckpointId(),
							message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

						discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

						break;
					case DISCARDED:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
							"because the pending checkpoint had been discarded. Discarding the " +
								"state handle tp avoid lingering state.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

						discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());
				}

				return true;
			}
			else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
						"Received message for discarded but non-removed checkpoint " + checkpointId);
			}
			else {
				boolean wasPendingCheckpoint;

				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				if (recentPendingCheckpoints.contains(checkpointId)) {
					wasPendingCheckpoint = true;
					LOG.warn("Received late message for now expired checkpoint attempt {} from task " +
						"{} of job {} at {}.", checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
				}
				else {
					LOG.debug("Received message for an unknown checkpoint {} from task {} of job {} at {}.",
						checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
					wasPendingCheckpoint = false;
				}

				// try to discard the state so that we don't have lingering state lying around
				discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

				return wasPendingCheckpoint;
			}
		}
	}

	/**
	 * Try to complete the given pending checkpoint.
	 *
	 * <p>Important: This method should only be called in the checkpoint lock scope.
	 *
	 * @param pendingCheckpoint to complete
	 * @throws CheckpointException if the completion failed
	 */
	private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException {
		final long checkpointId = pendingCheckpoint.getCheckpointId();
		final CompletedCheckpoint completedCheckpoint;

		// As a first step to complete the checkpoint, we register its state with the registry
		Map<OperatorID, OperatorState> operatorStates = pendingCheckpoint.getOperatorStates();
		sharedStateRegistry.registerAll(operatorStates.values());

		try {
			try {
				completedCheckpoint = pendingCheckpoint.finalizeCheckpoint();
				failureManager.handleCheckpointSuccess(pendingCheckpoint.getCheckpointId());
			}
			catch (Exception e1) {
				// abort the current pending checkpoint if we fails to finalize the pending checkpoint.
				if (!pendingCheckpoint.isDiscarded()) {
					failPendingCheckpoint(pendingCheckpoint, CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1);
				}

				throw new CheckpointException("Could not finalize the pending checkpoint " + checkpointId + '.',
					CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1);
			}

			// the pending checkpoint must be discarded after the finalization
			Preconditions.checkState(pendingCheckpoint.isDiscarded() && completedCheckpoint != null);

			try {
				completedCheckpointStore.addCheckpoint(completedCheckpoint);
			} catch (Exception exception) {
				// we failed to store the completed checkpoint. Let's clean up
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							completedCheckpoint.discardOnFailedStoring();
						} catch (Throwable t) {
							LOG.warn("Could not properly discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), t);
						}
					}
				});

				throw new CheckpointException("Could not complete the pending checkpoint " + checkpointId + '.',
					CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, exception);
			}
		} finally {
			pendingCheckpoints.remove(checkpointId);

			triggerQueuedRequests();
		}

		rememberRecentCheckpointId(checkpointId);

		// drop those pending checkpoints that are at prior to the completed one
		dropSubsumedCheckpoints(checkpointId);

		// record the time when this was completed, to calculate
		// the 'min delay between checkpoints'
		lastCheckpointCompletionNanos = System.nanoTime();

		LOG.info("Completed checkpoint {} for job {} ({} bytes in {} ms).", checkpointId, job,
			completedCheckpoint.getStateSize(), completedCheckpoint.getDuration());

		if (LOG.isDebugEnabled()) {
			StringBuilder builder = new StringBuilder();
			builder.append("Checkpoint state: ");
			for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
				builder.append(state);
				builder.append(", ");
			}
			// Remove last two chars ", "
			builder.setLength(builder.length() - 2);

			LOG.debug(builder.toString());
		}

		// send the "notify complete" call to all vertices
		final long timestamp = completedCheckpoint.getTimestamp();

		for (ExecutionVertex ev : tasksToCommitTo) {
			Execution ee = ev.getCurrentExecutionAttempt();
			if (ee != null) {
				ee.notifyCheckpointComplete(checkpointId, timestamp);
			}
		}
	}

	/**
	 * Fails all pending checkpoints which have not been acknowledged by the given execution
	 * attempt id.
	 *
	 * @param executionAttemptId for which to discard unacknowledged pending checkpoints
	 * @param cause of the failure
	 */
	public void failUnacknowledgedPendingCheckpointsFor(ExecutionAttemptID executionAttemptId, Throwable cause) {
		synchronized (lock) {
			Iterator<PendingCheckpoint> pendingCheckpointIterator = pendingCheckpoints.values().iterator();

			while (pendingCheckpointIterator.hasNext()) {
				final PendingCheckpoint pendingCheckpoint = pendingCheckpointIterator.next();

				if (!pendingCheckpoint.isAcknowledgedBy(executionAttemptId)) {
					pendingCheckpointIterator.remove();
					discardCheckpoint(pendingCheckpoint, cause, executionAttemptId);
				}
			}
		}
	}

	private void rememberRecentCheckpointId(long id) {
		if (recentPendingCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
			recentPendingCheckpoints.removeFirst();
		}
		recentPendingCheckpoints.addLast(id);
	}

	private void dropSubsumedCheckpoints(long checkpointId) {
		Iterator<Map.Entry<Long, PendingCheckpoint>> entries = pendingCheckpoints.entrySet().iterator();

		while (entries.hasNext()) {
			PendingCheckpoint p = entries.next().getValue();
			// remove all pending checkpoints that are lesser than the current completed checkpoint
			if (p.getCheckpointId() < checkpointId && p.canBeSubsumed()) {
				rememberRecentCheckpointId(p.getCheckpointId());
				failPendingCheckpoint(p, CheckpointFailureReason.CHECKPOINT_SUBSUMED);
				entries.remove();
			}
		}
	}

	/**
	 * Triggers the queued request, if there is one.
	 *
	 * <p>NOTE: The caller of this method must hold the lock when invoking the method!
	 */
	private void triggerQueuedRequests() {
		if (triggerRequestQueued) {
			triggerRequestQueued = false;

			// trigger the checkpoint from the trigger timer, to finish the work of this thread before
			// starting with the next checkpoint
			if (periodicScheduling) {
				if (currentPeriodicTrigger != null) {
					currentPeriodicTrigger.cancel(false);
				}
				currentPeriodicTrigger = scheduleTriggerWithDelay(0L);
			}
			else {
				timer.execute(new ScheduledTrigger());
			}
		}
	}

	@VisibleForTesting
	int getNumScheduledTasks() {
		return timer.getQueue().size();
	}

	// --------------------------------------------------------------------------------------------
	//  Checkpoint State Restoring
	// --------------------------------------------------------------------------------------------

	/**
	 * Restores the latest checkpointed state.
	 *
	 * @param tasks Map of job vertices to restore. State for these vertices is
	 * restored via {@link Execution#setInitialState(JobManagerTaskRestore)}.
	 * @param errorIfNoCheckpoint Fail if no completed checkpoint is available to
	 * restore from.
	 * @param allowNonRestoredState Allow checkpoint state that cannot be mapped
	 * to any job vertex in tasks.
	 * @return <code>true</code> if state was restored, <code>false</code> otherwise.
	 * @throws IllegalStateException If the CheckpointCoordinator is shut down.
	 * @throws IllegalStateException If no completed checkpoint is available and
	 *                               the <code>failIfNoCheckpoint</code> flag has been set.
	 * @throws IllegalStateException If the checkpoint contains state that cannot be
	 *                               mapped to any job vertex in <code>tasks</code> and the
	 *                               <code>allowNonRestoredState</code> flag has not been set.
	 * @throws IllegalStateException If the max parallelism changed for an operator
	 *                               that restores state from this checkpoint.
	 * @throws IllegalStateException If the parallelism changed for an operator
	 *                               that restores <i>non-partitioned</i> state from this
	 *                               checkpoint.
	 */
	public boolean restoreLatestCheckpointedState(
			Map<JobVertexID, ExecutionJobVertex> tasks,
			boolean errorIfNoCheckpoint,
			boolean allowNonRestoredState) throws Exception {

		synchronized (lock) {
			if (shutdown) {
				throw new IllegalStateException("CheckpointCoordinator is shut down");
			}

			// We create a new shared state registry object, so that all pending async disposal requests from previous
			// runs will go against the old object (were they can do no harm).
			// This must happen under the checkpoint lock.
			sharedStateRegistry.close();
			sharedStateRegistry = sharedStateRegistryFactory.create(executor);

			// Recover the checkpoints, TODO this could be done only when there is a new leader, not on each recovery
			completedCheckpointStore.recover();

			// Now, we re-register all (shared) states from the checkpoint store with the new registry
			for (CompletedCheckpoint completedCheckpoint : completedCheckpointStore.getAllCheckpoints()) {
				completedCheckpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
			}

			LOG.debug("Status of the shared state registry of job {} after restore: {}.", job, sharedStateRegistry);

			// Restore from the latest checkpoint
			CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint(isPreferCheckpointForRecovery);

			if (latest == null) {
				if (errorIfNoCheckpoint) {
					throw new IllegalStateException("No completed checkpoint available");
				} else {
					LOG.debug("Resetting the master hooks.");
					MasterHooks.reset(masterHooks.values(), LOG);

					return false;
				}
			}

			LOG.info("Restoring job {} from latest valid checkpoint: {}.", job, latest);

			// re-assign the task states
			final Map<OperatorID, OperatorState> operatorStates = latest.getOperatorStates();

			StateAssignmentOperation stateAssignmentOperation =
					new StateAssignmentOperation(latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState);

			stateAssignmentOperation.assignStates();

			// call master hooks for restore

			MasterHooks.restoreMasterHooks(
					masterHooks,
					latest.getMasterHookStates(),
					latest.getCheckpointID(),
					allowNonRestoredState,
					LOG);

			// update metrics

			if (statsTracker != null) {
				long restoreTimestamp = System.currentTimeMillis();
				RestoredCheckpointStats restored = new RestoredCheckpointStats(
					latest.getCheckpointID(),
					latest.getProperties(),
					restoreTimestamp,
					latest.getExternalPointer());

				statsTracker.reportRestoredCheckpoint(restored);
			}

			return true;
		}
	}

	/**
	 * Restore the state with given savepoint.
	 *
	 * @param savepointPointer The pointer to the savepoint.
	 * @param allowNonRestored True if allowing checkpoint state that cannot be
	 *                         mapped to any job vertex in tasks.
	 * @param tasks            Map of job vertices to restore. State for these
	 *                         vertices is restored via
	 *                         {@link Execution#setInitialState(JobManagerTaskRestore)}.
	 * @param userClassLoader  The class loader to resolve serialized classes in
	 *                         legacy savepoint versions.
	 */
	public boolean restoreSavepoint(
			String savepointPointer,
			boolean allowNonRestored,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			ClassLoader userClassLoader) throws Exception {

		Preconditions.checkNotNull(savepointPointer, "The savepoint path cannot be null.");

		LOG.info("Starting job {} from savepoint {} ({})",
				job, savepointPointer, (allowNonRestored ? "allowing non restored state" : ""));

		final CompletedCheckpointStorageLocation checkpointLocation = checkpointStorage.resolveCheckpoint(savepointPointer);

		// Load the savepoint as a checkpoint into the system
		CompletedCheckpoint savepoint = Checkpoints.loadAndValidateCheckpoint(
				job, tasks, checkpointLocation, userClassLoader, allowNonRestored);

		completedCheckpointStore.addCheckpoint(savepoint);

		// Reset the checkpoint ID counter
		long nextCheckpointId = savepoint.getCheckpointID() + 1;
		checkpointIdCounter.setCount(nextCheckpointId);

		LOG.info("Reset the checkpoint ID of job {} to {}.", job, nextCheckpointId);

		return restoreLatestCheckpointedState(tasks, true, allowNonRestored);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public int getNumberOfPendingCheckpoints() {
		return this.pendingCheckpoints.size();
	}

	public int getNumberOfRetainedSuccessfulCheckpoints() {
		synchronized (lock) {
			return completedCheckpointStore.getNumberOfRetainedCheckpoints();
		}
	}

	public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
		synchronized (lock) {
			return new HashMap<>(this.pendingCheckpoints);
		}
	}

	public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
		synchronized (lock) {
			return completedCheckpointStore.getAllCheckpoints();
		}
	}

	public CheckpointStorageCoordinatorView getCheckpointStorage() {
		return checkpointStorage;
	}

	public CompletedCheckpointStore getCheckpointStore() {
		return completedCheckpointStore;
	}

	public CheckpointIDCounter getCheckpointIdCounter() {
		return checkpointIdCounter;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	@VisibleForTesting
	boolean isCurrentPeriodicTriggerAvailable() {
		return currentPeriodicTrigger != null;
	}

	/**
	 * Returns whether periodic checkpointing has been configured.
	 *
	 * @return <code>true</code> if periodic checkpoints have been configured.
	 */
	public boolean isPeriodicCheckpointingConfigured() {
		return baseInterval != Long.MAX_VALUE;
	}

	// --------------------------------------------------------------------------------------------
	//  Periodic scheduling of checkpoints
	// --------------------------------------------------------------------------------------------

	public void startCheckpointScheduler() {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			// make sure all prior timers are cancelled
			stopCheckpointScheduler();

			periodicScheduling = true;
			currentPeriodicTrigger = scheduleTriggerWithDelay(getRandomInitDelay());
		}
	}

	public void stopCheckpointScheduler() {
		synchronized (lock) {
			triggerRequestQueued = false;
			periodicScheduling = false;

			if (currentPeriodicTrigger != null) {
				currentPeriodicTrigger.cancel(false);
				currentPeriodicTrigger = null;
			}

			abortPendingCheckpoints(new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND));

			numUnsuccessfulCheckpointsTriggers.set(0);
		}
	}

	/**
	 * Aborts all the pending checkpoints due to en exception.
	 * @param exception The exception.
	 */
	public void abortPendingCheckpoints(CheckpointException exception) {
		synchronized (lock) {
			for (PendingCheckpoint p : pendingCheckpoints.values()) {
				failPendingCheckpoint(p, exception.getCheckpointFailureReason());
			}

			pendingCheckpoints.clear();
		}
	}

	/**
	 * If too many checkpoints are currently in progress, we need to mark that a request is queued
	 *
	 * @throws CheckpointException If too many checkpoints are currently in progress.
	 */
	private void checkConcurrentCheckpoints() throws CheckpointException {
		if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
			triggerRequestQueued = true;
			if (currentPeriodicTrigger != null) {
				currentPeriodicTrigger.cancel(false);
				currentPeriodicTrigger = null;
			}
			throw new CheckpointException(CheckpointFailureReason.TOO_MANY_CONCURRENT_CHECKPOINTS);
		}
	}

	/**
	 * Make sure the minimum interval between checkpoints has passed
	 *
	 * @throws CheckpointException If the minimum interval between checkpoints has not passed.
	 */
	private void checkMinPauseBetweenCheckpoints() throws CheckpointException {
		final long earliestNext = lastCheckpointCompletionNanos + minPauseBetweenCheckpointsNanos;
		final long durationTillNextMillis = (earliestNext - System.nanoTime()) / 1_000_000;

		if (durationTillNextMillis > 0) {
			if (currentPeriodicTrigger != null) {
				currentPeriodicTrigger.cancel(false);
				currentPeriodicTrigger = null;
			}
			// Reassign the new trigger to the currentPeriodicTrigger
			currentPeriodicTrigger = scheduleTriggerWithDelay(durationTillNextMillis);

			throw new CheckpointException(CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS);
		}
	}

	private long getRandomInitDelay() {
		return ThreadLocalRandom.current().nextLong(
			minPauseBetweenCheckpointsNanos / 1_000_000L, baseInterval + 1L);
	}

	private ScheduledFuture<?> scheduleTriggerWithDelay(long initDelay) {
		return timer.scheduleAtFixedRate(
			new ScheduledTrigger(),
			initDelay, baseInterval, TimeUnit.MILLISECONDS);
	}

	// ------------------------------------------------------------------------
	//  job status listener that schedules / cancels periodic checkpoints
	// ------------------------------------------------------------------------

	public JobStatusListener createActivatorDeactivator() {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			if (jobStatusListener == null) {
				jobStatusListener = new CheckpointCoordinatorDeActivator(this);
			}

			return jobStatusListener;
		}
	}

	// ------------------------------------------------------------------------

	private final class ScheduledTrigger implements Runnable {

		@Override
		public void run() {
			try {
				triggerCheckpoint(System.currentTimeMillis(), true);
			}
			catch (Exception e) {
				LOG.error("Exception while triggering checkpoint for job {}.", job, e);
			}
		}
	}

	/**
	 * Discards the given pending checkpoint because of the given cause.
	 *
	 * @param pendingCheckpoint to discard
	 * @param cause for discarding the checkpoint
	 * @param executionAttemptID the execution attempt id of the failing task.
	 */
	private void discardCheckpoint(
		PendingCheckpoint pendingCheckpoint,
		@Nullable Throwable cause,
		ExecutionAttemptID executionAttemptID) {
		assert(Thread.holdsLock(lock));
		Preconditions.checkNotNull(pendingCheckpoint);

		final long checkpointId = pendingCheckpoint.getCheckpointId();

		LOG.info("Discarding checkpoint {} of job {}.", checkpointId, job, cause);

		if (cause == null) {
			failPendingCheckpointDueToTaskFailure(pendingCheckpoint, CheckpointFailureReason.CHECKPOINT_DECLINED, executionAttemptID);
		} else if (cause instanceof CheckpointException) {
			CheckpointException exception = (CheckpointException) cause;
			failPendingCheckpointDueToTaskFailure(pendingCheckpoint, exception.getCheckpointFailureReason(), cause, executionAttemptID);
		} else {
			failPendingCheckpointDueToTaskFailure(pendingCheckpoint, CheckpointFailureReason.JOB_FAILURE, cause, executionAttemptID);
		}

		rememberRecentCheckpointId(checkpointId);

		// we don't have to schedule another "dissolving" checkpoint any more because the
		// cancellation barriers take care of breaking downstream alignments
		// we only need to make sure that suspended queued requests are resumed

		boolean haveMoreRecentPending = false;
		for (PendingCheckpoint p : pendingCheckpoints.values()) {
			if (!p.isDiscarded() && p.getCheckpointId() >= pendingCheckpoint.getCheckpointId()) {
				haveMoreRecentPending = true;
				break;
			}
		}

		if (!haveMoreRecentPending) {
			triggerQueuedRequests();
		}
	}

	/**
	 * Discards the given state object asynchronously belonging to the given job, execution attempt
	 * id and checkpoint id.
	 *
	 * @param jobId identifying the job to which the state object belongs
	 * @param executionAttemptID identifying the task to which the state object belongs
	 * @param checkpointId of the state object
	 * @param subtaskState to discard asynchronously
	 */
	private void discardSubtaskState(
			final JobID jobId,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointId,
			final TaskStateSnapshot subtaskState) {

		if (subtaskState != null) {
			executor.execute(new Runnable() {
				@Override
				public void run() {

					try {
						subtaskState.discardState();
					} catch (Throwable t2) {
						LOG.warn("Could not properly discard state object of checkpoint {} " +
							"belonging to task {} of job {}.", checkpointId, executionAttemptID, jobId, t2);
					}
				}
			});
		}
	}

	private void failPendingCheckpoint(
			final PendingCheckpoint pendingCheckpoint,
			final CheckpointFailureReason reason) {

		failPendingCheckpoint(pendingCheckpoint, reason, null);
	}

	private void failPendingCheckpoint(
		final PendingCheckpoint pendingCheckpoint,
		final CheckpointFailureReason reason,
		@Nullable final Throwable cause) {

		CheckpointException exception = new CheckpointException(reason, cause);
		pendingCheckpoint.abort(reason, cause);
		failureManager.handleJobLevelCheckpointException(exception, pendingCheckpoint.getCheckpointId());

		checkAndResetCheckpointScheduler();
	}

	private void failPendingCheckpointDueToTaskFailure(
		final PendingCheckpoint pendingCheckpoint,
		final CheckpointFailureReason reason,
		final ExecutionAttemptID executionAttemptID) {

		failPendingCheckpointDueToTaskFailure(pendingCheckpoint, reason, null, executionAttemptID);
	}

	private void failPendingCheckpointDueToTaskFailure(
			final PendingCheckpoint pendingCheckpoint,
			final CheckpointFailureReason reason,
			@Nullable final Throwable cause,
			final ExecutionAttemptID executionAttemptID) {

		CheckpointException exception = new CheckpointException(reason, cause);
		pendingCheckpoint.abort(reason, cause);
		failureManager.handleTaskLevelCheckpointException(exception, pendingCheckpoint.getCheckpointId(), executionAttemptID);

		checkAndResetCheckpointScheduler();
	}

	private void checkAndResetCheckpointScheduler() {
		if (!shutdown && periodicScheduling && currentPeriodicTrigger == null) {
			synchronized (lock) {
				if (pendingCheckpoints.isEmpty() || allPendingCheckpointsDiscarded()) {
					triggerRequestQueued = false;
					currentPeriodicTrigger = scheduleTriggerWithDelay(getRandomInitDelay());
				}
			}
		}
	}

	private boolean allPendingCheckpointsDiscarded() {
		return pendingCheckpoints.values().stream().allMatch(PendingCheckpoint::isDiscarded);
	}
}
