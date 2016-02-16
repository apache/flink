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

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.NotifyCheckpointComplete;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state.
 * It triggers the checkpoint by sending the messages to the relevant tasks and collects the
 * checkpoint acknowledgements. It also collects and maintains the overview of the state handles
 * reported by the tasks that acknowledge the checkpoint.
 *
 * <p>Depending on the configured {@link RecoveryMode}, the behaviour of the {@link
 * CompletedCheckpointStore} and {@link CheckpointIDCounter} change. The default standalone
 * implementations don't support any recovery.
 */
public class CheckpointCoordinator {

	static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	/** The number of recent checkpoints whose IDs are remembered */
	private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

	/** Coordinator-wide lock to safeguard the checkpoint updates */
	protected final Object lock = new Object();

	/** The job whose checkpoint this coordinator coordinates */
	private final JobID job;

	/** Tasks who need to be sent a message when a checkpoint is started */
	private final ExecutionVertex[] tasksToTrigger;

	/** Tasks who need to acknowledge a checkpoint before it succeeds */
	private final ExecutionVertex[] tasksToWaitFor;

	/** Tasks who need to be sent a message when a checkpoint is confirmed */
	private final ExecutionVertex[] tasksToCommitTo;

	/** Map from checkpoint ID to the pending checkpoint */
	private final Map<Long, PendingCheckpoint> pendingCheckpoints;

	/** Completed checkpoints. Implementations can be blocking. Make sure calls to methods
	 * accessing this don't block the job manager actor and run asynchronously. */
	private final CompletedCheckpointStore completedCheckpointStore;

	/** A list of recent checkpoint IDs, to identify late messages (vs invalid ones) */
	private final ArrayDeque<Long> recentPendingCheckpoints;

	/** Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these
	 * need to be ascending across job managers. */
	protected final CheckpointIDCounter checkpointIdCounter;

	/** Class loader used to deserialize the state handles (as they may be user-defined) */
	private final ClassLoader userClassLoader;

	/** The base checkpoint interval. Actual trigger time may be affected by the
	 * max concurrent checkpoints and minimum-pause values */
	private final long baseInterval;

	/** The max time (in ms) that a checkpoint may take */
	private final long checkpointTimeout;

	/** The min time(in ms) to delay after a checkpoint could be triggered. Allows to
	 * enforce minimum processing time between checkpoint attempts */
	private final long minPauseBetweenCheckpoints;

	/** The maximum number of checkpoints that may be in progress at the same time */
	private final int maxConcurrentCheckpointAttempts;

	/** The timer that handles the checkpoint timeouts and triggers periodic checkpoints */
	private final Timer timer;

	/** Actor that receives status updates from the execution graph this coordinator works for */
	private ActorGateway jobStatusListener;

	/** The number of consecutive failed trigger attempts */
	private int numUnsuccessfulCheckpointsTriggers;

	private ScheduledTrigger currentPeriodicTrigger;

	/** Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
	 * Non-volatile, because only accessed in synchronized scope */
	private boolean periodicScheduling;

	/** Flag whether a trigger request could not be handled immediately. Non-volatile, because only
	 * accessed in synchronized scope */
	private boolean triggerRequestQueued;

	/** Flag marking the coordinator as shut down (not accepting any messages any more) */
	private volatile boolean shutdown;

	/** Shutdown hook thread to clean up state handles. */
	private final Thread shutdownHook;

	/** Helper for tracking checkpoint statistics  */
	private final CheckpointStatsTracker statsTracker;

	// --------------------------------------------------------------------------------------------

	public CheckpointCoordinator(
			JobID job,
			long baseInterval,
			long checkpointTimeout,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			ClassLoader userClassLoader,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			RecoveryMode recoveryMode) throws Exception {

		this(job, baseInterval, checkpointTimeout, 0L, Integer.MAX_VALUE,
				tasksToTrigger, tasksToWaitFor, tasksToCommitTo,
				userClassLoader, checkpointIDCounter, completedCheckpointStore, recoveryMode,
				new DisabledCheckpointStatsTracker());
	}

	public CheckpointCoordinator(
			JobID job,
			long baseInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpointAttempts,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			ClassLoader userClassLoader,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			RecoveryMode recoveryMode,
			CheckpointStatsTracker statsTracker) throws Exception {

		// Sanity check
		checkArgument(baseInterval > 0, "Checkpoint timeout must be larger than zero");
		checkArgument(checkpointTimeout >= 1, "Checkpoint timeout must be larger than zero");
		checkArgument(minPauseBetweenCheckpoints >= 0, "minPauseBetweenCheckpoints must be >= 0");
		checkArgument(maxConcurrentCheckpointAttempts >= 1, "maxConcurrentCheckpointAttempts must be >= 1");

		this.job = checkNotNull(job);
		this.baseInterval = baseInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
		this.tasksToTrigger = checkNotNull(tasksToTrigger);
		this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
		this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
		this.pendingCheckpoints = new LinkedHashMap<Long, PendingCheckpoint>();
		this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
		this.recentPendingCheckpoints = new ArrayDeque<Long>(NUM_GHOST_CHECKPOINT_IDS);
		this.userClassLoader = userClassLoader;

		// Started with the periodic scheduler
		this.checkpointIdCounter = checkNotNull(checkpointIDCounter);

		this.timer = new Timer("Checkpoint Timer", true);

		this.statsTracker = checkNotNull(statsTracker);

		if (recoveryMode == RecoveryMode.STANDALONE) {
			// Add shutdown hook to clean up state handles when no checkpoint recovery is
			// possible. In case of another configured recovery mode, the checkpoints need to be
			// available for the standby job managers.
			this.shutdownHook = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						CheckpointCoordinator.this.shutdown();
					}
					catch (Throwable t) {
						LOG.error("Error during shutdown of checkpoint coordinator via " +
								"JVM shutdown hook: " + t.getMessage(), t);
					}
				}
			});

			try {
				// Add JVM shutdown hook to call shutdown of service
				Runtime.getRuntime().addShutdownHook(shutdownHook);
			}
			catch (IllegalStateException ignored) {
				// JVM is already shutting down. No need to do anything.
			}
			catch (Throwable t) {
				LOG.error("Cannot register checkpoint coordinator shutdown hook.", t);
			}
		}
		else {
			this.shutdownHook = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Callbacks
	// --------------------------------------------------------------------------------------------

	/**
	 * Callback on shutdown of the coordinator. Called in lock scope.
	 */
	protected void onShutdown() {
	}

	/**
	 * Callback on cancellation of a checkpoint. Called in lock scope.
	 */
	protected void onCancelCheckpoint(long canceledCheckpointId) {
	}

	/**
	 * Callback on full acknowledgement of a checkpoint. Called in lock scope.
	 */
	protected void onFullyAcknowledgedCheckpoint(CompletedCheckpoint checkpoint) {
	}

	// --------------------------------------------------------------------------------------------
	//  Clean shutdown
	// --------------------------------------------------------------------------------------------

	/**
	 * Shuts down the checkpoint coordinator.
	 *
	 * After this method has been called, the coordinator does not accept and further
	 * messages and cannot trigger any further checkpoints.
	 */
	public void shutdown() throws Exception {
		synchronized (lock) {
			try {
				if (!shutdown) {
					shutdown = true;
					LOG.info("Stopping checkpoint coordinator for job " + job);

					periodicScheduling = false;
					triggerRequestQueued = false;

					// shut down the thread that handles the timeouts and pending triggers
					timer.cancel();

					// make sure that the actor does not linger
					if (jobStatusListener != null) {
						jobStatusListener.tell(PoisonPill.getInstance());
						jobStatusListener = null;
					}

					checkpointIdCounter.stop();

					// clear and discard all pending checkpoints
					for (PendingCheckpoint pending : pendingCheckpoints.values()) {
						pending.discard(userClassLoader);
					}
					pendingCheckpoints.clear();

					// clean and discard all successful checkpoints
					completedCheckpointStore.discardAllCheckpoints();

					onShutdown();
				}
			}
			finally {
				// Remove shutdown hook to prevent resource leaks, unless this is invoked by the
				// shutdown hook itself.
				if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
					try {
						Runtime.getRuntime().removeShutdownHook(shutdownHook);
					}
					catch (IllegalStateException ignored) {
						// race, JVM is in shutdown already, we can safely ignore this
					}
					catch (Throwable t) {
						LOG.warn("Error unregistering checkpoint coordinator shutdown hook.", t);
					}
				}
			}
		}
	}

	public boolean isShutdown() {
		return shutdown;
	}

	// --------------------------------------------------------------------------------------------
	//  Handling checkpoints and messages
	// --------------------------------------------------------------------------------------------

	/**
	 * Triggers a new checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 *
	 * @param timestamp The timestamp for the checkpoint.
	 */
	public boolean triggerCheckpoint(long timestamp) throws Exception {
		return triggerCheckpoint(timestamp, -1);
	}

	/**
	 * Triggers a new checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 *
	 * @param timestamp The timestamp for the checkpoint.
	 * @param nextCheckpointId The checkpoint ID to use for this checkpoint or <code>-1</code> if
	 *                         the checkpoint ID counter should be queried.
	 */
	public boolean triggerCheckpoint(long timestamp, long nextCheckpointId) throws Exception {
		// make some eager pre-checks
		synchronized (lock) {
			// abort if the coordinator has been shutdown in the meantime
			if (shutdown) {
				return false;
			}

			// sanity check: there should never be more than one trigger request queued
			if (triggerRequestQueued) {
				LOG.warn("Trying to trigger another checkpoint while one was queued already");
				return false;
			}

			// if too many checkpoints are currently in progress, we need to mark that a request is queued
			if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
				triggerRequestQueued = true;
				if (currentPeriodicTrigger != null) {
					currentPeriodicTrigger.cancel();
					currentPeriodicTrigger = null;
				}
				return false;
			}
		}

		// first check if all tasks that we need to trigger are running.
		// if not, abort the checkpoint
		ExecutionAttemptID[] triggerIDs = new ExecutionAttemptID[tasksToTrigger.length];
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee != null && ee.getState() == ExecutionState.RUNNING) {
				triggerIDs[i] = ee.getAttemptId();
			} else {
				LOG.info("Checkpoint triggering task {} is not being executed at the moment. Aborting checkpoint.",
						tasksToTrigger[i].getSimpleName());
				return false;
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
				LOG.info("Checkpoint acknowledging task {} is not being executed at the moment. Aborting checkpoint.",
						ev.getSimpleName());
				return false;
			}
		}

		// we will actually trigger this checkpoint!

		final long checkpointID;
		if (nextCheckpointId < 0) {
			try {
				// this must happen outside the locked scope, because it communicates
				// with external services (in HA mode) and may block for a while.
				checkpointID = checkpointIdCounter.getAndIncrement();
			}
			catch (Throwable t) {
				int numUnsuccessful = ++numUnsuccessfulCheckpointsTriggers;
				LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
				return false;
			}
		}
		else {
			checkpointID = nextCheckpointId;
		}

		LOG.info("Triggering checkpoint " + checkpointID + " @ " + timestamp);

		final PendingCheckpoint checkpoint = new PendingCheckpoint(job, checkpointID, timestamp, ackTasks);

		// schedule the timer that will clean up the expired checkpoints
		TimerTask canceller = new TimerTask() {
			@Override
			public void run() {
				try {
					synchronized (lock) {
						// only do the work if the checkpoint is not discarded anyways
						// note that checkpoint completion discards the pending checkpoint object
						if (!checkpoint.isDiscarded()) {
							LOG.info("Checkpoint " + checkpointID + " expired before completing.");

							checkpoint.discard(userClassLoader);
							pendingCheckpoints.remove(checkpointID);
							rememberRecentCheckpointId(checkpointID);

							onCancelCheckpoint(checkpointID);

							triggerQueuedRequests();
						}
					}
				}
				catch (Throwable t) {
					LOG.error("Exception while handling checkpoint timeout", t);
				}
			}
		};

		try {
			// re-acquire the lock
			synchronized (lock) {
				// since we released the lock in the meantime, we need to re-check
				// that the conditions still hold. this is clumsy, but it allows us to
				// release the lock in the meantime while calls to external services are
				// blocking progress, and still gives us early checks that skip work
				// if no checkpoint can happen anyways
				if (shutdown) {
					return false;
				}
				else if (triggerRequestQueued) {
					LOG.warn("Trying to trigger another checkpoint while one was queued already");
					return false;
				}
				else if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
					triggerRequestQueued = true;
					if (currentPeriodicTrigger != null) {
						currentPeriodicTrigger.cancel();
						currentPeriodicTrigger = null;
					}
					return false;
				}

				pendingCheckpoints.put(checkpointID, checkpoint);
				timer.schedule(canceller, checkpointTimeout);
			}
			// end of lock scope

			// send the messages to the tasks that trigger their checkpoint
			for (int i = 0; i < tasksToTrigger.length; i++) {
				ExecutionAttemptID id = triggerIDs[i];
				TriggerCheckpoint message = new TriggerCheckpoint(job, id, checkpointID, timestamp);
				tasksToTrigger[i].sendMessageToCurrentExecution(message, id);
			}

			numUnsuccessfulCheckpointsTriggers = 0;
			return true;
		}
		catch (Throwable t) {
			// guard the map against concurrent modifications
			synchronized (lock) {
				pendingCheckpoints.remove(checkpointID);
			}

			int numUnsuccessful = ++numUnsuccessfulCheckpointsTriggers;
			LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
			if (!checkpoint.isDiscarded()) {
				checkpoint.discard(userClassLoader);
			}
			return false;
		}
	}

	/**
	 * Receives a {@link DeclineCheckpoint} message and returns whether the
	 * message was associated with a pending checkpoint.
	 *
	 * @param message Checkpoint decline from the task manager
	 *
	 * @return Flag indicating whether the declined checkpoint was associated
	 * with a pending checkpoint.
	 */
	public boolean receiveDeclineMessage(DeclineCheckpoint message) throws Exception {
		if (shutdown || message == null) {
			return false;
		}
		if (!job.equals(message.getJob())) {
			LOG.error("Received DeclineCheckpoint message for wrong job: {}", message);
			return false;
		}

		final long checkpointId = message.getCheckpointId();

		CompletedCheckpoint completed = null;
		PendingCheckpoint checkpoint;

		// Flag indicating whether the ack message was for a known pending
		// checkpoint.
		boolean isPendingCheckpoint;

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return false;
			}

			checkpoint = pendingCheckpoints.get(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				isPendingCheckpoint = true;

				LOG.info("Discarding checkpoint " + checkpointId
					+ " because of checkpoint decline from task " + message.getTaskExecutionId());

				pendingCheckpoints.remove(checkpointId);
				checkpoint.discard(userClassLoader);
				rememberRecentCheckpointId(checkpointId);

				boolean haveMoreRecentPending = false;
				Iterator<Map.Entry<Long, PendingCheckpoint>> entries = pendingCheckpoints.entrySet().iterator();
				while (entries.hasNext()) {
					PendingCheckpoint p = entries.next().getValue();
					if (!p.isDiscarded() && p.getCheckpointTimestamp() >= checkpoint.getCheckpointTimestamp()) {
						haveMoreRecentPending = true;
						break;
					}
				}
				if (!haveMoreRecentPending && !triggerRequestQueued) {
					LOG.info("Triggering new checkpoint because of discarded checkpoint " + checkpointId);
					triggerCheckpoint(System.currentTimeMillis());
				} else if (!haveMoreRecentPending) {
					LOG.info("Promoting queued checkpoint request because of discarded checkpoint " + checkpointId);
					triggerQueuedRequests();
				}
			} else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
					"Received message for discarded but non-removed checkpoint " + checkpointId);
			} else {
				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				if (recentPendingCheckpoints.contains(checkpointId)) {
					isPendingCheckpoint = true;
					LOG.info("Received another decline checkpoint message for now expired checkpoint attempt " + checkpointId);
				} else {
					isPendingCheckpoint = false;
				}
			}
		}

		return isPendingCheckpoint;
	}

	/**
	 * Receives an AcknowledgeCheckpoint message and returns whether the
	 * message was associated with a pending checkpoint.
	 *
	 * @param message Checkpoint ack from the task manager
	 *
	 * @return Flag indicating whether the ack'd checkpoint was associated
	 * with a pending checkpoint.
	 *
	 * @throws Exception If the checkpoint cannot be added to the completed checkpoint store.
	 */
	public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message) throws Exception {
		if (shutdown || message == null) {
			return false;
		}
		if (!job.equals(message.getJob())) {
			LOG.error("Received AcknowledgeCheckpoint message for wrong job: {}", message);
			return false;
		}

		final long checkpointId = message.getCheckpointId();

		CompletedCheckpoint completed = null;
		PendingCheckpoint checkpoint;

		// Flag indicating whether the ack message was for a known pending
		// checkpoint.
		boolean isPendingCheckpoint;

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return false;
			}

			checkpoint = pendingCheckpoints.get(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				isPendingCheckpoint = true;

				if (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getState(), message.getStateSize())) {
					if (checkpoint.isFullyAcknowledged()) {
						completed = checkpoint.toCompletedCheckpoint();

						completedCheckpointStore.addCheckpoint(completed);

						LOG.info("Completed checkpoint " + checkpointId + " (in " +
								completed.getDuration() + " ms)");
						LOG.debug(completed.getStates().toString());

						pendingCheckpoints.remove(checkpointId);
						rememberRecentCheckpointId(checkpointId);

						dropSubsumedCheckpoints(completed.getTimestamp());

						onFullyAcknowledgedCheckpoint(completed);

						triggerQueuedRequests();
					}
				}
				else {
					// checkpoint did not accept message
					LOG.error("Received duplicate or invalid acknowledge message for checkpoint " + checkpointId
							+ " , task " + message.getTaskExecutionId());
				}
			}
			else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
						"Received message for discarded but non-removed checkpoint " + checkpointId);
			}
			else {
				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				if (recentPendingCheckpoints.contains(checkpointId)) {
					isPendingCheckpoint = true;
					LOG.warn("Received late message for now expired checkpoint attempt " + checkpointId);
				}
				else {
					isPendingCheckpoint = false;
				}
			}
		}

		// send the confirmation messages to the necessary targets. we do this here
		// to be outside the lock scope
		if (completed != null) {
			final long timestamp = completed.getTimestamp();

			for (ExecutionVertex ev : tasksToCommitTo) {
				Execution ee = ev.getCurrentExecutionAttempt();
				if (ee != null) {
					ExecutionAttemptID attemptId = ee.getAttemptId();
					NotifyCheckpointComplete notifyMessage = new NotifyCheckpointComplete(job, attemptId, checkpointId, timestamp);
					ev.sendMessageToCurrentExecution(notifyMessage, ee.getAttemptId());
				}
			}

			statsTracker.onCompletedCheckpoint(completed);
		}

		return isPendingCheckpoint;
	}

	private void rememberRecentCheckpointId(long id) {
		if (recentPendingCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
			recentPendingCheckpoints.removeFirst();
		}
		recentPendingCheckpoints.addLast(id);
	}

	private void dropSubsumedCheckpoints(long timestamp) {
		Iterator<Map.Entry<Long, PendingCheckpoint>> entries = pendingCheckpoints.entrySet().iterator();
		while (entries.hasNext()) {
			PendingCheckpoint p = entries.next().getValue();
			if (p.getCheckpointTimestamp() < timestamp) {
				rememberRecentCheckpointId(p.getCheckpointId());

				p.discard(userClassLoader);

				onCancelCheckpoint(p.getCheckpointId());

				entries.remove();
			}
		}
	}

	/**
	 * Triggers the queued request, if there is one.
	 *
	 * <p>NOTE: The caller of this method must hold the lock when invoking the method!
	 */
	private void triggerQueuedRequests() throws Exception {
		if (triggerRequestQueued) {
			triggerRequestQueued = false;

			// trigger the checkpoint from the trigger timer, to finish the work of this thread before
			// starting with the next checkpoint
			ScheduledTrigger trigger = new ScheduledTrigger();
			if (periodicScheduling) {
				if (currentPeriodicTrigger != null) {
					currentPeriodicTrigger.cancel();
				}
				currentPeriodicTrigger = trigger;
				timer.scheduleAtFixedRate(trigger, 0L, baseInterval);
			}
			else {
				timer.schedule(trigger, 0L);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Checkpoint State Restoring
	// --------------------------------------------------------------------------------------------

	public void restoreLatestCheckpointedState(
			Map<JobVertexID, ExecutionJobVertex> tasks,
			boolean errorIfNoCheckpoint,
			boolean allOrNothingState) throws Exception {

		synchronized (lock) {
			if (shutdown) {
				throw new IllegalStateException("CheckpointCoordinator is shut down");
			}

			// Recover the checkpoints
			completedCheckpointStore.recover();

			// restore from the latest checkpoint
			CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint();

			if (latest == null) {
				if (errorIfNoCheckpoint) {
					throw new IllegalStateException("No completed checkpoint available");
				} else {
					return;
				}
			}

			long recoveryTimestamp = System.currentTimeMillis();

			if (allOrNothingState) {
				Map<ExecutionJobVertex, Integer> stateCounts = new HashMap<ExecutionJobVertex, Integer>();

				for (StateForTask state : latest.getStates()) {
					ExecutionJobVertex vertex = tasks.get(state.getOperatorId());
					Execution exec = vertex.getTaskVertices()[state.getSubtask()].getCurrentExecutionAttempt();
					exec.setInitialState(state.getState(), recoveryTimestamp);

					Integer count = stateCounts.get(vertex);
					if (count != null) {
						stateCounts.put(vertex, count+1);
					} else {
						stateCounts.put(vertex, 1);
					}
				}

				// validate that either all task vertices have state, or none
				for (Map.Entry<ExecutionJobVertex, Integer> entry : stateCounts.entrySet()) {
					ExecutionJobVertex vertex = entry.getKey();
					if (entry.getValue() != vertex.getParallelism()) {
						throw new IllegalStateException(
								"The checkpoint contained state only for a subset of tasks for vertex " + vertex);
					}
				}
			}
			else {
				for (StateForTask state : latest.getStates()) {
					ExecutionJobVertex vertex = tasks.get(state.getOperatorId());
					Execution exec = vertex.getTaskVertices()[state.getSubtask()].getCurrentExecutionAttempt();
					exec.setInitialState(state.getState(), recoveryTimestamp);
				}
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Accessors
	// --------------------------------------------------------------------------------------------

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
			return new HashMap<Long, PendingCheckpoint>(this.pendingCheckpoints);
		}
	}

	public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
		synchronized (lock) {
			return completedCheckpointStore.getAllCheckpoints();
		}
	}

	protected long getAndIncrementCheckpointId() {
		try {
			// this must happen outside the locked scope, because it communicates
			// with external services (in HA mode) and may block for a while.
			return checkpointIdCounter.getAndIncrement();
		}
		catch (Throwable t) {
			int numUnsuccessful = ++numUnsuccessfulCheckpointsTriggers;
			LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
			return -1;
		}
	}

	protected ActorGateway getJobStatusListener() {
		return jobStatusListener;
	}

	protected void setJobStatusListener(ActorGateway jobStatusListener) {
		this.jobStatusListener = jobStatusListener;
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

			try {
				// Multiple start calls are OK
				checkpointIdCounter.start();
			} catch (Exception e) {
				String msg = "Failed to start checkpoint ID counter: " + e.getMessage();
				throw new RuntimeException(msg, e);
			}

			periodicScheduling = true;
			currentPeriodicTrigger = new ScheduledTrigger();
			timer.scheduleAtFixedRate(currentPeriodicTrigger, baseInterval, baseInterval);
		}
	}

	public void stopCheckpointScheduler() {
		synchronized (lock) {
			triggerRequestQueued = false;
			periodicScheduling = false;

			if (currentPeriodicTrigger != null) {
				currentPeriodicTrigger.cancel();
				currentPeriodicTrigger = null;
			}

			for (PendingCheckpoint p : pendingCheckpoints.values()) {
				p.discard(userClassLoader);
			}
			pendingCheckpoints.clear();

			numUnsuccessfulCheckpointsTriggers = 0;
		}
	}

	// ------------------------------------------------------------------------
	//  job status listener that schedules / cancels periodic checkpoints
	// ------------------------------------------------------------------------

	public ActorGateway createActivatorDeactivator(ActorSystem actorSystem, UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			if (jobStatusListener == null) {
				Props props = Props.create(CheckpointCoordinatorDeActivator.class, this, leaderSessionID);

				// wrap the ActorRef in a AkkaActorGateway to support message decoration
				jobStatusListener = new AkkaActorGateway(actorSystem.actorOf(props), leaderSessionID);
			}

			return jobStatusListener;
		}
	}

	// ------------------------------------------------------------------------

	private class ScheduledTrigger extends TimerTask {

		@Override
		public void run() {
			try {
				triggerCheckpoint(System.currentTimeMillis());
			}
			catch (Exception e) {
				LOG.error("Exception while triggering checkpoint", e);
			}
		}
	}
}
