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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
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

	/** The number of recent checkpoints whose IDs are remembered */
	private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

	// ------------------------------------------------------------------------

	/** Coordinator-wide lock to safeguard the checkpoint updates */
	private final Object lock = new Object();

	/** Lock specially to make sure that trigger requests do not overtake each other.
	 * This is not done with the coordinator-wide lock, because as part of triggering,
	 * blocking operations may happen (distributed atomic counters).
	 * Using a dedicated lock, we avoid blocking the processing of 'acknowledge/decline'
	 * messages during that phase. */
	private final Object triggerLock = new Object();

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

	/** Default directory for persistent checkpoints; <code>null</code> if none configured. */
	private final String checkpointDirectory;

	/** A list of recent checkpoint IDs, to identify late messages (vs invalid ones) */
	private final ArrayDeque<Long> recentPendingCheckpoints;

	/** Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these
	 * need to be ascending across job managers. */
	private final CheckpointIDCounter checkpointIdCounter;

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
	private JobStatusListener jobStatusListener;

	/** The number of consecutive failed trigger attempts */
	private final AtomicInteger numUnsuccessfulCheckpointsTriggers = new AtomicInteger(0);

	private ScheduledTrigger currentPeriodicTrigger;

	private long lastTriggeredCheckpoint;

	/** Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
	 * Non-volatile, because only accessed in synchronized scope */
	private boolean periodicScheduling;

	/** Flag whether a trigger request could not be handled immediately. Non-volatile, because only
	 * accessed in synchronized scope */
	private boolean triggerRequestQueued;

	/** Flag marking the coordinator as shut down (not accepting any messages any more) */
	private volatile boolean shutdown;

	/** Helper for tracking checkpoint statistics  */
	private final CheckpointStatsTracker statsTracker;

	/** Default checkpoint properties **/
	private final CheckpointProperties checkpointProperties;

	// --------------------------------------------------------------------------------------------

	public CheckpointCoordinator(
			JobID job,
			long baseInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpointAttempts,
			ExternalizedCheckpointSettings externalizeSettings,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			String checkpointDirectory,
			CheckpointStatsTracker statsTracker) {

		// sanity checks
		checkArgument(baseInterval > 0, "Checkpoint timeout must be larger than zero");
		checkArgument(checkpointTimeout >= 1, "Checkpoint timeout must be larger than zero");
		checkArgument(minPauseBetweenCheckpoints >= 0, "minPauseBetweenCheckpoints must be >= 0");
		checkArgument(maxConcurrentCheckpointAttempts >= 1, "maxConcurrentCheckpointAttempts must be >= 1");

		if (externalizeSettings.externalizeCheckpoints() && checkpointDirectory == null) {
			throw new IllegalStateException("CheckpointConfig says to persist periodic " +
					"checkpoints, but no checkpoint directory has been configured. You can " +
					"configure configure one via key '" + ConfigConstants.CHECKPOINTS_DIRECTORY_KEY + "'.");
		}

		// it does not make sense to schedule checkpoints more often then the desired
		// time between checkpoints
		if (baseInterval < minPauseBetweenCheckpoints) {
			baseInterval = minPauseBetweenCheckpoints;
		}

		this.job = checkNotNull(job);
		this.baseInterval = baseInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
		this.tasksToTrigger = checkNotNull(tasksToTrigger);
		this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
		this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
		this.pendingCheckpoints = new LinkedHashMap<>();
		this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
		this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
		this.checkpointDirectory = checkpointDirectory;
		this.recentPendingCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
		this.statsTracker = checkNotNull(statsTracker);

		this.timer = new Timer("Checkpoint Timer", true);

		if (externalizeSettings.externalizeCheckpoints()) {
			LOG.info("Persisting periodic checkpoints externally at {}.", checkpointDirectory);
			checkpointProperties = CheckpointProperties.forExternalizedCheckpoint(externalizeSettings.deleteOnCancellation());
		} else {
			checkpointProperties = CheckpointProperties.forStandardCheckpoint();
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
				LOG.info("Stopping checkpoint coordinator for job " + job);

				periodicScheduling = false;
				triggerRequestQueued = false;

				// shut down the thread that handles the timeouts and pending triggers
				timer.cancel();

				// clear and discard all pending checkpoints
				for (PendingCheckpoint pending : pendingCheckpoints.values()) {
					pending.abortError(new Exception("Checkpoint Coordinator is shutting down"));
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
	//  Handling checkpoints and messages
	// --------------------------------------------------------------------------------------------

	/**
	 * Triggers a savepoint with the given savepoint directory as a target.
	 *
	 * @param timestamp The timestamp for the savepoint.
	 * @param targetDirectory Target directory for the savepoint.
	 * @return A future to the completed checkpoint
	 * @throws IllegalStateException If no savepoint directory has been
	 *                               specified and no default savepoint directory has been
	 *                               configured
	 * @throws Exception             Failures during triggering are forwarded
	 */
	public Future<CompletedCheckpoint> triggerSavepoint(long timestamp, String targetDirectory) throws Exception {
		checkNotNull(targetDirectory, "Savepoint target directory");

		CheckpointProperties props = CheckpointProperties.forStandardSavepoint();
		CheckpointTriggerResult result = triggerCheckpoint(timestamp, props, targetDirectory, false);

		if (result.isSuccess()) {
			return result.getPendingCheckpoint().getCompletionFuture();
		} else {
			Throwable cause = new Exception("Failed to trigger savepoint: " + result.getFailureReason().message());
			Future<CompletedCheckpoint> failed = FlinkCompletableFuture.completedExceptionally(cause);
			return failed;
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
	public boolean triggerCheckpoint(long timestamp, boolean isPeriodic) throws Exception {
		return triggerCheckpoint(timestamp, checkpointProperties, checkpointDirectory, isPeriodic).isSuccess();
	}

	CheckpointTriggerResult triggerCheckpoint(
			long timestamp,
			CheckpointProperties props,
			String targetDirectory,
			boolean isPeriodic) throws Exception {

		// Sanity check
		if (props.externalizeCheckpoint() && targetDirectory == null) {
			throw new IllegalStateException("No target directory specified to persist checkpoint to.");
		}

		// make some eager pre-checks
		synchronized (lock) {
			// abort if the coordinator has been shutdown in the meantime
			if (shutdown) {
				return new CheckpointTriggerResult(CheckpointDeclineReason.COORDINATOR_SHUTDOWN);
			}

			// Don't allow periodic checkpoint if scheduling has been disabled
			if (isPeriodic && !periodicScheduling) {
				return new CheckpointTriggerResult(CheckpointDeclineReason.PERIODIC_SCHEDULER_SHUTDOWN);
			}

			// validate whether the checkpoint can be triggered, with respect to the limit of
			// concurrent checkpoints, and the minimum time between checkpoints.
			// these checks are not relevant for savepoints
			if (!props.forceCheckpoint()) {
				// sanity check: there should never be more than one trigger request queued
				if (triggerRequestQueued) {
					LOG.warn("Trying to trigger another checkpoint while one was queued already");
					return new CheckpointTriggerResult(CheckpointDeclineReason.ALREADY_QUEUED);
				}

				// if too many checkpoints are currently in progress, we need to mark that a request is queued
				if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
					triggerRequestQueued = true;
					if (currentPeriodicTrigger != null) {
						currentPeriodicTrigger.cancel();
						currentPeriodicTrigger = null;
					}
					return new CheckpointTriggerResult(CheckpointDeclineReason.TOO_MANY_CONCURRENT_CHECKPOINTS);
				}

				// make sure the minimum interval between checkpoints has passed
				long nextCheckpointEarliest = lastTriggeredCheckpoint + minPauseBetweenCheckpoints;
				if (nextCheckpointEarliest < 0) {
					// overflow
					nextCheckpointEarliest = Long.MAX_VALUE;
				}

				if (nextCheckpointEarliest > timestamp) {
					if (currentPeriodicTrigger != null) {
						currentPeriodicTrigger.cancel();
						currentPeriodicTrigger = null;
					}
					ScheduledTrigger trigger = new ScheduledTrigger();
					// Reassign the new trigger to the currentPeriodicTrigger
					currentPeriodicTrigger = trigger;
					long delay = nextCheckpointEarliest - timestamp;
					timer.scheduleAtFixedRate(trigger, delay, baseInterval);
					return new CheckpointTriggerResult(CheckpointDeclineReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS);
				}
			}
		}

		// check if all tasks that we need to trigger are running.
		// if not, abort the checkpoint
		Execution[] executions = new Execution[tasksToTrigger.length];
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee != null && ee.getState() == ExecutionState.RUNNING) {
				executions[i] = ee;
			} else {
				LOG.info("Checkpoint triggering task {} is not being executed at the moment. Aborting checkpoint.",
						tasksToTrigger[i].getSimpleName());
				return new CheckpointTriggerResult(CheckpointDeclineReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
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
				return new CheckpointTriggerResult(CheckpointDeclineReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}

		// we will actually trigger this checkpoint!

		// we lock with a special lock to make sure that trigger requests do not overtake each other.
		// this is not done with the coordinator-wide lock, because the 'checkpointIdCounter'
		// may issue blocking operations. Using a different lock than teh coordinator-wide lock,
		// we avoid blocking the processing of 'acknowledge/decline' messages during that time.
		synchronized (triggerLock) {
			final long checkpointID;
			try {
				// this must happen outside the coordinator-wide lock, because it communicates
				// with external services (in HA mode) and may block for a while.
				checkpointID = checkpointIdCounter.getAndIncrement();
			}
			catch (Throwable t) {
				int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
				LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
				return new CheckpointTriggerResult(CheckpointDeclineReason.EXCEPTION);
			}

			final PendingCheckpoint checkpoint = new PendingCheckpoint(
					job,
					checkpointID,
					timestamp,
					ackTasks,
					isPeriodic,
					props,
					targetDirectory);

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

								checkpoint.abortExpired();
								pendingCheckpoints.remove(checkpointID);
								rememberRecentCheckpointId(checkpointID);

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
				// re-acquire the coordinator-wide lock
				synchronized (lock) {
					// since we released the lock in the meantime, we need to re-check
					// that the conditions still hold.
					if (shutdown) {
						return new CheckpointTriggerResult(CheckpointDeclineReason.COORDINATOR_SHUTDOWN);
					}
					else if (!props.forceCheckpoint()) {
						if (triggerRequestQueued) {
							LOG.warn("Trying to trigger another checkpoint while one was queued already");
							return new CheckpointTriggerResult(CheckpointDeclineReason.ALREADY_QUEUED);
						}

						if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
							triggerRequestQueued = true;
							if (currentPeriodicTrigger != null) {
								currentPeriodicTrigger.cancel();
								currentPeriodicTrigger = null;
							}
							return new CheckpointTriggerResult(CheckpointDeclineReason.TOO_MANY_CONCURRENT_CHECKPOINTS);
						}

						// make sure the minimum interval between checkpoints has passed
						long nextCheckpointEarliest = lastTriggeredCheckpoint + minPauseBetweenCheckpoints;
						if (nextCheckpointEarliest < 0) {
							// overflow
							nextCheckpointEarliest = Long.MAX_VALUE;
						}

						if (nextCheckpointEarliest > timestamp) {
							if (currentPeriodicTrigger != null) {
								currentPeriodicTrigger.cancel();
								currentPeriodicTrigger = null;
							}
							ScheduledTrigger trigger = new ScheduledTrigger();
							// Reassign the new trigger to the currentPeriodicTrigger
							currentPeriodicTrigger = trigger;
							long delay = nextCheckpointEarliest - timestamp;
							timer.scheduleAtFixedRate(trigger, delay, baseInterval);
							return new CheckpointTriggerResult(CheckpointDeclineReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS);
						}
					}

					LOG.info("Triggering checkpoint " + checkpointID + " @ " + timestamp);

					lastTriggeredCheckpoint = Math.max(timestamp, lastTriggeredCheckpoint);
					pendingCheckpoints.put(checkpointID, checkpoint);
					timer.schedule(canceller, checkpointTimeout);
				}
				// end of lock scope

				// send the messages to the tasks that trigger their checkpoint
				for (Execution execution: executions) {
					execution.triggerCheckpoint(checkpointID, timestamp);
				}

				numUnsuccessfulCheckpointsTriggers.set(0);
				return new CheckpointTriggerResult(checkpoint);
			}
			catch (Throwable t) {
				// guard the map against concurrent modifications
				synchronized (lock) {
					pendingCheckpoints.remove(checkpointID);
				}

				int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
				LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);

				if (!checkpoint.isDiscarded()) {
					checkpoint.abortError(new Exception("Failed to trigger checkpoint"));
				}
				return new CheckpointTriggerResult(CheckpointDeclineReason.EXCEPTION);
			}

		} // end trigger lock
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
				checkpoint.abortDeclined();
				rememberRecentCheckpointId(checkpointId);

				boolean haveMoreRecentPending = false;

				for (PendingCheckpoint p : pendingCheckpoints.values()) {
					if (!p.isDiscarded() && p.getCheckpointTimestamp() >= checkpoint.getCheckpointTimestamp()) {
						haveMoreRecentPending = true;
						break;
					}
				}
				if (!haveMoreRecentPending && !triggerRequestQueued) {
					LOG.info("Triggering new checkpoint because of discarded checkpoint " + checkpointId);
					triggerCheckpoint(System.currentTimeMillis(), checkpoint.getProps(), checkpoint.getTargetDirectory(), checkpoint.isPeriodic());
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

				if (checkpoint.acknowledgeTask(
						message.getTaskExecutionId(),
						message.getSubtaskState())) {
					if (checkpoint.isFullyAcknowledged()) {
						completed = checkpoint.finalizeCheckpoint();

						completedCheckpointStore.addCheckpoint(completed);

						LOG.info("Completed checkpoint " + checkpointId + " (in " +
								completed.getDuration() + " ms)");

						if (LOG.isDebugEnabled()) {
							StringBuilder builder = new StringBuilder();
							for (Map.Entry<JobVertexID, TaskState> entry : completed.getTaskStates().entrySet()) {
								builder.append("JobVertexID: ").append(entry.getKey()).append(" {").append(entry.getValue()).append("}");
							}

							LOG.debug(builder.toString());
						}

						pendingCheckpoints.remove(checkpointId);
						rememberRecentCheckpointId(checkpointId);

						dropSubsumedCheckpoints(completed.getCheckpointID());

						triggerQueuedRequests();
					}
				} else {
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
					ee.notifyCheckpointComplete(checkpointId, timestamp);
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

	private void dropSubsumedCheckpoints(long checkpointId) throws Exception {
		Iterator<Map.Entry<Long, PendingCheckpoint>> entries = pendingCheckpoints.entrySet().iterator();

		while (entries.hasNext()) {
			PendingCheckpoint p = entries.next().getValue();
			// remove all pending checkpoints that are lesser than the current completed checkpoint
			if (p.getCheckpointId() < checkpointId && p.canBeSubsumed()) {
				rememberRecentCheckpointId(p.getCheckpointId());
				p.abortSubsumed();
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

	public boolean restoreLatestCheckpointedState(
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
					return false;
				}
			}

			LOG.info("Restoring from latest valid checkpoint: {}.", latest);

			StateAssignmentOperation stateAssignmentOperation =
					new StateAssignmentOperation(tasks, latest, allOrNothingState);

			stateAssignmentOperation.assignStates();

			return true;
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
			return new HashMap<>(this.pendingCheckpoints);
		}
	}

	public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
		synchronized (lock) {
			return completedCheckpointStore.getAllCheckpoints();
		}
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
				try {
					p.abortError(new Exception("Checkpoint Coordinator is suspending."));
				} catch (Throwable t) {
					LOG.error("Error while disposing pending checkpoint", t);
				}
			}

			pendingCheckpoints.clear();
			numUnsuccessfulCheckpointsTriggers.set(0);
		}
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

	private class ScheduledTrigger extends TimerTask {

		@Override
		public void run() {
			try {
				triggerCheckpoint(System.currentTimeMillis(), true);
			}
			catch (Exception e) {
				LOG.error("Exception while triggering checkpoint", e);
			}
		}
	}
}
