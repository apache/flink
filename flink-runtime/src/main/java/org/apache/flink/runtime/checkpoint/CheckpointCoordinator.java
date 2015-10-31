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
import java.util.concurrent.atomic.AtomicInteger;

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
	
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);
	
	/** The number of recent checkpoints whose IDs are remembered */
	private static final int NUM_GHOST_CHECKPOINT_IDS = 16;
	
	/** Coordinator-wide lock to safeguard the checkpoint updates */
	private final Object lock = new Object();
	
	/** The job whose checkpoint this coordinator coordinates */
	private final JobID job;
	
	/** Tasks who need to be sent a message when a checkpoint is started */
	private final ExecutionVertex[] tasksToTrigger;

	/** Tasks who need to acknowledge a checkpoint before it succeeds */
	private final ExecutionVertex[] tasksToWaitFor;
	
	/** Tasks who need to be sent a message when a checkpoint is confirmed */
	private final ExecutionVertex[] tasksToCommitTo;

	private final Map<Long, PendingCheckpoint> pendingCheckpoints;

	/**
	 * Completed checkpoints. Implementations can be blocking. Make sure calls to methods
	 * accessing this don't block the job manager actor and run asynchronously.
	 */
	private final CompletedCheckpointStore completedCheckpointStore;
	
	private final ArrayDeque<Long> recentPendingCheckpoints;

	/**
	 * Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these
	 * need to be ascending across job managers.
	 */
	private final CheckpointIDCounter checkpointIdCounter;

	private final AtomicInteger numUnsuccessfulCheckpointsTriggers = new AtomicInteger();

	/** The timer that handles the checkpoint timeouts and triggers periodic checkpoints */
	private final Timer timer;
	
	private final long checkpointTimeout;
	
	private TimerTask periodicScheduler;
	
	private ActorGateway jobStatusListener;
	
	private ClassLoader userClassLoader;
	
	private volatile boolean shutdown;

	/** Shutdown hook thread to clean up state handles. */
	private final Thread shutdownHook;
	
	// --------------------------------------------------------------------------------------------

	public CheckpointCoordinator(
			JobID job,
			long checkpointTimeout,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			ClassLoader userClassLoader,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			RecoveryMode recoveryMode) throws Exception {
		
		// Sanity check
		checkArgument(checkpointTimeout >= 1, "Checkpoint timeout must be larger than zero");
		
		this.job = checkNotNull(job);
		this.checkpointTimeout = checkpointTimeout;
		this.tasksToTrigger = checkNotNull(tasksToTrigger);
		this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
		this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
		this.pendingCheckpoints = new LinkedHashMap<Long, PendingCheckpoint>();
		this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
		this.recentPendingCheckpoints = new ArrayDeque<Long>(NUM_GHOST_CHECKPOINT_IDS);
		this.userClassLoader = userClassLoader;
		this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
		checkpointIDCounter.start();

		this.timer = new Timer("Checkpoint Timer", true);

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
						LOG.error("Error during shutdown of checkpoint coordniator via " +
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

					// shut down the thread that handles the timeouts
					timer.cancel();

					// make sure that the actor does not linger
					if (jobStatusListener != null) {
						jobStatusListener.tell(PoisonPill.getInstance());
						jobStatusListener = null;
					}

					// the scheduling thread needs also to go away
					if (periodicScheduler != null) {
						periodicScheduler.cancel();
						periodicScheduler = null;
					}

					checkpointIdCounter.stop();

					// clear and discard all pending checkpoints
					for (PendingCheckpoint pending : pendingCheckpoints.values()) {
							pending.discard(userClassLoader, true);
					}
					pendingCheckpoints.clear();

					// clean and discard all successful checkpoints
					completedCheckpointStore.discardAllCheckpoints();
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
						LOG.warn("Error unregistering checkpoint cooordniator shutdown hook.", t);
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
	 * Triggers a new checkpoint and uses the current system time as the
	 * checkpoint time.
	 */
	public void triggerCheckpoint() throws Exception {
		triggerCheckpoint(System.currentTimeMillis());
	}

	/**
	 * Triggers a new checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 * 
	 * @param timestamp The timestamp for the checkpoint.
	 */
	public boolean triggerCheckpoint(final long timestamp) throws Exception {
		if (shutdown) {
			LOG.error("Cannot trigger checkpoint, checkpoint coordinator has been shutdown.");
			return false;
		}
		
		final long checkpointID = checkpointIdCounter.getAndIncrement();
		LOG.info("Triggering checkpoint " + checkpointID + " @ " + timestamp);
		
		try {
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
			Map<ExecutionAttemptID, ExecutionVertex> ackTasks =
								new HashMap<ExecutionAttemptID, ExecutionVertex>(tasksToWaitFor.length);

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
			
			// register a new pending checkpoint. this makes sure we can properly receive acknowledgements
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
								
								checkpoint.discard(userClassLoader, true);
								
								pendingCheckpoints.remove(checkpointID);
								rememberRecentCheckpointId(checkpointID);
							}
						}
					}
					catch (Throwable t) {
						LOG.error("Exception while handling checkpoint timeout", t);
					}
				}
			};
			
			synchronized (lock) {
				if (shutdown) {
					throw new IllegalStateException("Checkpoint coordinator has been shutdown.");
				}
				pendingCheckpoints.put(checkpointID, checkpoint);
				timer.schedule(canceller, checkpointTimeout);
			}

			// send the messages to the tasks that trigger their checkpoint
			for (int i = 0; i < tasksToTrigger.length; i++) {
				ExecutionAttemptID id = triggerIDs[i];
				TriggerCheckpoint message = new TriggerCheckpoint(job, id, checkpointID, timestamp);
				tasksToTrigger[i].sendMessageToCurrentExecution(message, id);
			}
			
			numUnsuccessfulCheckpointsTriggers.set(0);
			return true;
		}
		catch (Throwable t) {
			int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
			LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
			
			synchronized (lock) {
				PendingCheckpoint checkpoint = pendingCheckpoints.remove(checkpointID);
				if (checkpoint != null && !checkpoint.isDiscarded()) {
					checkpoint.discard(userClassLoader, true);
				}
			}
			
			return false;
		}
	}
	
	public void receiveAcknowledgeMessage(AcknowledgeCheckpoint message) throws Exception {
		if (shutdown || message == null) {
			return;
		}
		if (!job.equals(message.getJob())) {
			LOG.error("Received AcknowledgeCheckpoint message for wrong job: {}", message);
			return;
		}
		
		final long checkpointId = message.getCheckpointId();

		CompletedCheckpoint completed = null;
		PendingCheckpoint checkpoint;
		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return;
			}
			
			checkpoint = pendingCheckpoints.get(checkpointId);
			
			if (checkpoint != null && !checkpoint.isDiscarded()) {
				if (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getState())) {
					
					if (checkpoint.isFullyAcknowledged()) {
						completed = checkpoint.toCompletedCheckpoint();

						completedCheckpointStore.addCheckpoint(completed);

						LOG.info("Completed checkpoint " + checkpointId);
						LOG.debug(completed.getStates().toString());

						pendingCheckpoints.remove(checkpointId);
						rememberRecentCheckpointId(checkpointId);
						
						dropSubsumedCheckpoints(completed.getTimestamp());
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
					LOG.warn("Received late message for now expired checkpoint attempt " + checkpointId);
				}
				else {
					LOG.info("Received message for non-existing checkpoint " + checkpointId);
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
		}
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

				p.discard(userClassLoader, true);

				entries.remove();
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

			if (allOrNothingState) {
				Map<ExecutionJobVertex, Integer> stateCounts = new HashMap<ExecutionJobVertex, Integer>();

				for (StateForTask state : latest.getStates()) {
					ExecutionJobVertex vertex = tasks.get(state.getOperatorId());
					Execution exec = vertex.getTaskVertices()[state.getSubtask()].getCurrentExecutionAttempt();
					exec.setInitialState(state.getState());

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
					exec.setInitialState(state.getState());
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

	// --------------------------------------------------------------------------------------------
	//  Periodic scheduling of checkpoints
	// --------------------------------------------------------------------------------------------
	
	public void startPeriodicCheckpointScheduler(long interval) {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}
			
			// cancel any previous scheduler
			stopPeriodicCheckpointScheduler();
			
			// start a new scheduler
			periodicScheduler = new TimerTask() {
				@Override
				public void run() {
					try {
						triggerCheckpoint();
					}
					catch (Exception e) {
						LOG.error("Exception while triggering checkpoint", e);
					}
				}
			};
			timer.scheduleAtFixedRate(periodicScheduler, interval, interval);
		}
	}
	
	public void stopPeriodicCheckpointScheduler() {
		synchronized (lock) {
			if (periodicScheduler != null) {
				periodicScheduler.cancel();
				periodicScheduler = null;
			}
		}
	}
	
	public ActorGateway createJobStatusListener(
			ActorSystem actorSystem,
			long checkpointInterval,
			UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			if (jobStatusListener == null) {
				Props props = Props.create(
						CheckpointCoordinatorDeActivator.class,
						this,
						checkpointInterval,
						leaderSessionID);

				// wrap the ActorRef in a AkkaActorGateway to support message decoration
				jobStatusListener = new AkkaActorGateway(
						actorSystem.actorOf(props),
						leaderSessionID);
			}

			return jobStatusListener;
		}
	}
}
