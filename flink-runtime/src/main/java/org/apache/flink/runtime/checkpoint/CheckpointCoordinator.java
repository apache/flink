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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.ConfirmCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
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
	
	private final ArrayDeque<SuccessfulCheckpoint> completedCheckpoints;
	
	private final ArrayDeque<Long> recentPendingCheckpoints;

	private final AtomicLong checkpointIdCounter = new AtomicLong(1);

	private final AtomicInteger numUnsuccessfulCheckpointsTriggers = new AtomicInteger();

	/** The timer that processes the checkpoint timeouts */
	private final Timer timeoutTimer;
	
	private final long checkpointTimeout;
	
	private final int numSuccessfulCheckpointsToRetain;
	
	private boolean shutdown;
	
	// --------------------------------------------------------------------------------------------

	public CheckpointCoordinator(JobID job, int numSuccessfulCheckpointsToRetain, long checkpointTimeout,
								ExecutionVertex[] tasksToTrigger,
								ExecutionVertex[] tasksToWaitFor,
								ExecutionVertex[] tasksToCommitTo) {
		
		// some sanity checks
		if (job == null || tasksToTrigger == null ||
				tasksToWaitFor == null || tasksToCommitTo == null) {
			throw new NullPointerException();
		}
		if (numSuccessfulCheckpointsToRetain < 1) {
			throw new IllegalArgumentException("Must retain at least one successful checkpoint");
		}
		if (checkpointTimeout < 1) {
			throw new IllegalArgumentException("Checkpoint timeout must be larger than zero");
		}
		
		this.job = job;
		this.numSuccessfulCheckpointsToRetain = numSuccessfulCheckpointsToRetain;
		this.checkpointTimeout = checkpointTimeout;
		this.tasksToTrigger = tasksToTrigger;
		this.tasksToWaitFor = tasksToWaitFor;
		this.tasksToCommitTo = tasksToCommitTo;
		this.pendingCheckpoints = new LinkedHashMap<Long, PendingCheckpoint>();
		this.completedCheckpoints = new ArrayDeque<SuccessfulCheckpoint>(numSuccessfulCheckpointsToRetain + 1);
		this.recentPendingCheckpoints = new ArrayDeque<Long>(NUM_GHOST_CHECKPOINT_IDS);

		timeoutTimer = new Timer("Checkpoint Timeout Handler", true);
	}

	/**
	 * Shuts down the checkpoint coordinator.
	 * 
	 * After this method has been called, the coordinator does not accept and further
	 * messages and cannot trigger any further checkpoints.
	 */
	public void shutdown() {
		synchronized (lock) {
			if (shutdown) {
				return;
			}
			shutdown = true;
			
			// shut down the thread that handles the timeouts
			timeoutTimer.cancel();
			
			// clear and discard all pending checkpoints
			for (PendingCheckpoint pending : pendingCheckpoints.values()) {
				pending.discard();
			}
			pendingCheckpoints.clear();
			
			// clean and discard all successful checkpoints
			for (SuccessfulCheckpoint checkpoint : completedCheckpoints) {
				checkpoint.dispose();
			}
			completedCheckpoints.clear();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Handling checkpoints and messages
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Triggers a new checkpoint and uses the current system time as the
	 * checkpoint time.
	 */
	public void triggerCheckpoint() {
		triggerCheckpoint(System.currentTimeMillis());
	}

	/**
	 * Triggers a new checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 * 
	 * @param timestamp The timestamp for the checkpoint.
	 */
	public boolean triggerCheckpoint(final long timestamp) {
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
				if (ee != null) {
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
								checkpoint.discard();
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
				timeoutTimer.schedule(canceller, checkpointTimeout);
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
				pendingCheckpoints.remove(checkpointID);
			}
			
			return false;
		}
	}
	
	public void receiveAcknowledgeMessage(AcknowledgeCheckpoint message) {
		if (shutdown || message == null) {
			return;
		}
		if (!job.equals(message.getJob())) {
			LOG.error("Received AcknowledgeCheckpoint message for wrong job: {}", message);
			return;
		}
		
		final long checkpointId = message.getCheckpointId();
		boolean checkpointCompleted = false;
		
		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return;
			}
			
			PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);
			if (checkpoint != null && !checkpoint.isDiscarded()) {
				if (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getState())) {
					
					if (checkpoint.isFullyAcknowledged()) {
						LOG.info("Completed checkpoint " + checkpointId);

						SuccessfulCheckpoint completed = checkpoint.toCompletedCheckpoint();
						completedCheckpoints.addLast(completed);
						if (completedCheckpoints.size() > numSuccessfulCheckpointsToRetain) {
							completedCheckpoints.removeFirst();
						}
						pendingCheckpoints.remove(checkpointId);
						rememberRecentCheckpointId(checkpointId);
						
						dropSubsumedCheckpoints(completed.getTimestamp());
						
						checkpointCompleted = true;
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
		if (checkpointCompleted) {
			for (ExecutionVertex ev : tasksToCommitTo) {
				Execution ee = ev.getCurrentExecutionAttempt();
				if (ee != null) {
					ExecutionAttemptID attemptId = ee.getAttemptId();
					ConfirmCheckpoint confirmMessage = new ConfirmCheckpoint(job, attemptId, checkpointId);
					ev.sendMessageToCurrentExecution(confirmMessage, ee.getAttemptId());
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
				p.discard();
				entries.remove();
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
		return this.completedCheckpoints.size();
	}

	public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
		synchronized (lock) {
			return new HashMap<Long, PendingCheckpoint>(this.pendingCheckpoints);
		}
	}
	
	public List<SuccessfulCheckpoint> getSuccessfulCheckpoints() {
		synchronized (lock) {
			return new ArrayList<SuccessfulCheckpoint>(this.completedCheckpoints);
		}
	}
}
