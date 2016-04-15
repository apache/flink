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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.google.common.collect.Maps;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.ResponseStackTraceSampleFailure;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.ResponseStackTraceSampleSuccess;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.TriggerStackTraceSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A coordinator for triggering and collecting stack traces of running tasks.
 */
public class StackTraceSampleCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(StackTraceSampleCoordinator.class);

	private static final int NUM_GHOST_SAMPLE_IDS = 10;

	private final Object lock = new Object();

	/** Actor for responses. */
	private final ActorGateway responseActor;

	/** Time out after the expected sampling duration. */
	private final int sampleTimeout;

	/** In progress samples (guarded by lock). */
	private final Map<Integer, PendingStackTraceSample> pendingSamples = new HashMap<>();

	/** A list of recent sample IDs to identify late messages vs. invalid ones. */
	private final ArrayDeque<Integer> recentPendingSamples = new ArrayDeque<>(NUM_GHOST_SAMPLE_IDS);

	/** Sample ID counter (guarded by lock). */
	private int sampleIdCounter;

	/**
	 * Timer to discard expired in progress samples. Lazily initiated as the
	 * sample coordinator will not be used very often (guarded by lock).
	 */
	private Timer timer;

	/**
	 * Flag indicating whether the coordinator is still running (guarded by
	 * lock).
	 */
	private boolean isShutDown;

	/**
	 * Creates a new coordinator for the job.
	 *
	 * @param sampleTimeout Time out after the expected sampling duration.
	 *                      This is added to the expected duration of a
	 *                      sample, which is determined by the number of
	 *                      samples and the delay between each sample.
	 */
	public StackTraceSampleCoordinator(ActorSystem actorSystem, int sampleTimeout) {
		Props props = Props.create(StackTraceSampleCoordinatorActor.class, this);
		this.responseActor = new AkkaActorGateway(actorSystem.actorOf(props), null);

		checkArgument(sampleTimeout >= 0);
		this.sampleTimeout = sampleTimeout;
	}

	/**
	 * Triggers a stack trace sample to all tasks.
	 *
	 * @param tasksToSample       Tasks to sample.
	 * @param numSamples          Number of stack trace samples to collect.
	 * @param delayBetweenSamples Delay between consecutive samples.
	 * @param maxStackTraceDepth  Maximum depth of the stack trace. 0 indicates
	 *                            no maximum and keeps the complete stack trace.
	 * @return A future of the completed stack trace sample
	 */
	@SuppressWarnings("unchecked")
	public Future<StackTraceSample> triggerStackTraceSample(
			ExecutionVertex[] tasksToSample,
			int numSamples,
			FiniteDuration delayBetweenSamples,
			int maxStackTraceDepth) {

		checkNotNull(tasksToSample, "Tasks to sample");
		checkArgument(tasksToSample.length >= 1, "No tasks to sample");
		checkArgument(numSamples >= 1, "No number of samples");
		checkArgument(maxStackTraceDepth >= 0, "Negative maximum stack trace depth");

		// Execution IDs of running tasks
		ExecutionAttemptID[] triggerIds = new ExecutionAttemptID[tasksToSample.length];

		// Check that all tasks are RUNNING before triggering anything. The
		// triggering can still fail.
		for (int i = 0; i < triggerIds.length; i++) {
			Execution execution = tasksToSample[i].getCurrentExecutionAttempt();
			if (execution != null && execution.getState() == ExecutionState.RUNNING) {
				triggerIds[i] = execution.getAttemptId();
			} else {
				Promise failedPromise = new scala.concurrent.impl.Promise.DefaultPromise<>()
						.failure(new IllegalStateException("Task " + tasksToSample[i]
								.getTaskNameWithSubtaskIndex() + " is not running."));
				return failedPromise.future();
			}
		}

		synchronized (lock) {
			if (isShutDown) {
				Promise failedPromise = new scala.concurrent.impl.Promise.DefaultPromise<>()
						.failure(new IllegalStateException("Shut down"));
				return failedPromise.future();
			}

			if (timer == null) {
				timer = new Timer("Stack trace sample coordinator timer");
			}

			int sampleId = sampleIdCounter++;

			LOG.debug("Triggering stack trace sample {}", sampleId);

			final PendingStackTraceSample pending = new PendingStackTraceSample(
					sampleId, triggerIds);

			// Discard the sample if it takes too long. We don't send cancel
			// messages to the task managers, but only wait for the responses
			// and then ignore them.
			long expectedDuration = numSamples * delayBetweenSamples.toMillis();
			long discardDelay = expectedDuration + sampleTimeout;

			TimerTask discardTask = new TimerTask() {
				@Override
				public void run() {
					try {
						synchronized (lock) {
							if (!pending.isDiscarded()) {
								LOG.info("Sample {} expired before completing",
										pending.getSampleId());

								pending.discard(new RuntimeException("Time out"));
								if (pendingSamples.remove(pending.getSampleId()) != null) {
									rememberRecentSampleId(pending.getSampleId());
								}
							}
						}
					} catch (Throwable t) {
						LOG.error("Exception while handling sample timeout", t);
					}
				}
			};

			// Add the pending sample before scheduling the discard task to
			// prevent races with removing it again.
			pendingSamples.put(sampleId, pending);

			timer.schedule(discardTask, discardDelay);

			boolean success = true;
			try {
				// Trigger all samples
				for (int i = 0; i < tasksToSample.length; i++) {
					TriggerStackTraceSample msg = new TriggerStackTraceSample(
							sampleId,
							triggerIds[i],
							numSamples,
							delayBetweenSamples,
							maxStackTraceDepth);

					if (!tasksToSample[i].sendMessageToCurrentExecution(
							msg,
							triggerIds[i],
							responseActor)) {
						success = false;
						break;
					}
				}

				return pending.getStackTraceSampleFuture();
			} finally {
				if (!success) {
					pending.discard(new RuntimeException("Failed to trigger sample, " +
							"because task has been reset."));
					pendingSamples.remove(sampleId);
					rememberRecentSampleId(sampleId);
				}
			}
		}
	}

	/**
	 * Cancels a pending sample.
	 *
	 * @param sampleId ID of the sample to cancel.
	 * @param cause Cause of the cancelling (can be <code>null</code>).
	 */
	public void cancelStackTraceSample(int sampleId, Exception cause) {
		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			PendingStackTraceSample sample = pendingSamples.remove(sampleId);
			if (sample != null) {
				if (cause != null) {
					LOG.info("Cancelling sample " + sampleId, cause);
				} else {
					LOG.info("Cancelling sample {}", sampleId);
				}

				sample.discard(cause);
				rememberRecentSampleId(sampleId);
			}
		}
	}

	/**
	 * Shuts down the coordinator.
	 *
	 * <p>After shut down, no further operations are executed.
	 */
	public void shutDown() {
		synchronized (lock) {
			if (!isShutDown) {
				LOG.info("Shutting down stack trace sample coordinator.");

				for (PendingStackTraceSample pending : pendingSamples.values()) {
					pending.discard(new RuntimeException("Shut down"));
				}

				pendingSamples.clear();

				if (timer != null) {
					timer.cancel();
				}

				isShutDown = true;
			}
		}
	}

	/**
	 * Collects stack traces of a task.
	 *
	 * @param sampleId    ID of the sample.
	 * @param executionId ID of the sampled task.
	 * @param stackTraces Stack traces of the sampled task.
	 *
	 * @throws IllegalStateException If unknown sample ID and not recently
	 *                               finished or cancelled sample.
	 */
	public void collectStackTraces(
			int sampleId,
			ExecutionAttemptID executionId,
			List<StackTraceElement[]> stackTraces) {

		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Collecting stack trace sample {} of task {}", sampleId, executionId);
			}

			PendingStackTraceSample pending = pendingSamples.get(sampleId);

			if (pending != null) {
				pending.collectStackTraces(executionId, stackTraces);

				// Publish the sample
				if (pending.isComplete()) {
					pendingSamples.remove(sampleId);
					rememberRecentSampleId(sampleId);

					pending.completePromiseAndDiscard();
				}
			} else if (recentPendingSamples.contains(sampleId)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received late stack trace sample {} of task {}",
							sampleId, executionId);
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Unknown sample ID " + sampleId);
				}
			}
		}
	}

	private void rememberRecentSampleId(int sampleId) {
		if (recentPendingSamples.size() >= NUM_GHOST_SAMPLE_IDS) {
			recentPendingSamples.removeFirst();
		}
		recentPendingSamples.addLast(sampleId);
	}

	int getNumberOfPendingSamples() {
		synchronized (lock) {
			return pendingSamples.size();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A pending stack trace sample, which collects stack traces and owns a
	 * {@link StackTraceSample} promise.
	 *
	 * <p>Access pending sample in lock scope.
	 */
	private static class PendingStackTraceSample {

		private final int sampleId;
		private final long startTime;
		private final Set<ExecutionAttemptID> pendingTasks;
		private final Map<ExecutionAttemptID, List<StackTraceElement[]>> stackTracesByTask;
		private final Promise<StackTraceSample> stackTracePromise;

		private boolean isDiscarded;

		PendingStackTraceSample(
				int sampleId,
				ExecutionAttemptID[] tasksToCollect) {

			this.sampleId = sampleId;
			this.startTime = System.currentTimeMillis();
			this.pendingTasks = new HashSet<>(Arrays.asList(tasksToCollect));
			this.stackTracesByTask = Maps.newHashMapWithExpectedSize(tasksToCollect.length);
			this.stackTracePromise = new scala.concurrent.impl.Promise.DefaultPromise<>();
		}

		int getSampleId() {
			return sampleId;
		}

		long getStartTime() {
			return startTime;
		}

		boolean isDiscarded() {
			return isDiscarded;
		}

		boolean isComplete() {
			if (isDiscarded) {
				throw new IllegalStateException("Discarded");
			}

			return pendingTasks.isEmpty();
		}

		void discard(Throwable cause) {
			if (!isDiscarded) {
				pendingTasks.clear();
				stackTracesByTask.clear();

				stackTracePromise.failure(new RuntimeException("Discarded", cause));

				isDiscarded = true;
			}
		}

		void collectStackTraces(ExecutionAttemptID executionId, List<StackTraceElement[]> stackTraces) {
			if (isDiscarded) {
				throw new IllegalStateException("Discarded");
			}

			if (pendingTasks.remove(executionId)) {
				stackTracesByTask.put(executionId, Collections.unmodifiableList(stackTraces));
			} else if (isComplete()) {
				throw new IllegalStateException("Completed");
			} else {
				throw new IllegalArgumentException("Unknown task " + executionId);
			}
		}

		void completePromiseAndDiscard() {
			if (isComplete()) {
				isDiscarded = true;

				long endTime = System.currentTimeMillis();

				StackTraceSample stackTraceSample = new StackTraceSample(
						sampleId,
						startTime,
						endTime,
						stackTracesByTask);

				stackTracePromise.success(stackTraceSample);
			} else {
				throw new IllegalStateException("Not completed yet");
			}
		}

		@SuppressWarnings("unchecked")
		Future<StackTraceSample> getStackTraceSampleFuture() {
			return stackTracePromise.future();
		}
	}

	/**
	 * Actor for stack trace sample responses.
	 */
	private static class StackTraceSampleCoordinatorActor extends FlinkUntypedActor {

		StackTraceSampleCoordinator coordinator;

		public StackTraceSampleCoordinatorActor(StackTraceSampleCoordinator coordinator) {
			this.coordinator = checkNotNull(coordinator, "Stack trace sample coordinator");
		}

		@Override
		protected void handleMessage(Object msg) throws Exception {
			try {
				if (msg instanceof ResponseStackTraceSampleSuccess) {
					ResponseStackTraceSampleSuccess success = (ResponseStackTraceSampleSuccess) msg;

					coordinator.collectStackTraces(
							success.sampleId(),
							success.executionId(),
							success.samples());
				} else if (msg instanceof ResponseStackTraceSampleFailure) {
					ResponseStackTraceSampleFailure failure = (ResponseStackTraceSampleFailure) msg;

					coordinator.cancelStackTraceSample(failure.sampleId(), failure.cause());
				} else {
					throw new IllegalArgumentException("Unexpected task sample message");
				}
			} catch (Throwable t) {
				LOG.error("Error responding to message '" + msg + "': " + t.getMessage() + ".", t);
			}
		}

		@Override
		protected UUID getLeaderSessionID() {
			return null;
		}
	}

}
