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

package org.apache.flink.runtime.executiongraph;

import akka.actor.ActorRef;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.google.common.base.Preconditions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.messages.TaskMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Backtracking is a mechanism to schedule only those Execution Vertices of an Execution Graph which
 * do not have an intermediate result available. This is in contrast to the simple way of scheduling
 * a job, where all Execution Vertices are executed starting from the source. The Backtracking starts
 * from the sinks and traverses the Execution Graph to the sources. It only reaches the sources if
 * no intermediate result could be found on the way.
 *
 * @see ExecutionGraph
 * @see ExecutionVertex
 * @see Execution
 */
public class Backtracking {

	private static final Logger LOG = LoggerFactory.getLogger(Backtracking.class);

	private final Deque<TaskRequirement> taskRequirements = new ArrayDeque<TaskRequirement>();

	private final Map<IntermediateResultPartitionID, Boolean> visitedPartitions = new HashMap<IntermediateResultPartitionID, Boolean>();

	private ScheduleAction scheduleAction;
	private Runnable postBacktrackingHook;
	
	public Backtracking(Collection<ExecutionJobVertex> vertices) {
		Preconditions.checkNotNull(vertices);

		// add all sinks found to the stack
		for (ExecutionJobVertex ejv : vertices) {
			if (ejv.getJobVertex().isOutputVertex()) {
				for (ExecutionVertex ev : ejv.getTaskVertices()) {
					if (ev.getExecutionState() == ExecutionState.CREATED) {
						taskRequirements.add(new TaskRequirement(ev));
					}
				}
			}
		}

		this.scheduleAction = new ScheduleAction() {
			@Override
			public void schedule(ExecutionVertex ev) {}
		};

		this.postBacktrackingHook = new Runnable() {
			@Override
			public void run() {}
		};
	}

	/**
	 * Scheduling to be performed when an ExecutionVertex is encountered that cannot be resumed
	 */
	public interface ScheduleAction {
		void schedule(ExecutionVertex ev);
	}

	/**
	 * A TaskRequirement encapsulates an ExecutionVertex and its IntermediateResultPartitions which
	 * are required for execution.
	 */
	private class TaskRequirement {

		private final ExecutionVertex executionVertex;
		private final Deque<IntermediateResultPartition> pendingInputs = new ArrayDeque<IntermediateResultPartition>();
		private final int numInputs;

		private int availableInputs = 0;

		public TaskRequirement(ExecutionVertex executionVertex) {
			this.executionVertex = executionVertex;
			this.pendingInputs.addAll(executionVertex.getInputs());
			this.numInputs = pendingInputs.size();
		}

		public ExecutionVertex getExecutionVertex() {
			return executionVertex;
		}

		public boolean pendingRequirements() {
			Iterator<IntermediateResultPartition> iter = pendingInputs.iterator();
			while (iter.hasNext()) {
				Boolean visitedPartition = visitedPartitions.get(iter.next().getPartitionId());
				if (visitedPartition == null) {
					return true;
				} else {
					if (visitedPartition) {
						availableInputs++;
					}
					iter.remove();
				}
			}
			return false;
		}

		public IntermediateResultPartition getNextRequirement() {
			return pendingInputs.pop();
		}

		public boolean needsToBeScheduled() {
			return numInputs == availableInputs;
		}

		public void inputFound() {
			availableInputs++;
		}

		@Override
		public String toString() {
			return "TaskRequirement{" +
					"executionVertex=" + executionVertex +
					", pendingInputs=" + pendingInputs.size() +
					'}';
		}
	}

	/**
	 * Action to be performed on an ExecutionVertex when it is determined to be scheduled.
	 * @param scheduleAction A ScheduleAction which receives an ExecutionVertex.
	 */
	public void setScheduleAction(ScheduleAction scheduleAction) {
		Preconditions.checkNotNull(scheduleAction);
		this.scheduleAction = scheduleAction;
	}

	/**
	 * Hook executed after backtracking finishes. Note that because of the use of futures, this may
	 * not be when the scheduleUsingBacktracking() method returns.
	 * @param postBacktrackingHook A Runnable that is executed after backtracking finishes.
	 */
	public void setPostBacktrackingHook(Runnable postBacktrackingHook) {
		Preconditions.checkNotNull(postBacktrackingHook);
		this.postBacktrackingHook = postBacktrackingHook;
	}

	/* Visit the ExecutionGraph from the previously determined sinks using a pre-order depth-first
	 * iterative traversal.
	 */
	public void scheduleUsingBacktracking() {

		while (!taskRequirements.isEmpty()) {

			final TaskRequirement taskRequirement = taskRequirements.peek();
			final ExecutionVertex task = taskRequirement.getExecutionVertex();
			task.getCurrentExecutionAttempt().setScheduled();

			if (task.getExecutionState() != ExecutionState.CREATED && task.getExecutionState() != ExecutionState.DEPLOYING) {
				LOG.debug("Resetting ExecutionVertex {} from {} to CREATED.", task, task.getExecutionState());
				task.resetForNewExecution();
				task.getCurrentExecutionAttempt().setScheduled();
			}

			if (taskRequirement.pendingRequirements()) {

				final IntermediateResultPartition resultRequired = taskRequirement.getNextRequirement();

				if (resultRequired.isLocationAvailable()) {
					ActorRef taskManager = resultRequired.getLocation().getTaskManager();

					LOG.debug("Requesting availability of IntermediateResultPartition " + resultRequired.getPartitionId());
					// pin ResulPartition for this intermediate result
					Future<Object> future = Patterns.ask(
							taskManager,
							new TaskMessages.LockResultPartition(
									resultRequired.getPartitionId(),
									// We lock this result for all consumers. We have to make sure
									// to release it once a job finishes.
									resultRequired.getNumConsumers()
							),
							Timeout.durationToTimeout(AkkaUtils.getDefaultTimeout())
					);

					/** BEGIN Asynchronous callback **/
					future.onComplete(new OnComplete<Object>() {
						@Override
						public void onComplete(Throwable failure, Object success) {
							if (success instanceof TaskMessages.LockResultPartitionReply &&
									((TaskMessages.LockResultPartitionReply) success).locked()) {
								LOG.debug("Resuming from IntermediateResultPartition " + resultRequired.getPartitionId());
								visitedPartitions.put(resultRequired.getPartitionId(), true);
								taskRequirement.inputFound();
							} else {
								// intermediate result not available
								visitedPartitions.put(resultRequired.getPartitionId(), false);
								taskRequirements.push(new TaskRequirement(resultRequired.getProducer()));
							}
							// TODO try again in case of errors?
							// continue with backtracking
							scheduleUsingBacktracking();
						}
					}, AkkaUtils.globalExecutionContext());
					/** END Asynchronous callback **/

					// interrupt backtracking here and continue once future is complete
					return;

				} else {
					visitedPartitions.put(resultRequired.getPartitionId(), false);
					taskRequirements.push(new TaskRequirement(resultRequired.getProducer()));
				}

			} else {
				taskRequirements.pop();

				if (taskRequirement.needsToBeScheduled()) {
					scheduleAction.schedule(task);
				}

			}

		}

		LOG.debug("Finished backtracking.");
		postBacktrackingHook.run();
	}

}
