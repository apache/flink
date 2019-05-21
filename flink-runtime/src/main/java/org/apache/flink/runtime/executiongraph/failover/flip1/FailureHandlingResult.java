/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Result containing the tasks to be restarted upon a task failure.
 * Also contains the reason if the failure is not recoverable(non-recoverable
 * failure type or restarting suppressed by restart strategy).
 */
public class FailureHandlingResult {

	/** Task vertices to be restarted to recover from the failure. */
	private Set<ExecutionVertexID> verticesToBeRestarted;

	/** Delay before the restarting can be conducted. Negative values means no restart should be conducted. */
	private long restartDelayMS = -1;

	/** Reason why the failure is not recoverable. */
	private Throwable error;

	/**
	 * Creates a result of a set of tasks to be restarted to recover from the failure.
	 *
	 * @param verticesToBeRestarted containing task vertices to be restarted to recover from the failure
	 * @param restartDelayMS indicate a delay before conducting the restart
	 */
	public FailureHandlingResult(Set<ExecutionVertexID> verticesToBeRestarted, long restartDelayMS) {
		checkState(restartDelayMS >= 0);

		this.verticesToBeRestarted = checkNotNull(verticesToBeRestarted);
		this.restartDelayMS = restartDelayMS;
	}

	/**
	 * Creates a result that the failure is not recoverable and no restarting should be conducted.
	 *
	 * @param error reason why the failure is not recoverable
	 */
	public FailureHandlingResult(Throwable error) {
		this.error = checkNotNull(error);
	}

	/**
	 * Returns the tasks to be restarted.
	 *
	 * @return the tasks to be restarted
	 */
	public Set<ExecutionVertexID> getVerticesToBeRestarted() {
		return verticesToBeRestarted;
	}

	/**
	 * Returns the delay before the restarting.
	 *
	 * @return the delay before the restarting
	 */
	public long getRestartDelayMS() {
		return restartDelayMS;
	}

	/**
	 * Returns whether the restarting can be conducted.
	 *
	 * @return whether the restarting can be conducted
	 */
	public boolean canRestart() {
		return error == null && restartDelayMS >= 0;
	}

	/**
	 * Returns reason why the restarting cannot be conducted.
	 *
	 * @return reason why the restarting cannot be conducted
	 */
	public Throwable getError() {
		return error;
	}
}
