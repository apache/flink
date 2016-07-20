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

package org.apache.flink.runtime.jobgraph;

/**
 * Possible states of a job once it has been accepted by the job manager.
 */
public enum JobStatus {

	/** Job is newly created, no task has started to run. */
	CREATED(TerminalState.NON_TERMINAL),

	/** Some tasks are scheduled or running, some may be pending, some may be finished. */
	RUNNING(TerminalState.NON_TERMINAL),

	/** The job has failed and is currently waiting for the cleanup to complete */
	FAILING(TerminalState.NON_TERMINAL),
	
	/** The job has failed with a non-recoverable task failure */
	FAILED(TerminalState.GLOBALLY),

	/** Job is being cancelled */
	CANCELLING(TerminalState.NON_TERMINAL),
	
	/** Job has been cancelled */
	CANCELED(TerminalState.GLOBALLY),

	/** All of the job's tasks have successfully finished. */
	FINISHED(TerminalState.GLOBALLY),
	
	/** The job is currently undergoing a reset and total restart */
	RESTARTING(TerminalState.NON_TERMINAL),

	/**
	 * The job has been suspended which means that it has been stopped but not been removed from a
	 * potential HA job store.
	 */
	SUSPENDED(TerminalState.LOCALLY);
	
	// --------------------------------------------------------------------------------------------

	enum TerminalState {
		NON_TERMINAL,
		LOCALLY,
		GLOBALLY
	}
	
	private final TerminalState terminalState;
	
	JobStatus(TerminalState terminalState) {
		this.terminalState = terminalState;
	}
	
	public boolean isGloballyTerminalState() {
		return terminalState == TerminalState.GLOBALLY;
	}

	public boolean isTerminalState() {
		return terminalState != TerminalState.NON_TERMINAL;
	}
}


