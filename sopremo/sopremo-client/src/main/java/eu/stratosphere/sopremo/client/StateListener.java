/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.client;

import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;

/**
 * @author Arvid Heise
 */
public abstract class StateListener implements ProgressListener {
	private ExecutionState lastState;

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.client.ProgressListener#progressUpdate(eu.stratosphere.sopremo.execution.ExecutionResponse
	 * .ExecutionStatus, java.lang.String)
	 */
	@Override
	public void progressUpdate(ExecutionState status, String detail) {
		if (this.lastState != status) {
			statusChanged(this.lastState = status, detail);
		}
	}

	public abstract void statusChanged(ExecutionState executionStatus, String detail);

	/**
	 * Returns the lastState.
	 * 
	 * @return the lastState
	 */
	public ExecutionState getLastState() {
		return this.lastState;
	}
}
