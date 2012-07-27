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
package eu.stratosphere.meteor.server;

import eu.stratosphere.meteor.execution.ExecutionRequest;
import eu.stratosphere.meteor.execution.ExecutionResponse.ExecutionStatus;

/**
 * @author Arvid Heise
 */
public class MeteorExecutionEnvironment {
	private ExecutionRequest initialRequest;

	public MeteorExecutionEnvironment(ExecutionRequest initialRequest) {
		this.initialRequest = initialRequest;
	}

	private ExecutionStatus status = ExecutionStatus.ENQUEUED;

	private String detail;

	public String getDetail() {
		return detail;
	}
	
	/**
	 * Sets the detail to the specified value.
	 *
	 * @param detail the detail to set
	 */
	public void setDetail(String detail) {
		if (detail == null)
			throw new NullPointerException("detail must not be null");

		this.detail = detail;
	}

	/**
	 * Returns the initialRequest.
	 * 
	 * @return the initialRequest
	 */
	public ExecutionRequest getInitialRequest() {
		return this.initialRequest;
	}
	
	/**
	 * Returns the status.
	 * 
	 * @return the status
	 */
	public ExecutionStatus getStatus() {
		return this.status;
	}
}
