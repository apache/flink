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
package eu.stratosphere.sopremo.server;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;

/**
 * @author Arvid Heise
 */
public class SopremoJobInfo {
	private ExecutionRequest initialRequest;

	private Configuration configuration;

	private JobClient jobClient;

	public SopremoJobInfo(ExecutionRequest initialRequest, Configuration configuration) {
		this.initialRequest = initialRequest;
		this.configuration = configuration;
	}

	/**
	 * Returns the configuration.
	 * 
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return this.configuration;
	}

	private ExecutionState status = ExecutionState.ENQUEUED;

	private String detail = "";

	public String getDetail() {
		return this.detail;
	}

	/**
	 * Sets the jobClient to the specified value.
	 * 
	 * @param jobClient
	 *        the jobClient to set
	 */
	public void setJobClient(JobClient jobClient) {
		if (jobClient == null)
			throw new NullPointerException("jobClient must not be null");

		this.jobClient = jobClient;
	}

	/**
	 * Returns the jobClient.
	 * 
	 * @return the jobClient
	 */
	public JobClient getJobClient() {
		return this.jobClient;
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
	public ExecutionState getStatus() {
		return this.status;
	}

	/**
	 * @param running
	 * @param string
	 */
	public void setStatusAndDetail(ExecutionState status, String detail) {
		if (status == null)
			throw new NullPointerException("status must not be null");
		if (detail == null)
			throw new NullPointerException("detail must not be null");
		this.status = status;
		this.detail = detail;
	}
}
