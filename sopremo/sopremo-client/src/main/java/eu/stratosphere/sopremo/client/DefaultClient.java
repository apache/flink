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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
import eu.stratosphere.sopremo.execution.ExecutionResponse;
import eu.stratosphere.sopremo.execution.ExecutionRequest.ExecutionMode;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.execution.SopremoConstants;
import eu.stratosphere.sopremo.execution.SopremoExecutionProtocol;
import eu.stratosphere.sopremo.execution.SopremoID;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * @author Arvid Heise
 */
public class DefaultClient implements Closeable {
	private Configuration configuration;

	private SopremoExecutionProtocol executor;

	private InetSocketAddress serverAddress;

	private int updateTime = 5000;

	private ExecutionMode executionMode = ExecutionMode.RUN;

	/**
	 * Initializes DefaultClient.
	 */
	public DefaultClient() {
		this(new Configuration());
	}

	/**
	 * Initializes CLClient.
	 */
	public DefaultClient(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Returns the executionMode.
	 * 
	 * @return the executionMode
	 */
	public ExecutionMode getExecutionMode() {
		return this.executionMode;
	}

	/**
	 * Sets the executionMode to the specified value.
	 * 
	 * @param executionMode
	 *        the executionMode to set
	 */
	public void setExecutionMode(ExecutionMode executionMode) {
		if (executionMode == null)
			throw new NullPointerException("executionMode must not be null");

		this.executionMode = executionMode;
	}

	/**
	 * Returns the configuration.
	 * 
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return this.configuration;
	}

	/**
	 * Returns the serverAddress.
	 * 
	 * @return the serverAddress
	 */
	public InetSocketAddress getServerAddress() {
		return this.serverAddress;
	}

	/**
	 * Returns the updateTime.
	 * 
	 * @return the updateTime
	 */
	public int getUpdateTime() {
		return this.updateTime;
	}

	/**
	 * Sets the configuration to the specified value.
	 * 
	 * @param configuration
	 *        the configuration to set
	 */
	public void setConfiguration(Configuration configuration) {
		if (configuration == null)
			throw new NullPointerException("configuration must not be null");

		this.configuration = configuration;
	}

	/**
	 * Sets the serverAddress to the specified value.
	 * 
	 * @param serverAddress
	 *        the serverAddress to set
	 */
	public void setServerAddress(InetSocketAddress serverAddress) {
		if (serverAddress == null)
			throw new NullPointerException("serverAddress must not be null");

		this.serverAddress = serverAddress;
	}

	/**
	 * Sets the updateTime to the specified value.
	 * 
	 * @param updateTime
	 *        the updateTime to set
	 */
	public void setUpdateTime(int updateTime) {
		this.updateTime = updateTime;
	}

	public boolean submit(SopremoPlan plan, ProgressListener progressListener) {
		return submit(plan, progressListener, true);
	}

	public boolean submit(SopremoPlan plan, ProgressListener progressListener, boolean wait) {
		this.initConnection(progressListener);
		final ExecutionResponse response = this.sendPlan(plan, progressListener);
		if (response.getState() == ExecutionState.ERROR) {
			dealWithError(progressListener, null, "Error while waiting for job execution");
			return false;
		}

		if (wait)
			return this.waitForCompletion(response, progressListener);
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() {
		RPC.stopProxy(this.executor);
	}

	protected void sleepSafely(int updateTime) {
		try {
			Thread.sleep(updateTime);
		} catch (InterruptedException e) {
		}
	}

	private void dealWithError(ProgressListener progressListener, Exception e, final String detail) {
		progressListener.progressUpdate(ExecutionState.ERROR, detail
			+ e.getMessage());
	}

	private void initConnection(ProgressListener progressListener) {
		InetSocketAddress serverAddress = this.serverAddress;

		if (serverAddress == null) {
			String address = this.configuration.getString(SopremoConstants.SOPREMO_SERVER_IPC_ADDRESS_KEY, null);
			final int port = this.configuration.getInteger(SopremoConstants.SOPREMO_SERVER_IPC_PORT_KEY,
				SopremoConstants.DEFAULT_SOPREMO_SERVER_IPC_PORT);
			serverAddress = new InetSocketAddress(address, port);
		}

		try {
			this.executor =
				(SopremoExecutionProtocol) RPC.getProxy(SopremoExecutionProtocol.class, serverAddress,
					NetUtils.getSocketFactory());
		} catch (IOException e) {
			this.dealWithError(progressListener, e, "Error while connecting to the server");
		}
	}

	private ExecutionResponse sendPlan(SopremoPlan query, ProgressListener progressListener) {
		try {
			ExecutionRequest request = new ExecutionRequest(query);
			request.setMode(this.executionMode);
			return this.executor.execute(request);
		} catch (Exception e) {
			this.dealWithError(progressListener, e, "Error while sending the query to the server");
			return null;
		}
	}

	private boolean waitForCompletion(ExecutionResponse submissionResponse, ProgressListener progressListener) {
		ExecutionResponse lastResponse = submissionResponse;

		SopremoID submittedJobId = submissionResponse.getJobId();

		try {
			progressListener.progressUpdate(ExecutionState.ENQUEUED, lastResponse.getDetails());
			while (lastResponse.getState() == ExecutionState.ENQUEUED) {
				lastResponse = this.executor.getState(submittedJobId);
				sleepSafely(this.updateTime);
				progressListener.progressUpdate(ExecutionState.ENQUEUED, lastResponse.getDetails());
			}

			progressListener.progressUpdate(ExecutionState.RUNNING, lastResponse.getDetails());
			while (lastResponse.getState() == ExecutionState.RUNNING) {
				lastResponse = this.executor.getState(submittedJobId);
				sleepSafely(this.updateTime);
				progressListener.progressUpdate(ExecutionState.RUNNING, lastResponse.getDetails());
			}

			if (lastResponse.getState() == ExecutionState.ERROR) {
				progressListener.progressUpdate(ExecutionState.ERROR, lastResponse.getDetails());
				return false;
			}

			progressListener.progressUpdate(ExecutionState.FINISHED, lastResponse.getDetails());
			return true;
		} catch (Exception e) {
			dealWithError(progressListener, e, "Error while waiting for job execution");
			return false;
		}
	}

}
