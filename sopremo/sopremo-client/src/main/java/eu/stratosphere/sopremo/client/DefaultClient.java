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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.esotericsoftware.kryo.io.Input;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
import eu.stratosphere.sopremo.execution.ExecutionRequest.ExecutionMode;
import eu.stratosphere.sopremo.execution.ExecutionResponse;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.execution.SopremoConstants;
import eu.stratosphere.sopremo.execution.SopremoExecutionProtocol;
import eu.stratosphere.sopremo.execution.SopremoID;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * @author Arvid Heise
 */
public class DefaultClient implements Closeable {
	/**
	 * Avoids plenty of null checks.
	 * 
	 * @author Arvid Heise
	 */
	private static class DummyListener implements ProgressListener {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.client.ProgressListener#progressUpdate(eu.stratosphere.sopremo.execution.
		 * ExecutionResponse.ExecutionState, java.lang.String)
		 */
		@Override
		public void progressUpdate(ExecutionState state, String detail) {
		}
	}

	private Configuration configuration;

	private RPCService rpcService;
	
	private SopremoExecutionProtocol executor;

	private InetSocketAddress serverAddress;

	private int updateTime = 5000;

	private ExecutionMode executionMode = ExecutionMode.RUN;

	/**
	 * Initializes DefaultClient.
	 */
	public DefaultClient() {
		this(GlobalConfiguration.getConfiguration());
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
		if (progressListener == null)
			progressListener = new DummyListener();
		this.initConnection(progressListener);
		if (!this.transferLibraries(plan, progressListener))
			return false;

		final ExecutionResponse response = this.sendPlan(plan, progressListener);
		if (response == null)
			return false;

		if (response.getState() == ExecutionState.ERROR) {
			dealWithError(progressListener, null, "Error while submitting to job execution");
			return false;
		}

		if (wait)
			return this.waitForCompletion(response, progressListener);
		return true;
	}

	private boolean transferLibraries(SopremoPlan plan, ProgressListener progressListener) {
		final JobID dummyKey = new JobID();
		List<String> requiredLibraries = new ArrayList<String>(plan.getRequiredPackages());
		try {
			progressListener.progressUpdate(ExecutionState.SETUP, "");
			List<Path> libraryPaths = new ArrayList<Path>();
			for (String library : requiredLibraries) {
				final Input dis = new Input(new FileInputStream(library));
				final Path libraryPath = new Path(library);
				LibraryCacheManager.addLibrary(dummyKey, libraryPath, (int) new File(library).length(), dis);
				dis.close();
				libraryPaths.add(libraryPath);
			}

			LibraryCacheManager.register(dummyKey, libraryPaths.toArray(new Path[libraryPaths.size()]));
			LibraryCacheProfileRequest request = new LibraryCacheProfileRequest();
			final String[] internalJarNames = LibraryCacheManager.getRequiredJarFiles(dummyKey);
			request.setRequiredLibraries(internalJarNames);

			// Send the request
			LibraryCacheProfileResponse response = null;
			response = this.executor.getLibraryCacheProfile(request);

			// Check response and transfer libraries if necessary
			for (int k = 0; k < internalJarNames.length; k++)
				if (!response.isCached(k)) {
					final String library = internalJarNames[k];
					progressListener.progressUpdate(ExecutionState.SETUP, "Transfering " + requiredLibraries.get(k));
					LibraryCacheUpdate update = new LibraryCacheUpdate(library);
					this.executor.updateLibraryCache(update);
				}

			// now change the names of the required libraries to the internal names
			for (int index = 0; index < internalJarNames.length; index++)
				requiredLibraries.set(index, internalJarNames[index]);
			plan.setRequiredPackages(requiredLibraries);

			return true;
		} catch (IOException e) {
			dealWithError(progressListener, e, "Cannot transfer libraries");
			return false;
		} finally {
			try {
				LibraryCacheManager.unregister(dummyKey);
			} catch (IOException e) {
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() {
		this.rpcService.shutDown();
	}

	protected void sleepSafely(int updateTime) {
		try {
			Thread.sleep(updateTime);
		} catch (InterruptedException e) {
		}
	}

	private void dealWithError(ProgressListener progressListener, Exception e, final String detail) {
		progressListener.progressUpdate(ExecutionState.ERROR, String.format("%s: %s", detail,
			StringUtils.stringifyException(e)));
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
			this.rpcService = new RPCService();
			this.executor = this.rpcService.getProxy(serverAddress, SopremoExecutionProtocol.class);
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
