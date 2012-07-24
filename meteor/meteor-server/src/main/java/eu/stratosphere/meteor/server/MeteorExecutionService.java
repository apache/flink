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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import eu.stratosphere.meteor.execution.ExecutionRequest;
import eu.stratosphere.meteor.execution.ExecutionResponse;
import eu.stratosphere.meteor.execution.ExecutionResponse.ExecutionStatus;
import eu.stratosphere.meteor.execution.MeteorConstants;
import eu.stratosphere.meteor.execution.MeteorExecutor;
import eu.stratosphere.meteor.execution.MeteorID;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.ipc.RPC.Server;

/**
 * @author Arvid Heise
 */
public class MeteorExecutionService implements MeteorExecutor, Closeable {
	private Server server;

	private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

	/**
	 * Initializes MeteorExecutionService.
	 */
	public MeteorExecutionService() throws IOException {
		this.startServer();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			/*
			 * (non-Javadoc)
			 * @see java.lang.Thread#run()
			 */
			@Override
			public void run() {
				MeteorExecutionService.this.server.stop();
			}
		});
	}

	protected void startServer() throws IOException {
		final String address = GlobalConfiguration.getString(MeteorConstants.METEOR_SERVER_IPC_ADDRESS_KEY, null);
		final int port = GlobalConfiguration.getInteger(MeteorConstants.METEOR_SERVER_IPC_PORT_KEY,
			MeteorConstants.DEFAULT_METEOR_SERVER_IPC_PORT);
		final InetSocketAddress rpcServerAddress = new InetSocketAddress(address, port);

		final int handlerCount = GlobalConfiguration.getInteger("jobmanager.rpc.numhandler", 3);
		this.server = RPC.getServer(this, rpcServerAddress.getHostName(), rpcServerAddress.getPort(),
			handlerCount);
		this.server.start();
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		this.server.stop();
	}

	private Map<MeteorID, MeteorExecutionEnvironment> meteorInfo =
		new ConcurrentHashMap<MeteorID, MeteorExecutionEnvironment>();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.meteor.execution.MeteorExecutor#execute(eu.stratosphere.meteor.execution.ExecutionRequest)
	 */
	@Override
	public ExecutionResponse execute(ExecutionRequest request) {
		MeteorID jobId = new MeteorID();
		final MeteorExecutionEnvironment info = new MeteorExecutionEnvironment(request);
		this.meteorInfo.put(jobId, info);
		this.executorService.submit(new MeteorExecutionThread(info));
		return this.getStatus(jobId);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.meteor.execution.MeteorExecutor#getStatus(java.lang.String)
	 */
	@Override
	public ExecutionResponse getStatus(MeteorID jobId) {
		final MeteorExecutionEnvironment info = this.meteorInfo.get(jobId);
		if (info == null)
			return new ExecutionResponse(jobId, ExecutionStatus.ERROR, "Unknown job");
		return new ExecutionResponse(jobId, info.getStatus(), info.getDetail());
	}
}
