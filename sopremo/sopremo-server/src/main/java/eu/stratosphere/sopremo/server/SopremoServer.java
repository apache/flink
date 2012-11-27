/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
import eu.stratosphere.sopremo.execution.ExecutionResponse;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.execution.LibraryTransferAgent;
import eu.stratosphere.sopremo.execution.SopremoConstants;
import eu.stratosphere.sopremo.execution.SopremoExecutionProtocol;
import eu.stratosphere.sopremo.execution.SopremoID;

/**
 * @author Arvid Heise
 */
public class SopremoServer implements SopremoExecutionProtocol, Closeable {
	
	private RPCService rpcService;

	private Configuration configuration;

	private InetSocketAddress serverAddress, jobManagerAddress;

	private ScheduledExecutorService executorService = createExecutor();

	private Map<SopremoID, SopremoJobInfo> meteorInfo =
		new ConcurrentHashMap<SopremoID, SopremoJobInfo>();

	private boolean stopped = false;

	private LibraryTransferAgent libraryTransferAgent = new LibraryTransferAgent();

	private static final Log LOG = LogFactory.getLog(SopremoServer.class);

	private final static int SLEEPINTERVAL = 1000;

	public SopremoServer() {
		this(GlobalConfiguration.getConfiguration());
	}

	public SopremoServer(Configuration configuration) {
		this.configuration = configuration;
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() {
		if (this.rpcService != null) {
			this.rpcService.shutDown();
			this.rpcService = null;
		}
		this.executorService.shutdownNow();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.meteor.execution.SopremoExecutor#execute(eu.stratosphere.meteor.execution.ExecutionRequest)
	 */
	@Override
	public ExecutionResponse execute(ExecutionRequest request) {
		SopremoID jobId = SopremoID.generate();
		LOG.info("Receive execution request for job " + jobId);
		final SopremoJobInfo info = new SopremoJobInfo(jobId, request, this.configuration);
		this.meteorInfo.put(jobId, info);
		if (request.getQuery() == null)
			info.setStatusAndDetail(ExecutionState.ERROR, "No plan submitted");
		else if (request.getMode() == null)
			info.setStatusAndDetail(ExecutionState.ERROR, "No mode set");
		else
			this.executorService.submit(new SopremoExecutionThread(info, getJobManagerAddress()));
		return this.getState(jobId);
	}

	public InetSocketAddress getJobManagerAddress() {
		InetSocketAddress serverAddress = this.jobManagerAddress;
		if (serverAddress == null) {
			final String address =
				this.configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			final int port = this.configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

			serverAddress = new InetSocketAddress(address, port);
		}
		return serverAddress;
	}

	@Override
	public LibraryCacheProfileResponse getLibraryCacheProfile(LibraryCacheProfileRequest request) throws IOException {
		return this.libraryTransferAgent.getLibraryCacheProfile(request);
	}

	public InetSocketAddress getServerAddress() {
		InetSocketAddress serverAddress = this.serverAddress;
		if (serverAddress == null) {
			final String address = this.configuration.getString(SopremoConstants.SOPREMO_SERVER_IPC_ADDRESS_KEY, null);
			final int port = this.configuration.getInteger(SopremoConstants.SOPREMO_SERVER_IPC_PORT_KEY,
				SopremoConstants.DEFAULT_SOPREMO_SERVER_IPC_PORT);

			serverAddress = new InetSocketAddress(address, port);
		}
		return serverAddress;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.meteor.execution.SopremoExecutor#getStatus(java.lang.String)
	 */
	@Override
	public ExecutionResponse getState(SopremoID jobId) {
		final SopremoJobInfo info = this.meteorInfo.get(jobId);
		if (info == null)
			return new ExecutionResponse(jobId, ExecutionState.ERROR, "Unknown job");
		return new ExecutionResponse(jobId, info.getStatus(), info.getDetail());
	}

	/**
	 * Returns the stopped.
	 * 
	 * @return the stopped
	 */
	public boolean isStopped() {
		return this.stopped;
	}

	public void setJobManagerAddress(InetSocketAddress jobManagerAddress) {
		if (jobManagerAddress == null)
			throw new NullPointerException("jobManagerAddress must not be null");

		this.jobManagerAddress = jobManagerAddress;
	}

	public void setServerAddress(InetSocketAddress rpcServerAddress) {
		if (rpcServerAddress == null)
			throw new NullPointerException("rpcServerAddress must not be null");

		this.serverAddress = rpcServerAddress;
	}

	public void start() throws IOException {
		this.startServer();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			/*
			 * (non-Javadoc)
			 * @see java.lang.Thread#run()
			 */
			@Override
			public void run() {
				SopremoServer.this.stop();
				close();
			}
		});
	}

	public void stop() {
		this.stopped = true;
	}

	@Override
	public void updateLibraryCache(LibraryCacheUpdate update) throws IOException {
		this.libraryTransferAgent.updateLibraryCache(update);
	}

	private ScheduledThreadPoolExecutor createExecutor() {
		final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
		executor.setMaximumPoolSize(1);
		return executor;
	}

	private void startServer() throws IOException {
		InetSocketAddress rpcServerAddress = getServerAddress();
		this.rpcService = new RPCService(rpcServerAddress.getPort(), 2, null);
		this.rpcService.setProtocolCallbackHandler(SopremoExecutionProtocol.class, this);
	}

	/**
	 * Entry point for the program
	 * 
	 * @param args
	 *        arguments from the command line
	 */
	@SuppressWarnings("static-access")
	public static void main(final String[] args) {

		final Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg()
			.withDescription("Specify configuration directory.").create("configDir");

		final Options options = new Options();
		options.addOption(configDirOpt);

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
			final String configDir = line.getOptionValue(configDirOpt.getOpt(), null);
			GlobalConfiguration.loadConfiguration(configDir);
		} catch (ParseException e) {
			LOG.error("CLI Parsing failed. Reason: " + e.getMessage());
			System.exit(1);
		}

		// start server
		SopremoServer sopremoServer = new SopremoServer();
		try {
			sopremoServer.start();
		} catch (IOException e) {
			LOG.error("Cannot start Sopremo server: " + StringUtils.stringifyException(e));
			sopremoServer.close();
			return;
		}

		// and wait for any shutdown signal
		while (!Thread.interrupted()) {
			// Sleep
			try {
				Thread.sleep(SLEEPINTERVAL);
			} catch (InterruptedException e) {
				break;
			}
			// Do nothing here
		}

		sopremoServer.close();
	}

}
