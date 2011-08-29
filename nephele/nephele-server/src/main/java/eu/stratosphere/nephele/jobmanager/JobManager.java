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

/**
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

package eu.stratosphere.nephele.jobmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.client.AbstractJobResult;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.discovery.DiscoveryException;
import eu.stratosphere.nephele.discovery.DiscoveryService;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.local.LocalInstanceManager;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.ipc.Server;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitManager;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitWrapper;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.multicast.MulticastManager;
import eu.stratosphere.nephele.optimizer.Optimizer;
import eu.stratosphere.nephele.profiling.JobManagerProfiler;
import eu.stratosphere.nephele.profiling.ProfilingUtils;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.protocols.InputSplitProviderProtocol;
import eu.stratosphere.nephele.protocols.JobManagerProtocol;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionWrapper;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.SerializableArrayList;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * In Nephele the job manager is the central component for communication with clients, creating
 * schedules for incoming jobs and supervise their execution. A job manager may only exist once in
 * the system and its address must be known the clients.
 * Task managers can discover the job manager by means of an UDP broadcast and afterwards advertise
 * themselves as new workers for tasks.
 * 
 * @author warneke
 */
public class JobManager implements DeploymentManager, ExtendedManagementProtocol, InputSplitProviderProtocol,
		JobManagerProtocol, ChannelLookupProtocol, JobStatusListener {

	private static final Log LOG = LogFactory.getLog(JobManager.class);

	private Server jobManagerServer = null;

	private final JobManagerProfiler profiler;

	private final Optimizer optimizer;

	private final EventCollector eventCollector;

	private final InputSplitManager inputSplitManager;

	private final AbstractScheduler scheduler;

	private final MulticastManager multicastManager;

	private InstanceManager instanceManager;

	private final int recommendedClientPollingInterval;

	private final ExecutorService executorService = Executors.newSingleThreadExecutor();

	private final static int SLEEPINTERVAL = 1000;

	private final static int FAILURERETURNCODE = -1;

	private boolean isShutDown = false;

	/**
	 * Constructs a new job manager, starts its discovery service and its IPC service.
	 */
	public JobManager(final String configDir, final String executionMode) {

		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);

		final String ipcAddressString = GlobalConfiguration
			.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);

		InetAddress ipcAddress = null;
		if (ipcAddressString != null) {
			try {
				ipcAddress = InetAddress.getByName(ipcAddressString);
			} catch (UnknownHostException e) {
				LOG.error("Cannot convert " + ipcAddressString + " to an IP address: "
					+ StringUtils.stringifyException(e));
				System.exit(FAILURERETURNCODE);
			}
		}

		final int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		// First of all, start discovery manager
		try {
			DiscoveryService.startDiscoveryService(ipcAddress, ipcPort);
		} catch (DiscoveryException e) {
			LOG.error("Cannot start discovery manager: " + StringUtils.stringifyException(e));
			System.exit(FAILURERETURNCODE);
		}

		// Read the suggested client polling interval
		this.recommendedClientPollingInterval = GlobalConfiguration.getInteger("jobclient.polling.internval", 5);

		// Load the job progress collector
		this.eventCollector = new EventCollector(this.recommendedClientPollingInterval);

		// Load the input split manager
		this.inputSplitManager = new InputSplitManager();

		// Determine own RPC address
		final InetSocketAddress rpcServerAddress = new InetSocketAddress(ipcAddress, ipcPort);

		// Start job manager's IPC server
		try {
			final int handlerCount = GlobalConfiguration.getInteger("jobmanager.rpc.numhandler", 3);
			this.jobManagerServer = RPC.getServer(this, rpcServerAddress.getHostName(), rpcServerAddress.getPort(),
				handlerCount);
			this.jobManagerServer.start();
		} catch (IOException ioe) {
			LOG.error("Cannot start RPC server: " + StringUtils.stringifyException(ioe));
			System.exit(FAILURERETURNCODE);
		}

		LOG.info("Starting job manager in " + executionMode + " mode");

		// Try to load the instance manager for the given execution mode
		// Try to load the scheduler for the given execution mode
		if ("local".equals(executionMode)) {
			// TODO: Find a better solution for that
			try {
				LibraryCacheManager.setLocalMode();
			} catch (IOException e) {
				LOG.error(e);
			}
			try {
				this.instanceManager = new LocalInstanceManager(configDir);
			} catch (RuntimeException rte) {
				LOG.fatal("Cannot instantiate local instance manager: " + StringUtils.stringifyException(rte));
				System.exit(FAILURERETURNCODE);
			}
		} else {
			final String instanceManagerClassName = JobManagerUtils.getInstanceManagerClassName(executionMode);
			LOG.info("Trying to load " + instanceManagerClassName + " as instance manager");
			this.instanceManager = JobManagerUtils.loadInstanceManager(instanceManagerClassName);
			if (this.instanceManager == null) {
				LOG.error("UNable to load instance manager " + instanceManagerClassName);
				System.exit(FAILURERETURNCODE);
			}
		}

		// Try to load the scheduler for the given execution mode
		final String schedulerClassName = JobManagerUtils.getSchedulerClassName(executionMode);
		LOG.info("Trying to load " + schedulerClassName + " as scheduler");

		// Try to get the instance manager class name
		this.scheduler = JobManagerUtils.loadScheduler(schedulerClassName, this, this.instanceManager);
		if (this.scheduler == null) {
			LOG.error("Unable to load scheduler " + schedulerClassName);
			System.exit(FAILURERETURNCODE);
		}

		// Create multicastManager
		this.multicastManager = new MulticastManager(this.scheduler);

		// Load profiler if it should be used
		if (GlobalConfiguration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY, false)) {
			final String profilerClassName = GlobalConfiguration.getString(ProfilingUtils.JOBMANAGER_CLASSNAME_KEY,
				null);
			if (profilerClassName == null) {
				LOG.error("Cannot find class name for the profiler");
				System.exit(FAILURERETURNCODE);
			}
			this.profiler = ProfilingUtils.loadJobManagerProfiler(profilerClassName, ipcAddress);
			if (this.profiler == null) {
				LOG.error("Cannot load profiler");
				System.exit(FAILURERETURNCODE);
			}
		} else {
			this.profiler = null;
			LOG.debug("Profiler disabled");
		}

		// Load optimizer if it should be used
		if (GlobalConfiguration.getBoolean("jobmanager.optimizer.enable", false)) {
			final String optimizerClassName = GlobalConfiguration.getString("jobmanager.optimizer.classname", null);
			if (optimizerClassName == null) {
				LOG.error("Cannot find class name for the optimizer");
				System.exit(FAILURERETURNCODE);
			}
			this.optimizer = loadOptimizer(optimizerClassName);
		} else {
			this.optimizer = null;
			LOG.debug("Optimizer disabled");
		}

		// Add shutdown hook for clean up tasks
		Runtime.getRuntime().addShutdownHook(new JobManagerCleanUp(this));

	}

	@SuppressWarnings("unchecked")
	private Optimizer loadOptimizer(String optimizerClassName) {

		final Class<? extends Optimizer> optimizerClass;
		try {
			optimizerClass = (Class<? extends Optimizer>) Class.forName(optimizerClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("Cannot find class " + optimizerClassName + ": " + StringUtils.stringifyException(e));
			return null;
		}

		Optimizer optimizer = null;

		try {
			optimizer = optimizerClass.newInstance();
		} catch (InstantiationException e) {
			LOG.error("Cannot create optimizer: " + StringUtils.stringifyException(e));
			return null;
		} catch (IllegalAccessException e) {
			LOG.error("Cannot create optimizer: " + StringUtils.stringifyException(e));
			return null;
		} catch (IllegalArgumentException e) {
			LOG.error("Cannot create optimizer: " + StringUtils.stringifyException(e));
			return null;
		}

		return optimizer;
	}

	/**
	 * This is the main
	 */
	public void runTaskLoop() {

		while (!Thread.interrupted()) {

			// Sleep
			try {
				Thread.sleep(SLEEPINTERVAL);
			} catch (InterruptedException e) {
				break;
			}

			// Do nothing here
		}
	}

	public synchronized void shutdown() {

		if (this.isShutDown) {
			return;
		}

		// Stop instance manager
		if (this.instanceManager != null) {
			this.instanceManager.shutdown();
		}

		// Stop the discovery service
		DiscoveryService.stopDiscoveryService();

		// Stop profiling if enabled
		if (this.profiler != null) {
			this.profiler.shutdown();
		}

		// Stop RPC server
		if (this.jobManagerServer != null) {
			this.jobManagerServer.stop();
		}

		// Stop the executor service
		if (this.executorService != null) {
			this.executorService.shutdown();

		}

		// Stop and clean up the job progress collector
		if (this.eventCollector != null) {
			this.eventCollector.shutdown();
		}

		// Finally, shut down the scheduler
		if (this.scheduler != null) {
			this.scheduler.shutdown();
		}

		this.isShutDown = true;
		LOG.debug("Shutdown of job manager completed");
	}

	/**
	 * Entry point for the program
	 * 
	 * @param args
	 *        arguments from the command line
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {

		final Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg()
			.withDescription("Specify configuration directory.").create("configDir");

		final Option executionModeOpt = OptionBuilder.withArgName("execution mode").hasArg()
			.withDescription("Specify execution mode.").create("executionMode");

		final Options options = new Options();
		options.addOption(configDirOpt);
		options.addOption(executionModeOpt);

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			LOG.error("CLI Parsing failed. Reason: " + e.getMessage());
			System.exit(FAILURERETURNCODE);
		}

		final String configDir = line.getOptionValue(configDirOpt.getOpt(), null);
		final String executionMode = line.getOptionValue(executionModeOpt.getOpt(), "local");

		// Create a new job manager object
		JobManager jobManager = new JobManager(configDir, executionMode);

		// Run the main task loop
		jobManager.runTaskLoop();

		// Clean up task are triggered through a shutdown hook
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph job) throws IOException {

		// First check if job is null
		if (job == null) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Submitted job is null!");
			return result;
		}

		LOG.debug("Submitted job " + job.getName() + " is not null");

		// Check if any vertex of the graph has null edges
		AbstractJobVertex jv = job.findVertexWithNullEdges();
		if (jv != null) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, "Vertex "
				+ jv.getName() + " has at least one null edge");
			return result;
		}

		LOG.debug("Submitted job " + job.getName() + " has no null edges");

		// Next, check if the graph is weakly connected
		if (!job.isWeaklyConnected()) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Job graph is not weakly connected");
			return result;
		}

		LOG.debug("The graph of job " + job.getName() + " is weakly connected");

		// Check if job graph has cycles
		if (!job.isAcyclic()) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Job graph is not a DAG");
			return result;
		}

		LOG.debug("The graph of job " + job.getName() + " is acyclic");

		// Check constrains on degree
		jv = job.areVertexDegreesCorrect();
		if (jv != null) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Degree of vertex " + jv.getName() + " is incorrect");
			return result;
		}

		LOG.debug("All vertices of job " + job.getName() + " have the correct degree");

		if (!job.isInstanceDependencyChainAcyclic()) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"The dependency chain for instance sharing contains a cycle");

			return result;
		}

		LOG.debug("The dependency chain for instance sharing is acyclic");

		// Try to create initial execution graph from job graph
		LOG.info("Creating initial execution graph from job graph " + job.getName());
		ExecutionGraph eg = null;

		try {
			eg = new ExecutionGraph(job, this.instanceManager);
		} catch (GraphConversionException gce) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, gce.getMessage());
			return result;
		}

		// Check if profiling should be enabled for this job
		boolean profilingEnabled = false;
		if (this.profiler != null && job.getJobConfiguration().getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
			profilingEnabled = true;
		}

		// Register job with the progress collector
		if (this.eventCollector != null) {
			this.eventCollector.registerJob(eg, profilingEnabled);
		}

		// Check if profiling should be enabled for this job
		if (profilingEnabled) {
			this.profiler.registerProfilingJob(eg);

			if (this.eventCollector != null) {
				this.profiler.registerForProfilingData(eg.getJobID(), this.eventCollector);
			}
		}

		// Register job with the dynamic input split assigner
		this.inputSplitManager.registerJob(eg);

		// Perform graph optimizations
		if (this.optimizer != null) {
			this.optimizer.optimize(eg);
		}

		// Register for updates on the job status
		eg.registerJobStatusListener(this);

		// Schedule job
		LOG.info("Scheduling job " + job.getName());
		try {
			this.scheduler.schedulJob(eg);
		} catch (SchedulingException e) {
			unregisterJob(eg);
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, e.getMessage());
			return result;
		}

		// Return on success
		return new JobSubmissionResult(AbstractJobResult.ReturnCode.SUCCESS, null);
	}

	/**
	 * This method is a convenience method to unregister a job from all of
	 * Nephele's monitoring, profiling and optimization components at once.
	 * Currently, it is only being used to unregister from profiling (if activated).
	 * 
	 * @param executionGraph
	 *        the execution graph to remove from the job manager
	 */
	private void unregisterJob(final ExecutionGraph executionGraph) {

		// Remove job from profiler (if activated)
		if (this.profiler != null
			&& executionGraph.getJobConfiguration().getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
			this.profiler.unregisterProfilingJob(executionGraph);

			if (this.eventCollector != null) {
				this.profiler.unregisterFromProfilingData(executionGraph.getJobID(), this.eventCollector);
			}
		}

		// Remove job from input split manager
		if (this.inputSplitManager != null) {
			this.inputSplitManager.unregisterJob(executionGraph);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendHeartbeat(final InstanceConnectionInfo instanceConnectionInfo,
			final HardwareDescription hardwareDescription) {

		// Delegate call to instance manager
		if (this.instanceManager != null) {

			final Runnable heartBeatRunnable = new Runnable() {

				@Override
				public void run() {
					instanceManager.reportHeartBeat(instanceConnectionInfo, hardwareDescription);
				}
			};

			this.executorService.execute(heartBeatRunnable);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateTaskExecutionState(final TaskExecutionState executionState) throws IOException {

		// Ignore calls with executionResult == null
		if (executionState == null) {
			LOG.error("Received call to updateTaskExecutionState with executionState == null");
			return;
		}

		ExecutionGraph eg = this.scheduler.getExecutionGraphByID(executionState.getJobID());
		if (eg == null) {
			LOG.error("Cannot find execution graph for ID " + executionState.getJobID() + " to change state to "
				+ executionState.getExecutionState());
			return;
		}

		final ExecutionVertex vertex = eg.getVertexByID(executionState.getID());
		if (vertex == null) {
			LOG.error("Cannot find vertex with ID " + executionState.getID() + " of job " + eg.getJobID()
				+ " to change state to " + executionState.getExecutionState());
			return;
		}

		final Runnable taskStateChangeRunnable = new Runnable() {

			@Override
			public void run() {

				// The registered listeners of the vertex will make sure the appropriate actions are taken
				vertex.getEnvironment().changeExecutionState(executionState.getExecutionState(),
					executionState.getDescription());
			}
		};

		// Hand over to the executor service, as this may result in a longer operation with several IPC operations
		this.executorService.execute(taskStateChangeRunnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobCancelResult cancelJob(final JobID jobID) throws IOException {

		LOG.info("Trying to cancel job with ID " + jobID);

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			return new JobCancelResult(ReturnCode.ERROR, "Cannot find job with ID " + jobID);
		}

		final Runnable cancelJobRunnable = new Runnable() {

			@Override
			public void run() {
				final TaskCancelResult errorResult = cancelJob(eg);
				if (errorResult != null) {
					LOG.error("Cannot cancel job " + jobID + ": " + errorResult);
				}
			}
		};
		this.executorService.execute(cancelJobRunnable);

		LOG.info("Cancel of job " + jobID + " successfully triggered");

		return new JobCancelResult(AbstractJobResult.ReturnCode.SUCCESS, null);
	}

	/**
	 * Cancels all the tasks in the current and upper stages of the
	 * given execution graph.
	 * 
	 * @param eg
	 *        the execution graph representing the job to cancel.
	 * @return <code>null</code> no error occurred during the cancel attempt,
	 *         otherwise the returned object will describe the error
	 */
	private TaskCancelResult cancelJob(ExecutionGraph eg) {

		TaskCancelResult errorResult = null;

		/**
		 * Cancel all nodes in the current and upper execution stages.
		 */
		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, eg.getIndexOfCurrentExecutionStage(),
			false, true);
		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			final TaskCancelResult result = vertex.cancelTask();
			if (result.getReturnCode() == AbstractTaskResult.ReturnCode.ERROR) {
				errorResult = result;
			}
		}

		return errorResult;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobProgressResult getJobProgress(JobID jobID) throws IOException {

		if (this.eventCollector == null) {
			return new JobProgressResult(ReturnCode.ERROR, "JobManager does not support progress reports for jobs",
				null);
		}

		final SerializableArrayList<AbstractEvent> eventList = new SerializableArrayList<AbstractEvent>();
		this.eventCollector.getEventsForJob(jobID, eventList, false);

		return new JobProgressResult(ReturnCode.SUCCESS, null, eventList);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ConnectionInfoLookupResponse lookupConnectionInfo(final InstanceConnectionInfo caller, final JobID jobID,
			final ChannelID sourceChannelID) {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		final AbstractOutputChannel<? extends Record> outputChannel = eg.getOutputChannelByID(sourceChannelID);

		if (outputChannel == null) {

			AbstractInputChannel<? extends Record> inputChannel = eg.getInputChannelByID(sourceChannelID);

			final ChannelID connectedChannelID = inputChannel.getConnectedChannelID();
			final ExecutionVertex connectedVertex = eg.getVertexByChannelID(connectedChannelID);

			final AbstractInstance assignedInstance = connectedVertex.getAllocatedResource().getInstance();
			if (assignedInstance == null) {
				LOG.error("Cannot resolve lookup: vertex found for channel ID " + connectedChannelID
						+ " but no instance assigned");
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			// Check execution state
			final ExecutionState executionState = connectedVertex.getExecutionState();
			if (executionState != ExecutionState.RUNNING && executionState != ExecutionState.FINISHING) {
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			if (assignedInstance.getInstanceConnectionInfo().equals(caller)) {
				// Receiver runs on the same task manager
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(connectedChannelID);
			} else {
				// Receiver runs on a different task manager
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(assignedInstance
					.getInstanceConnectionInfo());
			}
		}

		if (outputChannel.isBroadcastChannel()) {

			return multicastManager.lookupConnectionInfo(caller, jobID, sourceChannelID);

		} else {

			// Find vertex of connected input channel
			final ExecutionVertex targetVertex = eg.getVertexByChannelID(outputChannel.getConnectedChannelID());
			if (targetVertex == null) {
				LOG.error("Cannot find vertex to input channel " + outputChannel.getConnectedChannelID());
				return ConnectionInfoLookupResponse.createReceiverNotFound();
			}

			// Check execution state
			final ExecutionState executionState = targetVertex.getExecutionState();
			if (executionState != ExecutionState.RUNNING && executionState != ExecutionState.FINISHING) {
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			final AbstractInstance assignedInstance = targetVertex.getAllocatedResource().getInstance();
			if (assignedInstance == null) {
				LOG.error("Cannot resolve lookup: vertex found for channel ID "
						+ outputChannel.getConnectedChannelID()
						+ " but no instance assigned");
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			if (assignedInstance.getInstanceConnectionInfo().equals(caller)) {
				// Receiver runs on the same task manager
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(outputChannel
						.getConnectedChannelID());
			} else {
				// Receiver runs on a different task manager
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(assignedInstance
						.getInstanceConnectionInfo());
			}
		}
		// LOG.error("Receiver(s) not found");

		// return ConnectionInfoLookupResponse.createReceiverNotFound();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ManagementGraph getManagementGraph(final JobID jobID) throws IOException {

		final ManagementGraph mg = this.eventCollector.getManagementGraph(jobID);
		if (mg == null) {
			throw new IOException("Cannot find job with ID " + jobID);
		}

		return mg;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) throws IOException {

		if (this.instanceManager != null) {
			return this.instanceManager.getNetworkTopology(jobID);
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IntegerRecord getRecommendedPollingInterval() throws IOException {

		return new IntegerRecord(this.recommendedClientPollingInterval);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<RecentJobEvent> getRecentJobs() throws IOException {

		final List<RecentJobEvent> eventList = new SerializableArrayList<RecentJobEvent>();

		if (this.eventCollector == null) {
			throw new IOException("No instance of the event collector found");
		}

		this.eventCollector.getRecentJobs(eventList);

		return eventList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AbstractEvent> getEvents(JobID jobID) throws IOException {

		final List<AbstractEvent> eventList = new SerializableArrayList<AbstractEvent>();

		if (this.eventCollector == null) {
			throw new IOException("No instance of the event collector found");
		}

		this.eventCollector.getEventsForJob(jobID, eventList, true);

		return eventList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cancelTask(JobID jobID, ManagementVertexID id) throws IOException {
		// TODO Auto-generated method stub
		LOG.debug("Cancelling job " + jobID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killInstance(StringRecord instanceName) throws IOException {
		// TODO Auto-generated method stub
		LOG.debug("Killing instance " + instanceName);
	}

	/**
	 * Collects all vertices with checkpoints from the given execution graph and advices the corresponding task managers
	 * to remove those checkpoints.
	 * 
	 * @param executionGraph
	 *        the execution graph from which the checkpoints shall be removed
	 */
	private void removeAllCheckpoints(ExecutionGraph executionGraph) {

		final InternalJobStatus jobStatus = executionGraph.getJobStatus();
		if (jobStatus != InternalJobStatus.FINISHED) {
			LOG.error("removeAllCheckpoints called for an unsuccesfull job, ignoring request");
		}

		final List<ExecutionVertex> verticesWithCheckpoints = executionGraph.getVerticesWithCheckpoints();
		// Group vertex IDs by assigned instance
		final Map<AbstractInstance, SerializableArrayList<ExecutionVertexID>> instanceMap = new HashMap<AbstractInstance, SerializableArrayList<ExecutionVertexID>>();
		final Iterator<ExecutionVertex> it = verticesWithCheckpoints.iterator();
		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			final AllocatedResource allocatedResource = vertex.getAllocatedResource();
			if (allocatedResource == null) {
				continue;
			}

			final AbstractInstance abstractInstance = allocatedResource.getInstance();
			if (abstractInstance == null) {
				continue;
			}

			SerializableArrayList<ExecutionVertexID> vertexIDs = instanceMap.get(abstractInstance);
			if (vertexIDs == null) {
				vertexIDs = new SerializableArrayList<ExecutionVertexID>();
				instanceMap.put(abstractInstance, vertexIDs);
			}
			vertexIDs.add(vertex.getID());
		}

		// Finally, trigger the removal of the checkpoints at each instance
		final Iterator<Map.Entry<AbstractInstance, SerializableArrayList<ExecutionVertexID>>> it2 = instanceMap
			.entrySet().iterator();
		while (it2.hasNext()) {

			final Map.Entry<AbstractInstance, SerializableArrayList<ExecutionVertexID>> entry = it2.next();
			final AbstractInstance abstractInstance = entry.getKey();
			if (abstractInstance == null) {
				LOG.error("Cannot remove checkpoint: abstractInstance is null");
				continue;
			}

			try {
				abstractInstance.removeCheckpoints(entry.getValue());
			} catch (IOException ioe) {
				LOG.error(StringUtils.stringifyException(ioe));
			}
		}

	}

	/**
	 * Tests whether the job manager has been shut down completely.
	 * 
	 * @return <code>true</code> if the job manager has been shut down completely, <code>false</code> otherwise
	 */
	public synchronized boolean isShutDown() {

		return this.isShutDown;
	}

	/**
	 * {@inheritDoc}
	 */
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {

		// Delegate call to the instance manager
		if (this.instanceManager != null) {
			return this.instanceManager.getMapOfAvailableInstanceTypes();
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void jobStatusHasChanged(ExecutionGraph executionGraph, InternalJobStatus newJobStatus,
			String optionalMessage) {

		LOG.info("Status of job " + executionGraph.getJobName() + "(" + executionGraph.getJobID() + ")"
			+ " changed to " + newJobStatus);

		if (newJobStatus == InternalJobStatus.CANCELING || newJobStatus == InternalJobStatus.FAILING) {

			// Cancel all remaining tasks
			cancelJob(executionGraph);
		}

		// Remove all checkpoints for a successfully finished job
		if (newJobStatus == InternalJobStatus.FINISHED) {
			removeAllCheckpoints(executionGraph);
		}

		if (newJobStatus == InternalJobStatus.CANCELED || newJobStatus == InternalJobStatus.FAILED
			|| newJobStatus == InternalJobStatus.FINISHED) {
			// Unregister job for Nephele's monitoring, optimization components, and dynamic input split assignment
			unregisterJob(executionGraph);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logBufferUtilization(final JobID jobID) throws IOException {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			return;
		}

		final Set<AbstractInstance> allocatedInstance = new HashSet<AbstractInstance>();
		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			final ExecutionState state = vertex.getExecutionState();
			if (state == ExecutionState.RUNNING || state == ExecutionState.FINISHING) {
				final AbstractInstance instance = vertex.getAllocatedResource().getInstance();

				if (instance instanceof DummyInstance) {
					LOG.error("Found instance of type DummyInstance for vertex " + vertex.getName() + " (state "
						+ state + ")");
					continue;
				}

				allocatedInstance.add(instance);
			}
		}

		// Send requests to task managers from separate thread
		final Runnable requestRunnable = new Runnable() {

			@Override
			public void run() {

				final Iterator<AbstractInstance> it2 = allocatedInstance.iterator();

				try {
					while (it2.hasNext()) {
						it2.next().logBufferUtilization();
					}
				} catch (IOException ioe) {
					LOG.error(StringUtils.stringifyException(ioe));
				}

			}
		};

		// Hand over to the executor service
		this.executorService.execute(requestRunnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deploy(final JobID jobID, final AbstractInstance instance,
			final List<ExecutionVertex> verticesToBeDeployed) {

		if (verticesToBeDeployed.isEmpty()) {
			LOG.error("Method 'deploy' called but list of vertices to be deployed is empty");
			return;
		}

		for (final ExecutionVertex vertex : verticesToBeDeployed) {

			// Check vertex state
			if (vertex.getExecutionState() != ExecutionState.READY) {
				LOG.error("Expected vertex " + vertex + " to be in state READY but it is in state "
					+ vertex.getExecutionState());
			}

			vertex.setExecutionState(ExecutionState.STARTING);
		}

		// Create a new runnable and pass it the executor service
		final Runnable deploymentRunnable = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {

				// Check if all required libraries are available on the instance
				try {
					instance.checkLibraryAvailability(jobID);
				} catch (IOException ioe) {
					LOG.error("Cannot check library availability: " + StringUtils.stringifyException(ioe));
				}

				final List<TaskSubmissionWrapper> submissionList = new SerializableArrayList<TaskSubmissionWrapper>();

				// Check the consistency of the call
				for (final ExecutionVertex vertex : verticesToBeDeployed) {

					submissionList.add(new TaskSubmissionWrapper(vertex.getID(), vertex.getEnvironment(), vertex
						.getExecutionGraph().getJobConfiguration(), vertex.constructInitialActiveOutputChannelsSet()));

					LOG.info("Starting task " + vertex + " on " + vertex.getAllocatedResource().getInstance());
				}

				List<TaskSubmissionResult> submissionResultList = null;

				try {
					submissionResultList = instance.submitTasks(submissionList);
				} catch (final IOException ioe) {
					final String errorMsg = StringUtils.stringifyException(ioe);
					for (final ExecutionVertex vertex : verticesToBeDeployed) {
						vertex.getEnvironment().changeExecutionState(ExecutionState.FAILED, errorMsg);
					}
				}

				if (verticesToBeDeployed.size() != submissionResultList.size()) {
					LOG.error("size of submission result list does not match size of list with vertices to be deployed");
				}

				int count = 0;
				for (final TaskSubmissionResult tsr : submissionResultList) {

					ExecutionVertex vertex = verticesToBeDeployed.get(count++);
					if (!vertex.getID().equals(tsr.getVertexID())) {
						LOG.error("Expected different order of objects in task result list");
						vertex = null;
						for (final ExecutionVertex candVertex : verticesToBeDeployed) {
							if (tsr.getVertexID().equals(candVertex.getID())) {
								vertex = candVertex;
								break;
							}
						}

						if (vertex == null) {
							LOG.error("Cannot find execution vertex for vertex ID " + tsr.getVertexID());
							continue;
						}
					}

					if (tsr.getReturnCode() == AbstractTaskResult.ReturnCode.ERROR) {
						// Change the execution state to failed and let the scheduler deal with the rest
						vertex.getEnvironment().changeExecutionState(ExecutionState.FAILED, tsr.getDescription());
					}
				}
			}
		};

		this.executorService.execute(deploymentRunnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitWrapper requestNextInputSplit(final JobID jobID, final ExecutionVertexID vertexID)
			throws IOException {

		final ExecutionGraph graph = this.scheduler.getExecutionGraphByID(jobID);
		if (graph == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return null;
		}

		final ExecutionVertex vertex = graph.getVertexByID(vertexID);
		if (vertex == null) {
			LOG.error("Cannot find execution vertex for vertex ID " + vertexID);
			return null;
		}

		return new InputSplitWrapper(jobID, this.inputSplitManager.getNextInputSplit(vertex));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialExecutionResourcesExhausted(final JobID jobID, final ExecutionVertexID vertexID,
			final ResourceUtilizationSnapshot resourceUtilizationSnapshot) throws IOException {

		final ExecutionGraph graph = this.scheduler.getExecutionGraphByID(jobID);
		if (graph == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return;
		}

		final ExecutionVertex vertex = graph.getVertexByID(vertexID);
		if (vertex == null) {
			LOG.error("Cannot find execution vertex with ID " + vertexID);
			return;
		}

		final Runnable taskStateChangeRunnable = new Runnable() {

			@Override
			public void run() {

				// The registered listeners of the vertex will make sure the appropriate actions are taken
				vertex.getEnvironment().initialExecutionResourcesExhausted(resourceUtilizationSnapshot);
			}
		};

		// Hand over to the executor service, as this may result in a longer operation with several IPC operations
		this.executorService.execute(taskStateChangeRunnable);
	}
}
