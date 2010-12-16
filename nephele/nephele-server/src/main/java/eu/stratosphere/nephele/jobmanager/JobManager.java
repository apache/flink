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
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
import eu.stratosphere.nephele.event.job.EventList;
import eu.stratosphere.nephele.event.job.NewJobEvent;
import eu.stratosphere.nephele.execution.ExecutionFailureException;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.executiongraph.ManagementGraphFactory;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.local.LocalInstanceManager;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.ipc.Server;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.jobmanager.scheduler.Scheduler;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingListener;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.optimizer.Optimizer;
import eu.stratosphere.nephele.profiling.JobManagerProfiler;
import eu.stratosphere.nephele.profiling.ProfilingUtils;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.protocols.JobManagerProtocol;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.types.StringRecord;
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
public class JobManager implements ExtendedManagementProtocol, JobManagerProtocol, ChannelLookupProtocol,
		SchedulingListener {

	private static final Log LOG = LogFactory.getLog(JobManager.class);

	private Server jobManagerServer = null;

	private final JobManagerProfiler profiler;

	private final Optimizer optimizer;

	private final EventCollector eventCollector;

	private final Scheduler scheduler;

	private final InstanceManager instanceManager;

	private final int recommendedClientPollingInterval;

	private final Set<ExecutionVertex> verticesReadyToRun = new HashSet<ExecutionVertex>();

	private final static int SLEEPINTERVAL = 1000;

	private long jobStartTime = 0;

	private long jobFinishTime = 0;

	private final static int FAILURERETURNCODE = -1;

	/**
	 * Constructs a new job manager, starts its discovery service and its IPC service.
	 */
	public JobManager(String configDir, String executionMode) {

		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);

		final int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
		;

		// First of all, start discovery manager
		try {
			DiscoveryService.startDiscoveryService(ipcPort);
		} catch (DiscoveryException e) {
			LOG.error("Cannot start discovery manager: " + StringUtils.stringifyException(e));
			System.exit(FAILURERETURNCODE);
		}

		// Read the suggested client polling interval
		this.recommendedClientPollingInterval = GlobalConfiguration.getInteger("jobclient.polling.internval", 5);

		// Determine own RPC address
		final InetSocketAddress rpcServerAddress = new InetSocketAddress(DiscoveryService.getServiceAddress(), ipcPort);

		// Start job manager's IPC server
		try {
			final int handlerCount = GlobalConfiguration.getInteger("jobmanager.rpc.numhandler", 3);
			this.jobManagerServer = RPC.getServer(this, rpcServerAddress.getHostName(), rpcServerAddress.getPort(),
				handlerCount, false);
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
			this.instanceManager = new LocalInstanceManager(configDir);
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

		// Load profiler if it should be used
		if (GlobalConfiguration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY, false)) {
			final String profilerClassName = GlobalConfiguration.getString(ProfilingUtils.JOBMANAGER_CLASSNAME_KEY,
				null);
			if (profilerClassName == null) {
				LOG.error("Cannot find class name for the profiler");
				System.exit(FAILURERETURNCODE);
			}
			this.profiler = ProfilingUtils.loadJobManagerProfiler(profilerClassName);
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

		// Load the job progress collector
		this.eventCollector = new EventCollector(this.recommendedClientPollingInterval);

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
				e.printStackTrace();
				continue;
			}

			// Run ready vertices
			runVerticesReadyForExecution();
		}
	}

	public void cleanUp() {

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

		// Stop and clean up the job progress collector
		if (this.eventCollector != null) {
			this.eventCollector.shutdown();
		}

		// Finally, shut down the scheduler
		if (this.scheduler != null) {
			this.scheduler.shutdown();
		}
	}

	/**
	 * Entry point for the program
	 * 
	 * @param args
	 *        arguments from the command line
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {

		final Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg().withDescription(
			"Specify configuration directory.").create("configDir");

		final Option executionModeOpt = OptionBuilder.withArgName("execution mode").hasArg().withDescription(
			"Specify execution mode.").create("executionMode");

		final Options options = new Options();
		options.addOption(configDirOpt);
		options.addOption(executionModeOpt);

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("CLI Parsing failed. Reason: " + e.getMessage());
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

		LOG.info("The graph of job " + job.getName() + " is acyclic");

		// Check constrains on degree
		jv = job.areVertexDegreesCorrect();
		if (jv != null) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Degree of vertex " + jv.getName() + " is incorrect");
			return result;
		}

		LOG.info("All vertices of job " + job.getName() + " have the correct degree");

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

		// Perform graph optimizations
		if (this.optimizer != null) {
			this.optimizer.optimize(eg);
		}

		// Schedule job
		LOG.info("Scheduling job " + job.getName());
		try {
			this.scheduler.schedulJob(eg);
		} catch (SchedulingException e) {
			unregisterJob(eg);
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, e.getMessage());
			return result;
		}

		jobStartTime = System.currentTimeMillis();
		System.out.println("Job started " + jobStartTime);
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
	private void unregisterJob(ExecutionGraph executionGraph) {

		// Remove job from profiler (if activated)
		if (this.profiler != null
			&& executionGraph.getJobConfiguration().getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
			this.profiler.unregisterProfilingJob(executionGraph);

			if (this.eventCollector != null) {
				this.profiler.unregisterFromProfilingData(executionGraph.getJobID(), this.eventCollector);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendHeartbeat(InstanceConnectionInfo instanceConnectionInfo) {

		// Delegate call to instance manager
		this.instanceManager.reportHeartBeat(instanceConnectionInfo);
	}

	/**
	 * Searches the current execution graph for execution vertices which have become
	 * ready for execution and triggers their execution.
	 */
	void runVerticesReadyForExecution() {

		final Set<ExecutionVertex> readyVertices = this.scheduler.getVerticesReadyToBeExecuted();

		synchronized (this.verticesReadyToRun) {

			this.verticesReadyToRun.addAll(readyVertices);

			final Iterator<ExecutionVertex> it = this.verticesReadyToRun.iterator();
			while (it.hasNext()) {

				final ExecutionVertex vertex = it.next();

				// Check vertex state
				if (vertex.getExecutionState() != ExecutionState.READY) {
					LOG.error("Expected vertex " + vertex + " to be in state READY but it is in state "
						+ vertex.getExecutionState());
				}

				/*
				 * START modification FH
				 */
				if (vertex.isInputVertex() && vertex.getEnvironment().getInputSplits().length == 0
					&& vertex.getGroupVertex().getStageNumber() == 0) {
					try {
						InputSplitAssigner.assignInputSplits(vertex);
					} catch (ExecutionFailureException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				/*
				 * END modification FH
				 */

				LOG.info("Starting task " + vertex + " on " + vertex.getAllocatedResource().getInstance());
				final TaskSubmissionResult submissionResult = vertex.startTask();
				it.remove(); // Remove task from ready set
				if (submissionResult.getReturnCode() == AbstractTaskResult.ReturnCode.ERROR) {
					// Change the execution state to failed and let the scheduler deal with the rest
					vertex.getEnvironment().changeExecutionState(ExecutionState.FAILED,
						submissionResult.getDescription());
				}

				// TODO: Implement lazy initialization
				// TODO: Check if vertex.prepare... is still required
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateTaskExecutionState(TaskExecutionState executionState) throws IOException {

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

		// The registered listeners of the vertex will make sure the appropriate actions are taken
		vertex.getEnvironment().changeExecutionState(executionState.getExecutionState(),
			executionState.getDescription());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobCancelResult cancelJob(JobID jobID) throws IOException {

		LOG.info("Trying to cancel job with ID " + jobID);

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			return new JobCancelResult(ReturnCode.ERROR, "Cannot find job with ID " + jobID);
		}

		final TaskCancelResult errorResult = cancelJob(eg);
		if (errorResult != null) {
			LOG.error("Cannot cancel job " + jobID + ": " + errorResult);
			return new JobCancelResult(AbstractJobResult.ReturnCode.ERROR, errorResult.getDescription());
		}

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

			final TaskCancelResult result = it.next().cancelTask();
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

		final EventList<AbstractEvent> eventList = new EventList<AbstractEvent>();
		this.eventCollector.getEventsForJob(jobID, eventList, false);

		return new JobProgressResult(ReturnCode.SUCCESS, null, eventList);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ConnectionInfoLookupResponse lookupConnectionInfo(JobID jobID, ChannelID targetChannelID) {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		final ExecutionVertex vertex = eg.getVertexByChannelID(targetChannelID);
		if (vertex == null) {
			LOG.debug("Cannot resolve ID " + targetChannelID + " to a vertex for job " + jobID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		final ExecutionState executionState = vertex.getExecutionState();
		if (executionState != ExecutionState.RUNNING && executionState != ExecutionState.FINISHING) {
			return ConnectionInfoLookupResponse.createReceiverNotReady();
		}

		// TODO: Start vertex if in lazy mode

		final AbstractInstance assignedInstance = vertex.getAllocatedResource().getInstance();

		if (assignedInstance == null) {
			LOG.debug("Cannot resolve lookup: vertex found for channel ID " + targetChannelID
				+ " but no instance assigned");
			return ConnectionInfoLookupResponse.createReceiverNotReady();
		}

		return ConnectionInfoLookupResponse.createReceiverFoundAndReady(assignedInstance.getInstanceConnectionInfo());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ManagementGraph getManagementGraph(JobID jobID) throws IOException {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			throw new IOException("Cannot find job with ID " + jobID);
		}

		return ManagementGraphFactory.fromExecutionGraph(eg);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) throws IOException {

		return this.instanceManager.getNetworkTopology(jobID);
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
	public EventList<NewJobEvent> getNewJobs() throws IOException {

		final EventList<NewJobEvent> eventList = new EventList<NewJobEvent>();

		if (this.eventCollector == null) {
			throw new IOException("No instance of the event collector found");
		}

		this.eventCollector.getNewJobs(eventList);

		return eventList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public EventList<AbstractEvent> getEvents(JobID jobID) throws IOException {

		final EventList<AbstractEvent> eventList = new EventList<AbstractEvent>();

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
		System.out.println("Cancelling job " + jobID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killInstance(StringRecord instanceName) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Killing instance " + instanceName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void jobRemovedFromScheduler(ExecutionGraph executionGraph) {

		LOG.info("Job " + executionGraph.getJobName() + " (" + executionGraph.getJobID() + ") removed from scheduler");

		final JobStatus jobStatus = executionGraph.getJobStatus();
		if (jobStatus != JobStatus.FINISHED && jobStatus != JobStatus.FAILED && jobStatus != JobStatus.CANCELLED) {
			LOG.error("Job " + executionGraph.getJobName() + " removed from scheduler with unexpected status "
				+ jobStatus);
		}

		if (jobStatus == JobStatus.FAILED) {
			// Make sure all tasks are really removed
			cancelJob(executionGraph);
		}

		// Unregister job for Nephele's monitoring and optimization components
		unregisterJob(executionGraph);
	}
}
