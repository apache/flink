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

package org.apache.flink.runtime.jobmanager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorEvent;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.AbstractJobResult;
import org.apache.flink.runtime.client.AbstractJobResult.ReturnCode;
import org.apache.flink.runtime.client.JobCancelResult;
import org.apache.flink.runtime.client.JobProgressResult;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.instance.Hardware;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.instance.LocalInstanceManager;
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.ipc.Server;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.accumulators.AccumulatorManager;
import org.apache.flink.runtime.jobmanager.archive.ArchiveListener;
import org.apache.flink.runtime.jobmanager.archive.MemoryArchivist;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.web.WebInfoServer;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;
import org.apache.flink.runtime.protocols.ChannelLookupProtocol;
import org.apache.flink.runtime.protocols.ExtendedManagementProtocol;
import org.apache.flink.runtime.protocols.InputSplitProviderProtocol;
import org.apache.flink.runtime.protocols.JobManagerProtocol;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.types.IntegerRecord;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.SerializableArrayList;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * The JobManager is the master that coordinates the distributed execution.
 * It receives jobs from clients, tracks the distributed execution.
 */
public class JobManager implements ExtendedManagementProtocol, InputSplitProviderProtocol,
		JobManagerProtocol, ChannelLookupProtocol, JobStatusListener, AccumulatorProtocol
{

	private static final Logger LOG = LoggerFactory.getLogger(JobManager.class);

	private final static int FAILURE_RETURN_CODE = 1;
	
	
	/** Executor service for asynchronous commands (to relieve the RPC threads of work) */
	private final ExecutorService executorService = Executors.newFixedThreadPool(2 * Hardware
			.getNumberCPUCores(), ExecutorThreadFactory.INSTANCE);
	

	/** The RPC end point through which the JobManager gets its calls */
	private final Server jobManagerServer;

	/** Keeps track of the currently available task managers */
	private final InstanceManager instanceManager;
	
	/** Assigns tasks to slots and keeps track on available and allocated task slots*/
	private final Scheduler scheduler;
	
	/** The currently running jobs */
	private final ConcurrentHashMap<JobID, ExecutionGraph> currentJobs;

	// begin: these will be consolidated / removed 
	private final EventCollector eventCollector;
	
	private final ArchiveListener archive;
	
	private final AccumulatorManager accumulatorManager;
	
	private final int recommendedClientPollingInterval;
	// end: these will be consolidated / removed

	private final AtomicBoolean isShutdownInProgress = new AtomicBoolean(false);
	
	private volatile boolean isShutDown;
	
	private WebInfoServer server;

	private BlobLibraryCacheManager libraryCacheManager;
	
	
	// --------------------------------------------------------------------------------------------
	//  Initialization & Shutdown
	// --------------------------------------------------------------------------------------------
	
	public JobManager(ExecutionMode executionMode) throws Exception {

		final String ipcAddressString = GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);

		InetAddress ipcAddress = null;
		if (ipcAddressString != null) {
			try {
				ipcAddress = InetAddress.getByName(ipcAddressString);
			} catch (UnknownHostException e) {
				throw new Exception("Cannot convert " + ipcAddressString + " to an IP address: " + e.getMessage(), e);
			}
		}

		final int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		// Read the suggested client polling interval
		this.recommendedClientPollingInterval = GlobalConfiguration.getInteger(
			ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY, ConfigConstants.DEFAULT_JOBCLIENT_POLLING_INTERVAL);

		// Load the job progress collector
		this.eventCollector = new EventCollector(this.recommendedClientPollingInterval);

		this.libraryCacheManager = new BlobLibraryCacheManager(new BlobServer(),
				GlobalConfiguration.getConfiguration());
		
		// Register simple job archive
		int archived_items = GlobalConfiguration.getInteger(
				ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT);
		if (archived_items > 0) {
			this.archive = new MemoryArchivist(archived_items);
			this.eventCollector.registerArchivist(archive);
		}
		else {
			this.archive = null;
		}
		
		this.currentJobs = new ConcurrentHashMap<JobID, ExecutionGraph>();
		
		// Create the accumulator manager, with same archiving limit as web
		// interface. We need to store the accumulators for at least one job.
		// Otherwise they might be deleted before the client requested the
		// accumulator results.
		this.accumulatorManager = new AccumulatorManager(Math.min(1, archived_items));


		// Determine own RPC address
		final InetSocketAddress rpcServerAddress = new InetSocketAddress(ipcAddress, ipcPort);

		// Start job manager's IPC server
		try {
			final int handlerCount = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_HANDLERS_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_HANDLERS);
			this.jobManagerServer = RPC.getServer(this, rpcServerAddress.getHostName(), rpcServerAddress.getPort(), handlerCount);
			this.jobManagerServer.start();
		} catch (IOException e) {
			throw new Exception("Cannot start RPC server: " + e.getMessage(), e);
		}

		LOG.info("Starting job manager in " + executionMode + " mode");

		// Try to load the instance manager for the given execution mode
		if (executionMode == ExecutionMode.LOCAL) {
			final int numTaskManagers = GlobalConfiguration.getInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1);
			this.instanceManager = new LocalInstanceManager(numTaskManagers);
		}
		else if (executionMode == ExecutionMode.CLUSTER) {
			this.instanceManager = new InstanceManager();
		}
		else {
			throw new IllegalArgumentException("ExecutionMode");
		}

		// create the scheduler and make it listen at the availability of new instances
		this.scheduler = new Scheduler(this.executorService);
		this.instanceManager.addInstanceListener(this.scheduler);
	}

	public void shutdown() {

		if (!this.isShutdownInProgress.compareAndSet(false, true)) {
			return;
		}
		
		for (ExecutionGraph e : this.currentJobs.values()) {
			e.fail(new Exception("The JobManager is shutting down."));
		}
		
		// Stop the executor service
		// this waits for any pending calls to be done
		if (this.executorService != null) {
			this.executorService.shutdown();
			try {
				this.executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOG.debug("Shutdown of executor thread pool interrupted", e);
			}
		}

		// Stop instance manager
		if (this.instanceManager != null) {
			this.instanceManager.shutdown();
		}

		// Stop the BLOB server
		if (this.libraryCacheManager != null) {
			try {
				this.libraryCacheManager.shutdown();
			} catch (IOException e) {
				LOG.warn("Could not properly shutdown the library cache manager.", e);
			}
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

		if(this.server != null) {
			try {
				this.server.stop();
			} catch (Exception e1) {
				throw new RuntimeException("Error shtopping the web-info-server.", e1);
			}
		}
		this.isShutDown = true;
		LOG.debug("Shutdown of job manager completed");
	}
	
	// --------------------------------------------------------------------------------------------
	//  Job Execution
	// --------------------------------------------------------------------------------------------

	@Override
	public JobSubmissionResult submitJob(JobGraph job) throws IOException {
		// First check the basics
		if (job == null) {
			return new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, "Submitted job is null!");
		}
		if (job.getNumberOfVertices() == 0) {
			return new JobSubmissionResult(ReturnCode.ERROR, "Job is empty.");
		}
		
		ExecutionGraph executionGraph = null;

		try {
			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Received job %s (%s)", job.getJobID(), job.getName()));
			}

			// Register this job with the library cache manager
			libraryCacheManager.register(job.getJobID(), job.getUserJarBlobKeys());
			
			// get the existing execution graph (if we attach), or construct a new empty one to attach
			executionGraph = this.currentJobs.get(job.getJobID());
			if (executionGraph == null) {
				if (LOG.isInfoEnabled()) {
					LOG.info("Creating new execution graph for job " + job.getJobID() + " (" + job.getName() + ')');
				}
				
				executionGraph = new ExecutionGraph(job.getJobID(), job.getName(),
						job.getJobConfiguration(), job.getUserJarBlobKeys(), this.executorService);
				ExecutionGraph previous = this.currentJobs.putIfAbsent(job.getJobID(), executionGraph);
				if (previous != null) {
					throw new JobException("Concurrent submission of a job with the same jobId: " + job.getJobID());
				}
			}
			else {
				if (LOG.isInfoEnabled()) {
					LOG.info(String.format("Found existing execution graph for id %s, attaching this job.", job.getJobID()));
				}
			}

			// Register for updates on the job status
			executionGraph.registerJobStatusListener(this);
			
			// grab the class loader for user-defined code
			final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(job.getJobID());
			if (userCodeLoader == null) {
				throw new JobException("The user code class loader could not be initialized.");
			}

			// first, perform the master initialization of the nodes
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Running master initialization of job %s (%s)", job.getJobID(), job.getName()));
			}

			for (AbstractJobVertex vertex : job.getVertices()) {
				// check that the vertex has an executable class
				String executableClass = vertex.getInvokableClassName();
				if (executableClass == null || executableClass.length() == 0) {
					throw new JobException(String.format("The vertex %s (%s) has no invokable class.", vertex.getID(), vertex.getName()));
				}

				// master side initialization
				vertex.initializeOnMaster(userCodeLoader);
			}

			// first topologically sort the job vertices to form the basis of creating the execution graph
			List<AbstractJobVertex> topoSorted = job.getVerticesSortedTopologicallyFromSources();
			
			// first convert this job graph to an execution graph
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Adding %d vertices from job graph %s (%s)", topoSorted.size(), job.getJobID(), job.getName()));
			}
			
			executionGraph.attachJobGraph(topoSorted);
			
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Successfully created execution graph from job graph %s (%s)", job.getJobID(), job.getName()));
			}
			
			// should the job fail if a vertex cannot be deployed immediately (streams, closed iterations)
			executionGraph.setQueuedSchedulingAllowed(job.getAllowQueuedScheduling());
	
			// Register job with the progress collector
			if (this.eventCollector != null) {
				this.eventCollector.registerJob(executionGraph, false, System.currentTimeMillis());
			}
	
			// Schedule job
			if (LOG.isInfoEnabled()) {
				LOG.info("Scheduling job " + job.getName());
			}
	
			executionGraph.scheduleForExecution(this.scheduler);
	
			return new JobSubmissionResult(AbstractJobResult.ReturnCode.SUCCESS, null);
		}
		catch (Throwable t) {
			LOG.error("Job submission failed.", t);
			if(executionGraph != null){
				executionGraph.fail(t);

				try {
					executionGraph.waitForJobEnd(10000);
				}catch(InterruptedException e){
					LOG.error("Interrupted while waiting for job to finish canceling.");
				}
			}

			// job was not prperly removed by the fail call
			if(currentJobs.contains(job.getJobID())){
				currentJobs.remove(job.getJobID());
				libraryCacheManager.unregister(job.getJobID());
			}

			return new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, StringUtils.stringifyException(t));
		}
	}

	@Override
	public JobCancelResult cancelJob(JobID jobID) throws IOException {

		LOG.info("Trying to cancel job with ID " + jobID);

		final ExecutionGraph eg = this.currentJobs.get(jobID);
		if (eg == null) {
			LOG.info("No job found with ID " + jobID);
			return new JobCancelResult(ReturnCode.ERROR, "Cannot find job with ID " + jobID);
		}

		final Runnable cancelJobRunnable = new Runnable() {
			@Override
			public void run() {
				eg.cancel();
			}
		};

		eg.execute(cancelJobRunnable);

		return new JobCancelResult(AbstractJobResult.ReturnCode.SUCCESS, null);
	}
	
	@Override
	public boolean updateTaskExecutionState(TaskExecutionState executionState) throws IOException {
		Preconditions.checkNotNull(executionState);


		final ExecutionGraph eg = this.currentJobs.get(executionState.getJobID());
		if (eg == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Orphaned execution task: UpdateTaskExecutionState call cannot find execution graph for ID " + executionState.getJobID() +
						" to change state to " + executionState.getExecutionState());
			}
			return false;
		}

		return eg.updateState(executionState);
	}
	
	@Override
	public InputSplit requestNextInputSplit(JobID jobID, JobVertexID vertexId) throws IOException {

		final ExecutionGraph graph = this.currentJobs.get(jobID);
		if (graph == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return null;
		}

		final ExecutionJobVertex vertex = graph.getJobVertex(vertexId);
		if (vertex == null) {
			LOG.error("Cannot find execution vertex for vertex ID " + vertexId);
			return null;
		}

		InputSplitAssigner splitAssigner = vertex.getSplitAssigner();
		if (splitAssigner == null) {
			LOG.error("No InputSplitAssigner for vertex ID " + vertexId);
			return null;
		}
		
		
		return splitAssigner.getNextInputSplit(null);
	}
	
	@Override
	public void jobStatusHasChanged(ExecutionGraph executionGraph, JobStatus newJobStatus, String optionalMessage) {

		final JobID jid = executionGraph.getJobID();
		
		if (LOG.isInfoEnabled()) {
			String message = optionalMessage == null ? "." : ": " + optionalMessage;
			LOG.info(String.format("Job %s (%s) switched to %s%s", 
					jid, executionGraph.getJobName(), newJobStatus, message));
		}

		// remove the job graph if the state is any terminal state
		if (newJobStatus.isTerminalState()) {
			this.currentJobs.remove(jid);
			
			try {
				libraryCacheManager.unregister(jid);
			}
			catch (Throwable t) {
				LOG.warn("Could not properly unregister job " + jid + " from the library cache.");
			}
		}
	}

	@Override
	public JobProgressResult getJobProgress(final JobID jobID) throws IOException {

		if (this.eventCollector == null) {
			return new JobProgressResult(ReturnCode.ERROR, "JobManager does not support progress reports for jobs", null);
		}

		final SerializableArrayList<AbstractEvent> eventList = new SerializableArrayList<AbstractEvent>();
		this.eventCollector.getEventsForJob(jobID, eventList, false);

		return new JobProgressResult(ReturnCode.SUCCESS, null, eventList);
	}


	@Override
	public ConnectionInfoLookupResponse lookupConnectionInfo(InstanceConnectionInfo caller, JobID jobID, ChannelID sourceChannelID) {

		final ExecutionGraph eg = this.currentJobs.get(jobID);
		if (eg == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		return eg.lookupConnectionInfoAndDeployReceivers(caller, sourceChannelID);
	}

	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Tests whether the job manager has been shut down completely.
	 * 
	 * @return <code>true</code> if the job manager has been shut down completely, <code>false</code> otherwise
	 */
	public boolean isShutDown() {
		return this.isShutDown;
	}
	
	public InstanceManager getInstanceManager() {
		return this.instanceManager;
	}

	@Override
	public IntegerRecord getRecommendedPollingInterval() throws IOException {
		return new IntegerRecord(this.recommendedClientPollingInterval);
	}

	@Override
	public List<RecentJobEvent> getRecentJobs() throws IOException {

		final List<RecentJobEvent> eventList = new SerializableArrayList<RecentJobEvent>();

		if (this.eventCollector == null) {
			throw new IOException("No instance of the event collector found");
		}

		this.eventCollector.getRecentJobs(eventList);

		return eventList;
	}


	@Override
	public List<AbstractEvent> getEvents(final JobID jobID) throws IOException {

		final List<AbstractEvent> eventList = new SerializableArrayList<AbstractEvent>();

		if (this.eventCollector == null) {
			throw new IOException("No instance of the event collector found");
		}

		this.eventCollector.getEventsForJob(jobID, eventList, true);

		return eventList;
	}

	@Override
	public int getTotalNumberOfRegisteredSlots() {
		return getInstanceManager().getTotalNumberOfSlots();
	}
	
	@Override
	public int getNumberOfSlotsAvailableToScheduler() {
		return scheduler.getNumberOfAvailableSlots();
	}
	
	/**
	 * Starts the Jetty Infoserver for the Jobmanager
	 * 
	 */
	public void startInfoServer() {
		final Configuration config = GlobalConfiguration.getConfiguration();
		// Start InfoServer
		try {
			int port = config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);
			server = new WebInfoServer(config, port, this);
			server.start();
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (Exception e) {
			LOG.error("Cannot instantiate info server: " + e.getMessage(), e);
		}
	}
	
	
	public List<RecentJobEvent> getOldJobs() throws IOException {
		if (this.archive == null) {
			throw new IOException("No instance of the event collector found");
		}

		return this.archive.getJobs();
	}
	
	public ArchiveListener getArchive() {
		return this.archive;
	}

	public int getNumberOfTaskManagers() {
		return this.instanceManager.getNumberOfRegisteredTaskManagers();
	}
	
	public Map<InstanceID, Instance> getInstances() {
		return this.instanceManager.getAllRegisteredInstances();
	}

	@Override
	public void reportAccumulatorResult(AccumulatorEvent accumulatorEvent) throws IOException {
		this.accumulatorManager.processIncomingAccumulators(accumulatorEvent.getJobID(),
				accumulatorEvent.getAccumulators(libraryCacheManager.getClassLoader(accumulatorEvent.getJobID()
				)));
	}

	@Override
	public AccumulatorEvent getAccumulatorResults(JobID jobID) throws IOException {
		return new AccumulatorEvent(jobID, this.accumulatorManager.getJobAccumulators(jobID));
	}
	
	public Map<String, Accumulator<?, ?>> getAccumulators(JobID jobID) {
		return this.accumulatorManager.getJobAccumulators(jobID);
	}
	
	public Map<JobID, ExecutionGraph> getCurrentJobs() {
		return Collections.unmodifiableMap(currentJobs);
	}
	
	public ExecutionGraph getRecentExecutionGraph(JobID jobID) throws IOException {
		ExecutionGraph eg = currentJobs.get(jobID);
		if (eg == null) {
			eg = this.eventCollector.getManagementGraph(jobID);
			if (eg == null && this.archive != null) {
				eg = this.archive.getExecutionGraph(jobID);
			}
		}
		
		if (eg == null) {
			throw new IOException("Cannot find execution graph for job with ID " + jobID);
		}
		return eg;
	}

	// --------------------------------------------------------------------------------------------
	//  TaskManager to JobManager communication
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean sendHeartbeat(InstanceID taskManagerId) {
		return this.instanceManager.reportHeartBeat(taskManagerId);
	}

	@Override
	public InstanceID registerTaskManager(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription, int numberOfSlots) {
		if (this.instanceManager != null && this.scheduler != null) {
			return this.instanceManager.registerTaskManager(instanceConnectionInfo, hardwareDescription, numberOfSlots);
		} else {
			return null;
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Executable
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Entry point for the program
	 * 
	 * @param args
	 *        arguments from the command line
	 */
	
	public static void main(String[] args) {
		
		JobManager jobManager;
		try {
			jobManager = initialize(args);
			// Start info server for jobmanager
			jobManager.startInfoServer();
		}
		catch (Exception e) {
			LOG.error(e.getMessage(), e);
			System.exit(FAILURE_RETURN_CODE);
		}
		
		// Clean up is triggered through a shutdown hook
		// freeze this thread to keep the JVM alive (the job manager threads are daemon threads)
		Object w = new Object();
		synchronized (w) {
			try {
				w.wait();
			} catch (InterruptedException e) {}
		}
	}
	
	@SuppressWarnings("static-access")
	public static JobManager initialize(String[] args) throws Exception {
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
			System.exit(FAILURE_RETURN_CODE);
		}

		final String configDir = line.getOptionValue(configDirOpt.getOpt(), null);
		final String executionModeName = line.getOptionValue(executionModeOpt.getOpt(), "local");
		
		ExecutionMode executionMode = null;
		if ("local".equals(executionModeName)) {
			executionMode = ExecutionMode.LOCAL;
		} else if ("cluster".equals(executionModeName)) {
			executionMode = ExecutionMode.CLUSTER;
		} else {
			System.err.println("Unrecognized execution mode: " + executionModeName);
			System.exit(FAILURE_RETURN_CODE);
		}
		
		// print some startup environment info, like user, code revision, etc
		EnvironmentInformation.logEnvironmentInfo(LOG, "JobManager");
		
		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);

		// Create a new job manager object
		JobManager jobManager = new JobManager(executionMode);
		
		// Set base dir for info server
		Configuration infoserverConfig = GlobalConfiguration.getConfiguration();
		if (configDir != null && new File(configDir).isDirectory()) {
			infoserverConfig.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, configDir+"/..");
		}
		GlobalConfiguration.includeConfiguration(infoserverConfig);
		return jobManager;
	}

	@Override
	public int getBlobServerPort() {
		return libraryCacheManager.getBlobServerPort();
	}
}
