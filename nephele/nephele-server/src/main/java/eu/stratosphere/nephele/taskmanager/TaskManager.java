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

package eu.stratosphere.nephele.taskmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.checkpointing.CheckpointDecision;
import eu.stratosphere.nephele.checkpointing.CheckpointReplayManager;
import eu.stratosphere.nephele.checkpointing.CheckpointReplayResult;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.discovery.DiscoveryException;
import eu.stratosphere.nephele.discovery.DiscoveryService;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.ipc.Server;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.plugins.PluginID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.plugins.TaskManagerPlugin;
import eu.stratosphere.nephele.profiling.ProfilingUtils;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.protocols.InputSplitProviderProtocol;
import eu.stratosphere.nephele.protocols.JobManagerProtocol;
import eu.stratosphere.nephele.protocols.PluginCommunicationProtocol;
import eu.stratosphere.nephele.protocols.TaskOperationProtocol;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult.ReturnCode;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;
import eu.stratosphere.nephele.util.SerializableArrayList;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * A task manager receives tasks from the job manager and executes them. After having executed them
 * (or in case of an execution error) it reports the execution result back to the job manager.
 * Task managers are able to automatically discover the job manager and receive its configuration from it
 * as long as the job manager is running on the same local network
 * 
 * @author warneke
 */
public class TaskManager implements TaskOperationProtocol {

	private static final Log LOG = LogFactory.getLog(TaskManager.class);

	private final JobManagerProtocol jobManager;

	private final InputSplitProviderProtocol globalInputSplitProvider;

	private final ChannelLookupProtocol lookupService;

	private final PluginCommunicationProtocol pluginCommunicationService;

	private static final int handlerCount = 1;

	private final Server taskManagerServer;

	/**
	 * This map contains all the tasks whose threads are in a state other than TERMINATED. If any task
	 * is stored inside this map and its thread status is TERMINATED, this indicates a virtual machine error.
	 * As a result, task status will switch to FAILED and reported to the {@link JobManager}.
	 */
	private final Map<ExecutionVertexID, Task> runningTasks = new HashMap<ExecutionVertexID, Task>();

	private final InstanceConnectionInfo localInstanceConnectionInfo;

	private final static int FAILURERETURNCODE = -1;

	private final static int DEFAULTPERIODICTASKSINTERVAL = 1000;

	/**
	 * The instance of the {@link ByteBufferedChannelManager} which is responsible for
	 * setting up and cleaning up the byte buffered channels of the tasks.
	 */
	private final ByteBufferedChannelManager byteBufferedChannelManager;

	/**
	 * The instance of the {@link CheckpointReplayManager} to restore
	 * previously written checkpoints.
	 */
	private final CheckpointReplayManager checkpointManager;

	/**
	 * Instance of the task manager profile if profiling is enabled.
	 */
	private final TaskManagerProfiler profiler;

	private final MemoryManager memoryManager;

	private final IOManager ioManager;

	private final HardwareDescription hardwareDescription;

	private final Map<PluginID, TaskManagerPlugin> taskManagerPlugins;

	/**
	 * Stores whether the task manager has already been shut down.
	 */
	private boolean isShutDown = false;

	/**
	 * Constructs a new task manager, starts its IPC service and attempts to discover the job manager to
	 * receive an initial configuration.
	 * 
	 * @param configDir
	 *        the directory containing the configuration files for the task manager
	 */
	public TaskManager(String configDir) throws Exception {

		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);

		// Use discovery service to find the job manager in the network?
		final String address = GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		InetSocketAddress jobManagerAddress = null;
		if (address == null) {
			// Address is null, use discovery manager to determine address
			LOG.info("Using discovery service to locate job manager");
			try {
				jobManagerAddress = DiscoveryService.getJobManagerAddress();
			} catch (DiscoveryException e) {
				throw new Exception("Failed to locate job manager via discovery: " + e.getMessage(), e);
			}
		} else {
			LOG.info("Reading location of job manager from configuration");

			final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

			// Try to convert configured address to {@link InetAddress}
			try {
				final InetAddress tmpAddress = InetAddress.getByName(address);
				jobManagerAddress = new InetSocketAddress(tmpAddress, port);
			} catch (UnknownHostException e) {
				throw new Exception("Failed to locate job manager based on configuration: " + e.getMessage(), e);
			}
		}

		LOG.info("Determined address of job manager to be " + jobManagerAddress);

		// Determine interface address that is announced to the job manager
		final int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT);
		final int dataPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);

		InetAddress taskManagerAddress = null;

		try {
			taskManagerAddress = DiscoveryService.getTaskManagerAddress(jobManagerAddress.getAddress());
		} catch (DiscoveryException e) {
			throw new Exception("Failed to initialize discovery service. " + e.getMessage(), e);
		}

		this.localInstanceConnectionInfo = new InstanceConnectionInfo(taskManagerAddress, ipcPort, dataPort);

		LOG.info("Announcing connection information " + this.localInstanceConnectionInfo + " to job manager");

		// Try to create local stub for the job manager
		JobManagerProtocol jobManager = null;
		try {
			jobManager = (JobManagerProtocol) RPC.getProxy(JobManagerProtocol.class, jobManagerAddress, NetUtils
				.getSocketFactory());
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new Exception("Failed to initialize connection to JobManager: " + e.getMessage(), e);
		}
		this.jobManager = jobManager;

		// Try to create local stub of the global input split provider
		InputSplitProviderProtocol globalInputSplitProvider = null;
		try {
			globalInputSplitProvider = (InputSplitProviderProtocol) RPC.getProxy(InputSplitProviderProtocol.class,
				jobManagerAddress, NetUtils.getSocketFactory());
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new Exception("Failed to initialize connection to global input split provider: " + e.getMessage(), e);
		}
		this.globalInputSplitProvider = globalInputSplitProvider;

		// Try to create local stub for the lookup service
		ChannelLookupProtocol lookupService = null;
		try {
			lookupService = (ChannelLookupProtocol) RPC.getProxy(ChannelLookupProtocol.class, jobManagerAddress,
				NetUtils.getSocketFactory());
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new Exception("Failed to initialize channel lookup protocol. " + e.getMessage(), e);
		}
		this.lookupService = lookupService;

		// Try to create local stub for the plugin communication service
		PluginCommunicationProtocol pluginCommunicationService = null;
		try {
			pluginCommunicationService = (PluginCommunicationProtocol) RPC.getProxy(PluginCommunicationProtocol.class,
				jobManagerAddress, NetUtils.getSocketFactory());
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new Exception("Failed to initialize plugin communication protocol. " + e.getMessage(), e);
		}
		this.pluginCommunicationService = pluginCommunicationService;

		// Start local RPC server
		Server taskManagerServer = null;
		try {
			taskManagerServer = RPC.getServer(this, taskManagerAddress.getHostName(), ipcPort, handlerCount);
			taskManagerServer.start();
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new Exception("Failed to taskmanager server. " + e.getMessage(), e);
		}
		this.taskManagerServer = taskManagerServer;

		// Load profiler if it should be used
		if (GlobalConfiguration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY, false)) {
			final String profilerClassName = GlobalConfiguration.getString(ProfilingUtils.TASKMANAGER_CLASSNAME_KEY,
				null);
			if (profilerClassName == null) {
				LOG.error("Cannot find class name for the profiler.");
				throw new Exception("Cannot find class name for the profiler.");
			}
			this.profiler = ProfilingUtils.loadTaskManagerProfiler(profilerClassName, jobManagerAddress.getAddress(),
				this.localInstanceConnectionInfo);
		} else {
			this.profiler = null;
			LOG.debug("Profiler disabled");
		}

		// Get the directory for storing temporary files
		final String[] tmpDirPaths = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(":");

		// Initialize the byte buffered channel manager
		ByteBufferedChannelManager byteBufferedChannelManager = null;
		try {
			byteBufferedChannelManager = new ByteBufferedChannelManager(this.lookupService,
				this.localInstanceConnectionInfo);
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
			throw new Exception("Failed to instantiate Byte-buffered channel manager. " + ioe.getMessage(), ioe);
		}
		this.byteBufferedChannelManager = byteBufferedChannelManager;

		// Initialize the checkpoint manager
		this.checkpointManager = new CheckpointReplayManager(this.byteBufferedChannelManager);

		// Determine hardware description
		HardwareDescription hardware = HardwareDescriptionFactory.extractFromSystem();
		if (hardware == null) {
			LOG.warn("Cannot determine hardware description");
		}

		// Check whether the memory size has been explicitly configured. if so that overrides the default mechanism
		// of taking as much as is mentioned in the hardware description
		long memorySize = GlobalConfiguration.getInteger(ConfigConstants.MEMORY_MANAGER_AVAILABLE_MEMORY_SIZE_KEY, -1);

		if (memorySize > 0) {
			// manually configured memory size. override the value in the hardware config
			hardware = HardwareDescriptionFactory.construct(hardware.getNumberOfCPUCores(),
				hardware.getSizeOfPhysicalMemory(), memorySize * 1024L * 1024L);
		}
		this.hardwareDescription = hardware;

		// Initialize the memory manager
		LOG.info("Initializing memory manager with " + (hardware.getSizeOfFreeMemory() >>> 20) + " megabytes of memory");
		try {
			this.memoryManager = new DefaultMemoryManager(hardware.getSizeOfFreeMemory());
		} catch (RuntimeException rte) {
			LOG.fatal("Unable to initialize memory manager with " + (hardware.getSizeOfFreeMemory() >>> 20)
				+ " megabytes of memory", rte);
			throw rte;
		}

		this.ioManager = new IOManager(tmpDirPaths);

		// Load the plugins
		this.taskManagerPlugins = PluginManager.getTaskManagerPlugins(configDir);

		// Add shutdown hook for clean up tasks
		Runtime.getRuntime().addShutdownHook(new TaskManagerCleanUp(this));
	}

	/**
	 * Entry point for the program.
	 * 
	 * @param args
	 *        arguments from the command line
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) {

		Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg().withDescription(
			"Specify configuration directory.").create("configDir");

		Options options = new Options();
		options.addOption(configDirOpt);

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("CLI Parsing failed. Reason: " + e.getMessage());
			System.exit(FAILURERETURNCODE);
		}

		String configDir = line.getOptionValue(configDirOpt.getOpt(), null);

		// Create a new task manager object
		TaskManager taskManager = null;
		try {
			taskManager = new TaskManager(configDir);
		} catch (Exception e) {
			LOG.fatal("Taskmanager startup failed:" + StringUtils.stringifyException(e));
			System.exit(FAILURERETURNCODE);
		}

		// Run the main I/O loop
		taskManager.runIOLoop();

		// Shut down
		taskManager.shutdown();
	}

	// This method is called by the TaskManagers main thread
	public void runIOLoop() {

		long interval = GlobalConfiguration.getInteger("taskmanager.setup.periodictaskinterval",
			DEFAULTPERIODICTASKSINTERVAL);

		while (!Thread.interrupted()) {

			// Sleep
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e1) {
				LOG.debug("Heartbeat thread was interrupted");
				break;
			}

			// Send heartbeat
			try {
				this.jobManager.sendHeartbeat(this.localInstanceConnectionInfo, this.hardwareDescription);
			} catch (IOException e) {
				LOG.debug("sending the heart beat caused on IO Exception");
			}

			// Check the status of the task threads to detect unexpected thread terminations
			checkTaskExecution();
		}

		// Shutdown the individual components of the task manager
		shutdown();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TaskCancelResult cancelTask(final ExecutionVertexID id) throws IOException {

		// Check if the task is registered with our task manager
		Task tmpTask;

		synchronized (this.runningTasks) {

			tmpTask = this.runningTasks.get(id);

			if (tmpTask == null) {
				final TaskCancelResult taskCancelResult = new TaskCancelResult(id, AbstractTaskResult.ReturnCode.ERROR);
				taskCancelResult.setDescription("No task with ID + " + id + " is currently running");
				return taskCancelResult;
			}
		}

		final Task task = tmpTask;
		// Execute call in a new thread so IPC thread can return immediately
		final Thread tmpThread = new Thread(new Runnable() {

			@Override
			public void run() {

				// Finally, request user code to cancel
				task.cancelExecution();
			}
		});
		tmpThread.start();

		return new TaskCancelResult(id, AbstractTaskResult.ReturnCode.SUCCESS);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TaskSubmissionResult submitTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final Environment ee, final Set<ChannelID> activeOutputChannels)
			throws IOException {

		// Register task manager components in environment
		ee.setMemoryManager(this.memoryManager);
		ee.setIOManager(this.ioManager);

		// Register a new task input split provider
		ee.setInputSplitProvider(new TaskInputSplitProvider(ee.getJobID(), id, this.globalInputSplitProvider));

		// Create task object and register it with the environment
		final Task task = new Task(id, ee, this);
		ee.setExecutionObserver(task);

		// Register the task
		TaskSubmissionResult result = registerTask(id, jobConfiguration, task, activeOutputChannels);
		if (result != null) { // If result is non-null, an error occurred during task registration
			return result;
		}

		// Start execution
		if (LOG.isDebugEnabled()) {
			LOG.debug("Starting execution of task with ID " + id);
		}
		task.startExecution();

		return new TaskSubmissionResult(id, AbstractTaskResult.ReturnCode.SUCCESS);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<TaskSubmissionResult> submitTasks(final List<TaskSubmissionWrapper> tasks) throws IOException {

		final List<TaskSubmissionResult> submissionResultList = new SerializableArrayList<TaskSubmissionResult>();
		final List<Task> tasksToStart = new ArrayList<Task>();

		// Make sure all tasks are fully registered before they are started
		for (final TaskSubmissionWrapper tsw : tasks) {

			final Environment ee = tsw.getEnvironment();
			final ExecutionVertexID id = tsw.getVertexID();
			final Configuration jobConfiguration = tsw.getConfiguration();
			final Set<ChannelID> activeOutputChannels = tsw.getActiveOutputChannels();

			// Register task manager components in environment
			ee.setMemoryManager(this.memoryManager);
			ee.setIOManager(this.ioManager);

			// Register a new task input split provider
			ee.setInputSplitProvider(new TaskInputSplitProvider(ee.getJobID(), id, this.globalInputSplitProvider));

			// Create task object and register it with the environment
			final Task task = new Task(id, ee, this);
			ee.setExecutionObserver(task);

			// Register the task
			TaskSubmissionResult result = registerTask(id, jobConfiguration, task, activeOutputChannels);
			if (result != null) { // If result is non-null, an error occurred during task registration
				submissionResultList.add(result);
			} else {
				submissionResultList.add(new TaskSubmissionResult(id, AbstractTaskResult.ReturnCode.SUCCESS));
			}

			tasksToStart.add(task);
		}

		// Now start the tasks
		for (final Task task : tasksToStart) {
			task.startExecution();
		}

		return submissionResultList;
	}

	/**
	 * Registers an newly incoming task with the task manager.
	 * 
	 * @param id
	 *        the ID of the task to register
	 * @param jobConfiguration
	 *        the job configuration that has been attached to the original job graph
	 * @param task
	 *        the task to register
	 * @param activeOutputChannels
	 *        the set of initially active output channels
	 * @return <code>null</code> if the registration has been successful or a {@link TaskSubmissionResult} containing
	 *         the error that occurred
	 */
	private TaskSubmissionResult registerTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final Task task, final Set<ChannelID> activeOutputChannels) {

		if (id == null) {
			throw new IllegalArgumentException("Argument id is null");
		}

		if (task == null) {
			throw new IllegalArgumentException("Argument task is null");
		}

		// Check if the task is already running
		synchronized (this.runningTasks) {
			if (this.runningTasks.containsKey(id)) {
				LOG.debug("Task with ID " + id + " is already running");
				TaskSubmissionResult result = new TaskSubmissionResult(id, AbstractTaskResult.ReturnCode.ERROR);
				result.setDescription("Task with ID " + id + " is already running");
				return result;
			}
		}

		final Environment ee = task.getEnvironment();

		// Check if the task has unbound input/output gates
		if (ee.hasUnboundInputGates() || ee.hasUnboundOutputGates()) {
			LOG.debug("Task with ID " + id + " has unbound gates");
			TaskSubmissionResult result = new TaskSubmissionResult(id, AbstractTaskResult.ReturnCode.ERROR);
			result.setDescription("Task with ID " + id + " has unbound gates");
			return result;
		}

		// Register the task with the byte buffered channel manager
		this.byteBufferedChannelManager.register(task, activeOutputChannels);

		boolean enableProfiling = false;
		if (this.profiler != null && jobConfiguration.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
			enableProfiling = true;
		}

		// Register environment, input, and output gates for profiling
		if (enableProfiling) {

			this.profiler.registerExecutionListener(task, jobConfiguration);

			for (int i = 0; i < ee.getNumberOfInputGates(); i++) {
				this.profiler.registerInputGateListener(id, jobConfiguration, ee.getInputGate(i));
			}

			for (int i = 0; i < ee.getNumberOfOutputGates(); i++) {
				this.profiler.registerOutputGateListener(id, jobConfiguration, ee.getOutputGate(i));
			}
		}

		// Allow plugins to register their listeners for this task
		if (!this.taskManagerPlugins.isEmpty()) {
			final Iterator<TaskManagerPlugin> it = this.taskManagerPlugins.values().iterator();
			while (it.hasNext()) {
				it.next().registerTask(id, jobConfiguration, ee);
			}
		}

		// The environment itself will put the task into the running task map

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<CheckpointReplayResult> replayCheckpoints(final List<ExecutionVertexID> vertexIDs) throws IOException {

		final List<CheckpointReplayResult> checkpointResultList = new SerializableArrayList<CheckpointReplayResult>();

		for (final ExecutionVertexID vertexID : vertexIDs) {

			if (!this.checkpointManager.hasCompleteCheckpointAvailable(vertexID)) {

				if (this.checkpointManager.hasPartialCheckpointAvailable(vertexID)) {
					synchronized (this.runningTasks) {
						if (!this.runningTasks.containsKey(vertexID)) {
							final CheckpointReplayResult result = new CheckpointReplayResult(vertexID, ReturnCode.ERROR);
							result
								.setDescription("Checkpoint is only partial and corresponding task is no longer running");
							checkpointResultList.add(result);
							continue;
						}
					}
				} else {
					final CheckpointReplayResult result = new CheckpointReplayResult(vertexID, ReturnCode.ERROR);
					result.setDescription("No checkpoint found");
					checkpointResultList.add(result);
					continue;
				}
			}

			// Replay the checkpoint
			this.checkpointManager.replayCheckpoint(vertexID);

			checkpointResultList.add(new CheckpointReplayResult(vertexID, ReturnCode.SUCCESS));
		}

		return checkpointResultList;
	}

	/**
	 * Unregisters a finished or aborted task.
	 * 
	 * @param id
	 *        the ID of the task to be unregistered
	 * @param task
	 *        the {@link Task} to be unregistered
	 */
	private void unregisterTask(final ExecutionVertexID id, final Task task) {

		// Unregister task from the byte buffered channel manager
		this.byteBufferedChannelManager.unregister(id, task);

		// Unregister task from profiling
		if (this.profiler != null) {
			this.profiler.unregisterOutputGateListeners(id);
			this.profiler.unregisterInputGateListeners(id);
			this.profiler.unregisterExecutionListener(id);
		}

		// Unregister task from memory manager
		if (this.memoryManager != null) {
			this.memoryManager.releaseAll(task.getEnvironment().getInvokable());
		}

		// Allow plugins to unregister their listeners for this task
		if (!this.taskManagerPlugins.isEmpty()) {
			final Iterator<TaskManagerPlugin> it = this.taskManagerPlugins.values().iterator();
			while (it.hasNext()) {
				it.next().unregisterTask(id, task.getEnvironment());
			}
		}

		// Check if there are still vertices running that belong to the same job
		int numberOfVerticesBelongingToThisJob = 0;
		synchronized (this.runningTasks) {
			final Iterator<Task> iterator = this.runningTasks.values().iterator();
			while (iterator.hasNext()) {
				final Task candidateTask = iterator.next();
				if (task.getJobID().equals(candidateTask.getJobID())) {
					numberOfVerticesBelongingToThisJob++;
				}
			}
		}

		// If there are no other vertices belonging to the same job, we can unregister the job's class loader
		if (numberOfVerticesBelongingToThisJob == 0) {
			try {
				LibraryCacheManager.unregister(task.getJobID());
			} catch (IOException e) {
				LOG.debug("Unregistering the job vertex ID " + id + " caused an IOException");
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LibraryCacheProfileResponse getLibraryCacheProfile(LibraryCacheProfileRequest request) throws IOException {

		LibraryCacheProfileResponse response = new LibraryCacheProfileResponse(request);
		String[] requiredLibraries = request.getRequiredLibraries();

		for (int i = 0; i < requiredLibraries.length; i++) {
			if (LibraryCacheManager.contains(requiredLibraries[i]) == null)
				response.setCached(i, false);
			else
				response.setCached(i, true);
		}

		return response;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateLibraryCache(LibraryCacheUpdate update) throws IOException {

		// Nothing to to here
	}

	void executionStateChanged(final JobID jobID, final ExecutionVertexID id, final Task task,
			final ExecutionState newExecutionState, final String optionalDescription) {

		if (newExecutionState == ExecutionState.RUNNING) {
			// Mark task as running by putting it in the corresponding map
			synchronized (this.runningTasks) {
				this.runningTasks.put(id, task);
			}
		}

		if (newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
			|| newExecutionState == ExecutionState.FAILED) {

			// In any of these states the task's thread will be terminated, so we remove the task from the running tasks
			// map
			synchronized (this.runningTasks) {
				this.runningTasks.remove(id);
			}

			// Unregister the task (free all buffers, remove all channels, task-specific class loaders, etc...
			unregisterTask(id, task);
		}

		// Get lock on the jobManager object and propagate the state change
		synchronized (this.jobManager) {
			try {
				this.jobManager.updateTaskExecutionState(new TaskExecutionState(jobID, id, newExecutionState,
					optionalDescription));
			} catch (IOException e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Called by an {@link EnvironmentWrapper} object to indicate that a task has exhausted its initial execution
	 * resources.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param id
	 *        the ID of the vertex representing the task
	 * @param resourceUtilizationSnapshot
	 *        snapshot of the task's resource utilization taken at the point in time when the exhaustion occurred
	 */
	void initialExecutionResourcesExhausted(final JobID jobID, final ExecutionVertexID id,
			final ResourceUtilizationSnapshot resourceUtilizationSnapshot) {

		synchronized (this.jobManager) {
			try {
				this.jobManager.initialExecutionResourcesExhausted(jobID, id, resourceUtilizationSnapshot);
			} catch (IOException e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}
	}

	void checkpointStateChanged(final JobID jobID, final ExecutionVertexID id, final CheckpointState newCheckpointState) {

		synchronized (this.jobManager) {
			try {
				this.jobManager.updateCheckpointState(new TaskCheckpointState(jobID, id, newCheckpointState));
			} catch (IOException e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Shuts the task manager down.
	 */
	public synchronized void shutdown() {

		if (this.isShutDown) {
			return;
		}

		LOG.info("Shutting down TaskManager");

		// Stop RPC proxy for the task manager
		RPC.stopProxy(this.jobManager);

		// Shut down the own RPC server
		this.taskManagerServer.stop();

		// Stop profiling if enabled
		if (this.profiler != null) {
			this.profiler.shutdown();
		}

		// Shut down the network channel manager
		this.byteBufferedChannelManager.shutdown();

		// Shut down the memory manager
		if (this.ioManager != null) {
			this.ioManager.shutdown();
		}

		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
		}

		// Shut down the plugins
		final Iterator<TaskManagerPlugin> it = this.taskManagerPlugins.values().iterator();
		while (it.hasNext()) {
			it.next().shutdown();
		}

		this.isShutDown = true;
	}

	/**
	 * Checks whether the task manager has already been shut down.
	 * 
	 * @return <code>true</code> if the task manager has already been shut down, <code>false</code> otherwise
	 */
	public synchronized boolean isShutDown() {

		return this.isShutDown;
	}

	/**
	 * This method is periodically called by the framework to check
	 * the state of the task threads. If any task thread has unexpectedly
	 * switch to TERMINATED, this indicates that an {@link Error} has occurred
	 * during its execution.
	 */
	private void checkTaskExecution() {

		final List<Task> failedTasks = new ArrayList<Task>();

		synchronized (this.runningTasks) {

			final Iterator<ExecutionVertexID> it = this.runningTasks.keySet().iterator();
			while (it.hasNext()) {
				final ExecutionVertexID executionVertexID = it.next();
				final Task task = this.runningTasks.get(executionVertexID);

				if (task.isTerminated()) {
					// Remove entry from the running tasks map
					it.remove();
					// Don't to IPC call while holding a lock on the runningTasks map
					failedTasks.add(task);
				}
			}
		}

		final Iterator<Task> it2 = failedTasks.iterator();
		while (it2.hasNext()) {
			it2.next().markAsFailed();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeCheckpoints(List<ExecutionVertexID> listOfVertexIDs) throws IOException {

		final Iterator<ExecutionVertexID> it = listOfVertexIDs.iterator();
		while (it.hasNext()) {
			this.checkpointManager.removeCheckpoint(it.next());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logBufferUtilization() throws IOException {

		this.byteBufferedChannelManager.logBufferUtilization();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void propagateCheckpointDecisions(final List<CheckpointDecision> checkpointDecisions) throws IOException {

		this.byteBufferedChannelManager.reportCheckpointDecisions(checkpointDecisions);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killTaskManager() throws IOException {

		// Kill the entire JVM after a delay of 10ms, so this RPC will finish properly before
		final Timer timer = new Timer();
		final TimerTask timerTask = new TimerTask() {

			@Override
			public void run() {

				System.exit(0);
			}
		};

		timer.schedule(timerTask, 10L);
	}

	/**
	 * Sends data from the plugin with the given ID to the respective component of the plugin running at the job
	 * manager.
	 * 
	 * @param pluginID
	 *        the ID of plugin
	 * @param data
	 *        the data to be sent
	 * @throws IOException
	 *         thrown if an I/O error occurs during the RPC call
	 */
	public void sendData(final PluginID pluginID, final IOReadableWritable data) throws IOException {

		synchronized (this.pluginCommunicationService) {
			this.pluginCommunicationService.sendData(pluginID, data);
		}
	}

	/**
	 * Sends a list of data from the plugin with the given ID to the respective component of the plugin running at the
	 * job manager.
	 * 
	 * @param pluginID
	 *        the ID of plugin
	 * @param listOfData
	 *        the data to be sent
	 * @throws IOException
	 *         thrown if an I/O error occurs during the RPC call
	 */
	public void sendData(final PluginID pluginID, final List<IOReadableWritable> listOfData) throws IOException {

		synchronized (this.pluginCommunicationService) {
			this.pluginCommunicationService.sendData(pluginID, listOfData);
		}
	}

	/**
	 * Requests data for the plugin with the given ID from the respective plugin component running at the job manager.
	 * 
	 * @param pluginID
	 *        the ID of the plugin
	 * @param data
	 *        the data to specify the request
	 * @return the requested data
	 * @throws IOException
	 *         thrown if an I/O error occurs during the RPC call
	 */
	public IOReadableWritable requestData(final PluginID pluginID, final IOReadableWritable data) throws IOException {

		synchronized (this.pluginCommunicationService) {
			return this.pluginCommunicationService.requestData(pluginID, data);
		}
	}

	/**
	 * Requests data for the plugin with the given ID from the respective plugin component running at the job manager.
	 * 
	 * @param pluginID
	 *        the ID of the plugin
	 * @param listOfData
	 *        the list of data to specify the request
	 * @return the requested data
	 * @throws IOException
	 *         thrown if an I/O error occurs during the RPC call
	 */
	public IOReadableWritable requestData(final PluginID pluginID, final List<IOReadableWritable> listOfData)
			throws IOException {

		synchronized (this.pluginCommunicationService) {
			return this.pluginCommunicationService.requestData(pluginID, listOfData);
		}
	}
}
