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

package org.apache.flink.runtime.taskmanager;

import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.protocols.VersionedProtocol;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FallbackLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.Hardware;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.ChannelManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkConnectionManager;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.ipc.Server;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.runtime.profiling.ProfilingUtils;
import org.apache.flink.runtime.profiling.TaskManagerProfiler;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;
import org.apache.flink.runtime.protocols.ChannelLookupProtocol;
import org.apache.flink.runtime.protocols.InputSplitProviderProtocol;
import org.apache.flink.runtime.protocols.JobManagerProtocol;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A task manager receives tasks from the job manager and executes them. After having executed them
 * (or in case of an execution error) it reports the execution result back to the job manager.
 * Task managers are able to automatically discover the job manager and receive its configuration from it
 * as long as the job manager is running on the same local network
 */
public class TaskManager implements TaskOperationProtocol {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;
	
	private static final int MAX_LOST_HEART_BEATS = 3;
	
	private static final int DELAY_AFTER_LOST_CONNECTION = 10000;
	
	
	public final static String ARG_CONF_DIR = "tempDir";
	
	// --------------------------------------------------------------------------------------------
	
	private final ExecutorService executorService = Executors.newFixedThreadPool(2 * Hardware.getNumberCPUCores(), ExecutorThreadFactory.INSTANCE);
	
	
	private final InstanceConnectionInfo localInstanceConnectionInfo;
	
	private final HardwareDescription hardwareDescription;
	
	private final ExecutionMode executionMode;
	
	
	private final JobManagerProtocol jobManager;

	private final InputSplitProviderProtocol globalInputSplitProvider;

	private final ChannelLookupProtocol lookupService;
	
	private final AccumulatorProtocol accumulatorProtocolProxy;

	private final LibraryCacheManager libraryCacheManager;
	
	private final Server taskManagerServer;

	private final FileCache fileCache = new FileCache();
	
	/** All currently running tasks */
	private final ConcurrentHashMap<ExecutionAttemptID, Task> runningTasks = new ConcurrentHashMap<ExecutionAttemptID, Task>();

	/** The {@link ChannelManager} sets up and cleans up the data exchange channels of the tasks. */
	private final ChannelManager channelManager;

	/** Instance of the task manager profile if profiling is enabled. */
	private final TaskManagerProfiler profiler;

	private final MemoryManager memoryManager;

	private final IOManager ioManager;

	private final int numberOfSlots;

	private final Thread heartbeatThread;
	
	private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);
	
	private volatile InstanceID registeredId;
	
	/** Stores whether the task manager has already been shut down. */
	private volatile boolean shutdownComplete;
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructor & Shutdown
	// --------------------------------------------------------------------------------------------
	
	public TaskManager(ExecutionMode executionMode, JobManagerProtocol jobManager, InputSplitProviderProtocol splitProvider, 
			ChannelLookupProtocol channelLookup, AccumulatorProtocol accumulators,
			InetSocketAddress jobManagerAddress, InetAddress taskManagerBindAddress)
		throws Exception
	{
		if (executionMode == null || jobManager == null || splitProvider == null || channelLookup == null || accumulators == null) {
			throw new NullPointerException();
		}
		
		LOG.info("TaskManager execution mode: " + executionMode);
		
		this.executionMode = executionMode;
		this.jobManager = jobManager;
		this.lookupService = channelLookup;
		this.globalInputSplitProvider = splitProvider;
		this.accumulatorProtocolProxy = accumulators;
		
		// initialize the number of slots
		{
			int slots = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, -1);
			if (slots == -1) {
				slots = 1;
				LOG.info("Number of task slots not configured. Creating one task slot.");
			} else if (slots <= 0) {
				throw new Exception("Illegal value for the number of task slots: " + slots);
			} else {
				LOG.info("Creating " + slots + " task slot(s).");
			}
			this.numberOfSlots = slots;
		}
		
		int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, -1);
		int dataPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, -1);
		if (ipcPort == -1) {
			ipcPort = getAvailablePort();
		}
		if (dataPort == -1) {
			dataPort = getAvailablePort();
		}
		
		this.localInstanceConnectionInfo = new InstanceConnectionInfo(taskManagerBindAddress, ipcPort, dataPort);
		LOG.info("TaskManager connection information:" + this.localInstanceConnectionInfo);

		// Start local RPC server, give it the number of threads as we have slots
		try {
			// some magic number for the handler threads
			final int numHandlers = Math.min(numberOfSlots, 2*Hardware.getNumberCPUCores());
			
			this.taskManagerServer = RPC.getServer(this, taskManagerBindAddress.getHostAddress(), ipcPort, numHandlers);
			this.taskManagerServer.start();
		} catch (IOException e) {
			LOG.error("Failed to start TaskManager server. " + e.getMessage(), e);
			throw new Exception("Failed to start taskmanager server. " + e.getMessage(), e);
		}
		

		// Load profiler if it should be used
		if (GlobalConfiguration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY, false)) {
			
			final String profilerClassName = GlobalConfiguration.getString(ProfilingUtils.TASKMANAGER_CLASSNAME_KEY,
				"org.apache.flink.runtime.profiling.impl.TaskManagerProfilerImpl");
			
			this.profiler = ProfilingUtils.loadTaskManagerProfiler(profilerClassName, jobManagerAddress.getAddress(),
				this.localInstanceConnectionInfo);
			
			if (this.profiler == null) {
				LOG.error("Cannot find class name for the profiler.");
			} else {
				LOG.info("Profiling of jobs is enabled.");
			}
		} else {
			this.profiler = null;
			LOG.info("Profiling of jobs is disabled.");
		}

		// Get the directory for storing temporary files
		final String[] tmpDirPaths = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(",|" + File.pathSeparator);

		checkTempDirs(tmpDirPaths);

		int numBuffers = GlobalConfiguration.getInteger(
				ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS);

		int bufferSize = GlobalConfiguration.getInteger(
				ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE);

		// Initialize the channel manager
		try {
			NetworkConnectionManager networkConnectionManager = null;

			switch (executionMode) {
				case LOCAL:
					networkConnectionManager = new LocalConnectionManager();
					break;
				case CLUSTER:
					int numInThreads = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NET_NUM_IN_THREADS_KEY,
							ConfigConstants.DEFAULT_TASK_MANAGER_NET_NUM_IN_THREADS);
	
					int numOutThreads = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NET_NUM_OUT_THREADS_KEY,
							ConfigConstants.DEFAULT_TASK_MANAGER_NET_NUM_OUT_THREADS);
	
					int lowWaterMark = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NET_NETTY_LOW_WATER_MARK,
							ConfigConstants.DEFAULT_TASK_MANAGER_NET_NETTY_LOW_WATER_MARK);
	
					int highWaterMark = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NET_NETTY_HIGH_WATER_MARK,
							ConfigConstants.DEFAULT_TASK_MANAGER_NET_NETTY_HIGH_WATER_MARK);
	
					networkConnectionManager = new NettyConnectionManager(localInstanceConnectionInfo.address(),
							localInstanceConnectionInfo.dataPort(), bufferSize, numInThreads, numOutThreads, lowWaterMark, highWaterMark);
					break;
			}

			channelManager = new ChannelManager(lookupService, localInstanceConnectionInfo, numBuffers, bufferSize, networkConnectionManager);
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
			throw new Exception("Failed to instantiate ChannelManager.", ioe);
		}
		
		// initialize the memory manager
		{
			// Check whether the memory size has been explicitly configured.
			final long configuredMemorySize = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1);
			final long memorySize;
			
			if (configuredMemorySize == -1) {
				// no manually configured memory. take a relative fraction of the free heap space
				float fraction = GlobalConfiguration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);
				memorySize = (long) (EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag() * fraction);
				LOG.info("Using " + fraction + " of the free heap space for managed memory.");
			}
			else if (configuredMemorySize <= 0) {
				throw new Exception("Invalid value for Memory Manager memory size: " + configuredMemorySize);
			}
			else {
				memorySize = configuredMemorySize << 20;
			}

			final int pageSize = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE);

			// Initialize the memory manager
			LOG.info("Initializing memory manager with " + (memorySize >>> 20) + " megabytes of memory. " +
					"Page size is " + pageSize + " bytes.");
			
			try {
				@SuppressWarnings("unused")
				final boolean lazyAllocation = GlobalConfiguration.getBoolean(ConfigConstants.TASK_MANAGER_MEMORY_LAZY_ALLOCATION_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_LAZY_ALLOCATION);
				
				this.memoryManager = new DefaultMemoryManager(memorySize, this.numberOfSlots, pageSize);
			} catch (Throwable t) {
				LOG.error("Unable to initialize memory manager with " + (memorySize >>> 20) + " megabytes of memory.", t);
				throw new Exception("Unable to initialize memory manager.", t);
			}
		}
		
		this.hardwareDescription = HardwareDescription.extractFromSystem(this.memoryManager.getMemorySize());

		// Determine the port of the BLOB server and register it with the library cache manager
		{
			final int blobPort = this.jobManager.getBlobServerPort();

			if (blobPort == -1) {
				LOG.warn("Unable to determine BLOB server address: User library download will not be available");
				this.libraryCacheManager = new FallbackLibraryCacheManager();
			} else {
				final InetSocketAddress blobServerAddress = new InetSocketAddress(
					jobManagerAddress.getAddress(), blobPort);
				LOG.info("Determined BLOB server address to be " + blobServerAddress);

				this.libraryCacheManager = new BlobLibraryCacheManager(new BlobCache
						(blobServerAddress), GlobalConfiguration.getConfiguration());
			}
		}
		this.ioManager = new IOManager(tmpDirPaths);
		
		// start the heart beats
		{
			final long interval = GlobalConfiguration.getInteger(
					ConfigConstants.TASK_MANAGER_HEARTBEAT_INTERVAL_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_HEARTBEAT_INTERVAL);
			
			this.heartbeatThread = new Thread() {
				@Override
				public void run() {
					registerAndRunHeartbeatLoop(interval, MAX_LOST_HEART_BEATS);
				}
			};
			this.heartbeatThread.setName("Heartbeat Thread");
			this.heartbeatThread.start();
		}

		// --------------------------------------------------------------------
		// Memory Usage
		// --------------------------------------------------------------------

		final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
		final List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();

		LOG.info(getMemoryUsageStatsAsString(memoryMXBean));

		boolean startMemoryUsageLogThread = GlobalConfiguration.getBoolean(
				ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
				ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD);

		if (startMemoryUsageLogThread && LOG.isDebugEnabled()) {
			final int logIntervalMs = GlobalConfiguration.getInteger(
					ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS,
					ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS);

			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						while (!isShutDown()) {
							Thread.sleep(logIntervalMs);

							if (LOG.isDebugEnabled()) {
								LOG.debug(getMemoryUsageStatsAsString(memoryMXBean));
								LOG.debug(getGarbageCollectorStatsAsString(gcMXBeans));
							}
						}
					} catch (InterruptedException e) {
						LOG.warn("Unexpected interruption of memory usage logger thread.");
					}
				}
			}).start();
		}
	}
	
	/**
	 * Shuts the task manager down.
	 */
	public void shutdown() {
		if (!this.shutdownStarted.compareAndSet(false, true)) {
			return;
		}

		LOG.info("Shutting down TaskManager");
		
		cancelAndClearEverything(new Exception("Task Manager is shutting down"));
		
		// first, stop the heartbeat thread and wait for it to terminate
		this.heartbeatThread.interrupt();
		try {
			this.heartbeatThread.join(1000);
		} catch (InterruptedException e) {}

		// Stop RPC proxy for the task manager
		stopProxy(this.jobManager);

		// Stop RPC proxy for the global input split assigner
		stopProxy(this.globalInputSplitProvider);

		// Stop RPC proxy for the lookup service
		stopProxy(this.lookupService);

		// Stop RPC proxy for accumulator reports
		stopProxy(this.accumulatorProtocolProxy);

		// Shut down the own RPC server
		try {
			this.taskManagerServer.stop();
		} catch (Throwable t) {
			LOG.warn("TaskManager RPC server did not shut down properly.", t);
		}

		// Stop profiling if enabled
		if (this.profiler != null) {
			this.profiler.shutdown();
		}

		// Shut down the channel manager
		try {
			this.channelManager.shutdown();
		} catch (Throwable t) {
			LOG.warn("ChannelManager did not shutdown properly: " + t.getMessage(), t);
		}

		// Shut down the memory manager
		if (this.ioManager != null) {
			this.ioManager.shutdown();
		}

		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
		}

		if(libraryCacheManager != null){
			try {
				this.libraryCacheManager.shutdown();
			} catch (IOException e) {
				LOG.warn("Could not properly shutdown the library cache manager.", e);
			}
		}

		this.fileCache.shutdown();

		// Shut down the executor service
		if (this.executorService != null) {
			this.executorService.shutdown();
			try {
				this.executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOG.debug("Shutdown of executor thread pool interrupted", e);
			}
		}

		this.shutdownComplete = true;
	}
	
	/**
	 * Checks whether the task manager has already been shut down.
	 * 
	 * @return <code>true</code> if the task manager has already been shut down, <code>false</code> otherwise
	 */
	public boolean isShutDown() {
		return this.shutdownComplete;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	public InstanceConnectionInfo getConnectionInfo() {
		return this.localInstanceConnectionInfo;
	}
	
	public ExecutionMode getExecutionMode() {
		return this.executionMode;
	}
	
	/**
	 * Gets the ID under which the TaskManager is currently registered at its JobManager.
	 * If the TaskManager has not been registered, yet, or if it lost contact, this is is null.
	 * 
	 * @return The ID under which the TaskManager is currently registered.
	 */
	public InstanceID getRegisteredId() {
		return this.registeredId;
	}
	
	/**
	 * Checks if the TaskManager is properly registered and ready to receive work.
	 * 
	 * @return True, if the TaskManager is registered, false otherwise.
	 */
	public boolean isRegistered() {
		return this.registeredId != null;
	}
	
	public Map<ExecutionAttemptID, Task> getAllRunningTasks() {
		return Collections.unmodifiableMap(this.runningTasks);
	}
	
	public ChannelManager getChannelManager() {
		return channelManager;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Task Operation
	// --------------------------------------------------------------------------------------------

	@Override
	public TaskOperationResult cancelTask(ExecutionAttemptID executionId) throws IOException {

		final Task task = this.runningTasks.get(executionId);

		if (task == null) {
			return new TaskOperationResult(executionId, false, "No task with that execution ID was found.");
		}

		// Pass call to executor service so IPC thread can return immediately
		final Runnable r = new Runnable() {
			@Override
			public void run() {
				task.cancelExecution();
			}
		};
		this.executorService.execute(r);

		// return success
		return new TaskOperationResult(executionId, true);
	}


	@Override
	public TaskOperationResult submitTask(TaskDeploymentDescriptor tdd) {
		final JobID jobID = tdd.getJobID();
		final JobVertexID vertexId = tdd.getVertexID();
		final ExecutionAttemptID executionId = tdd.getExecutionId();
		final int taskIndex = tdd.getIndexInSubtaskGroup();
		final int numSubtasks = tdd.getCurrentNumberOfSubtasks();
		
		boolean jarsRegistered = false;
		
		try {
			// Now register data with the library manager
			libraryCacheManager.register(jobID, tdd.getRequiredJarFiles());

			// library and classloader issues first
			jarsRegistered = true;

			final ClassLoader userCodeClassLoader = libraryCacheManager.getClassLoader(jobID);
			if (userCodeClassLoader == null) {
				throw new Exception("No user code ClassLoader available.");
			}
			
			final Task task = new Task(jobID, vertexId, taskIndex, numSubtasks, executionId, tdd.getTaskName(), this);
			if (this.runningTasks.putIfAbsent(executionId, task) != null) {
				throw new Exception("TaskManager contains already a task with executionId " + executionId);
			}
			
			// another try/finally-success block to ensure that the tasks are removed properly in case of an exception
			boolean success = false;
			try {
				final InputSplitProvider splitProvider = new TaskInputSplitProvider(this.globalInputSplitProvider, jobID, vertexId);
				final RuntimeEnvironment env = new RuntimeEnvironment(task, tdd, userCodeClassLoader, this.memoryManager, this.ioManager, splitProvider, this.accumulatorProtocolProxy);
				task.setEnvironment(env);
				
				// register the task with the network stack and profilers
				this.channelManager.register(task);
				
				final Configuration jobConfig = tdd.getJobConfiguration();
	
				boolean enableProfiling = this.profiler != null && jobConfig.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true);
	
				// Register environment, input, and output gates for profiling
				if (enableProfiling) {
					task.registerProfiler(this.profiler, jobConfig);
				}
				
				// now that the task is successfully created and registered, we can start copying the
				// distributed cache temp files
				Map<String, FutureTask<Path>> cpTasks = new HashMap<String, FutureTask<Path>>();
				for (Entry<String, DistributedCacheEntry> e : DistributedCache.readFileInfoFromConfig(tdd.getJobConfiguration())) {
					FutureTask<Path> cp = this.fileCache.createTmpFile(e.getKey(), e.getValue(), jobID);
					cpTasks.put(e.getKey(), cp);
				}
				env.addCopyTasksForCacheFile(cpTasks);
				
				if (!task.startExecution()) {
					throw new Exception("Cannot start task. Task was canceled or failed.");
				}
			
				// final check that we can go (we do this after the registration, so the the "happen's before"
				// relationship ensures that either the shutdown removes this task, or we are aware of the shutdown
				if (shutdownStarted.get()) {
					throw new Exception("Task Manager is shut down.");
				}
				
				success = true;
				return new TaskOperationResult(executionId, true);
			}
			finally {
				if (!success) {
					// remove task 
					this.runningTasks.remove(executionId);
					
					// delete distributed cache files
					for (Entry<String, DistributedCacheEntry> e : DistributedCache.readFileInfoFromConfig(tdd.getJobConfiguration())) {
						this.fileCache.deleteTmpFile(e.getKey(), e.getValue(), jobID);
					}
				}
			}
		}
		catch (Throwable t) {
			LOG.error("Could not instantiate task", t);
			
			if (jarsRegistered) {
				libraryCacheManager.unregister(jobID);
			}
			
			return new TaskOperationResult(executionId, false, ExceptionUtils.stringifyException(t));
		}
	}

	/**
	 * Unregisters a finished or aborted task.
	 * 
	 * @param executionId
	 *        the ID of the task to be unregistered
	 */
	private void unregisterTask(ExecutionAttemptID executionId) {

		// Task de-registration must be atomic
		final Task task = this.runningTasks.remove(executionId);
		if (task == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Cannot find task with ID " + executionId + " to unregister");
			}
			return;
		}

		// remove the local tmp file for unregistered tasks.
		for (Entry<String, DistributedCacheEntry> e: DistributedCache.readFileInfoFromConfig(task.getEnvironment().getJobConfiguration())) {
			this.fileCache.deleteTmpFile(e.getKey(), e.getValue(), task.getJobID());
		}
		
		// Unregister task from the byte buffered channel manager
		this.channelManager.unregister(executionId, task);

		// Unregister task from profiling
		task.unregisterProfiler(this.profiler);

		// Unregister task from memory manager
		task.unregisterMemoryManager(this.memoryManager);

		// Unregister task from library cache manager
		libraryCacheManager.unregister(task.getJobID());
	}

	public void notifyExecutionStateChange(JobID jobID, ExecutionAttemptID executionId, ExecutionState newExecutionState, Throwable optionalError) {
		
		// Get lock on the jobManager object and propagate the state change
		boolean success = false;
		try {
			success = this.jobManager.updateTaskExecutionState(new TaskExecutionState(jobID, executionId, newExecutionState, optionalError));
		}
		catch (Throwable t) {
			String msg = "Error sending task state update to JobManager.";
			LOG.error(msg, t);
			ExceptionUtils.rethrow(t, msg);
		}
		finally {
			// in case of a failure, or when the tasks is in a finished state, then unregister the
			// task (free all buffers, remove all channels, task-specific class loaders, etc...)
			if (!success || newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
					|| newExecutionState == ExecutionState.FAILED)
			{
				unregisterTask(executionId);
			}
		}
	}

	/**
	 * Removes all tasks from this TaskManager.
	 */
	public void cancelAndClearEverything(Throwable cause) {
		if (runningTasks.size() > 0) {
			LOG.info("Cancelling all computations and discarding all cached data.");
			
			for (Task t : runningTasks.values()) {
				t.failExternally(cause);
				runningTasks.remove(t.getExecutionId());
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Heartbeats
	// --------------------------------------------------------------------------------------------
	
	/**
	 * This method registers the TaskManager at the jobManager and send periodic heartbeats.
	 */
	private void registerAndRunHeartbeatLoop(long interval, int maxNonSuccessfulHeatbeats) {

		while (!shutdownStarted.get()) {
			InstanceID resultId = null;
	
			// try to register. We try as long as we need to, because it may be that the jobmanager is  not yet online
			{
				final long maxDelay = 10000;	// the maximal delay between registration attempts
				final long reportingDelay = 5000;
				long currentDelay = 100;		// initially, wait 100 msecs for the next registration attempt
				
				while (!shutdownStarted.get())
				{
					if (LOG.isDebugEnabled()) {
						LOG.debug("Trying to register at Jobmanager...");
					}
					
					try {
						resultId = this.jobManager.registerTaskManager(this.localInstanceConnectionInfo,
								this.hardwareDescription, this.numberOfSlots);
						
						if (resultId == null) {
							throw new Exception("Registration attempt refused by JobManager.");
						}
					}
					catch (Exception e) {
						// this may be if the job manager was not yet online
						// if this has happened for a while, report it. if it has just happened
						// at the very beginning, this may not mean anything (JM still in startup)
						if (currentDelay >= reportingDelay) {
							LOG.error("Connection to JobManager failed.", e);
						} else if (LOG.isDebugEnabled()) {
							LOG.debug("Could not connect to JobManager.", e);
						}
					}
					
					// check if we were accepted
					if (resultId != null) {
						// success
						this.registeredId = resultId;
						break;
					}
		
					try {
						Thread.sleep(currentDelay);
					}
					catch (InterruptedException e) {
						// may be due to shutdown
						if (!shutdownStarted.get()) {
							LOG.error("TaskManager's registration loop was interrupted without shutdown.");
						}
					}
					
					// increase the time between registration attempts, to not keep on pinging overly frequently
					currentDelay = Math.min(2 * currentDelay, maxDelay);
				}
			}
			
			// registration complete, or shutdown
			int successiveUnsuccessfulHeartbeats = 0;
			
			// the heart beat loop
			while (!shutdownStarted.get()) {
				// sleep until the next heart beat
				try {
					Thread.sleep(interval);
				}
				catch (InterruptedException e) {
					if (!shutdownStarted.get()) {
						LOG.error("TaskManager heart beat loop was interrupted without shutdown.");
					}
				}
	
				// send heart beat
				try {
					boolean accepted = this.jobManager.sendHeartbeat(resultId);
					
					if (accepted) {
						// reset the unsuccessful heart beats
						successiveUnsuccessfulHeartbeats = 0;
					} else {
						successiveUnsuccessfulHeartbeats++;
						LOG.error("JobManager rejected heart beat.");
					}
				}
				catch (IOException e) {
					if (!shutdownStarted.get()) {
						successiveUnsuccessfulHeartbeats++;
						LOG.error("Sending the heart beat failed on I/O error: " + e.getMessage(), e);
					}
				}
				
				if (successiveUnsuccessfulHeartbeats == maxNonSuccessfulHeatbeats) {
					// we are done for, we cannot connect to the jobmanager any more
					// or we are not welcome there any more
					// what to do now? Wait for a while and try to reconnect
					LOG.error("TaskManager has lost connection to JobManager.");
					
					// mark us as disconnected and abort all computation
					this.registeredId = null;
					cancelAndClearEverything(new Exception("TaskManager lost heartbeat connection to JobManager"));
					
					// wait for a while, then attempt to register again
					try {
						Thread.sleep(DELAY_AFTER_LOST_CONNECTION);
					}
					catch (InterruptedException e) {
						if (!shutdownStarted.get()) {
							LOG.error("TaskManager heart beat loop was interrupted without shutdown.");
						}
					}
					
					// leave the heart beat loop
					break;
				}
			} // end heart beat loop
		} // end while not shutdown
	}
	
	// --------------------------------------------------------------------------------------------
	//  Memory and Garbage Collection Debugging Utilities
	// --------------------------------------------------------------------------------------------

	private String getMemoryUsageStatsAsString(MemoryMXBean memoryMXBean) {
		MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
		MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();

		int mb = 1 << 20;

		int heapUsed = (int) (heap.getUsed() / mb);
		int heapCommitted = (int) (heap.getCommitted() / mb);
		int heapMax = (int) (heap.getMax() / mb);

		int nonHeapUsed = (int) (nonHeap.getUsed() / mb);
		int nonHeapCommitted = (int) (nonHeap.getCommitted() / mb);
		int nonHeapMax = (int) (nonHeap.getMax() / mb);

		String msg = String.format("Memory usage stats: [HEAP: %d/%d/%d MB, NON HEAP: %d/%d/%d MB (used/comitted/max)]",
				heapUsed, heapCommitted, heapMax, nonHeapUsed, nonHeapCommitted, nonHeapMax);

		return msg;
	}

	private String getGarbageCollectorStatsAsString(List<GarbageCollectorMXBean> gcMXBeans) {
		StringBuilder str = new StringBuilder();
		str.append("Garbage collector stats: ");

		for (int i = 0; i < gcMXBeans.size(); i++) {
			GarbageCollectorMXBean bean = gcMXBeans.get(i);

			String msg = String.format("[%s, GC TIME (ms): %d, GC COUNT: %d]",
					bean.getName(), bean.getCollectionTime(), bean.getCollectionCount());
			str.append(msg);
			str.append(i < gcMXBeans.size() - 1 ? ", " : "");
		}

		return str.toString();
	}

	
	// --------------------------------------------------------------------------------------------
	//  Execution & Initialization
	// --------------------------------------------------------------------------------------------
	
	public static TaskManager createTaskManager(ExecutionMode mode) throws Exception {
		
		// IMPORTANT! At this point, the GlobalConfiguration must have been read!
		
		final InetSocketAddress jobManagerAddress;
		LOG.info("Reading location of job manager from configuration");
					
		final String address = GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
					
		if (address == null) {
			throw new Exception("Job manager address not configured in the GlobalConfiguration.");
		}
			
		// Try to convert configured address to {@link InetAddress}
		try {
			final InetAddress tmpAddress = InetAddress.getByName(address);
			jobManagerAddress = new InetSocketAddress(tmpAddress, port);
		}
		catch (UnknownHostException e) {
			LOG.error("Could not resolve JobManager host name.");
			throw new Exception("Could not resolve JobManager host name: " + e.getMessage(), e);
		}
		
		return createTaskManager(mode, jobManagerAddress);
	}
	
	public static TaskManager createTaskManager(ExecutionMode mode, InetSocketAddress jobManagerAddress) throws Exception {
		// Determine our own public facing address and start the server
		final InetAddress taskManagerAddress;
		try {
			taskManagerAddress = getTaskManagerAddress(jobManagerAddress);
		}
		catch (IOException e) {
			throw new Exception("The TaskManager failed to determine the IP address of the interface that connects to the JobManager.", e);
		}
		
		return createTaskManager(mode, jobManagerAddress, taskManagerAddress);
	}
	
	
	public static TaskManager createTaskManager(ExecutionMode mode, InetSocketAddress jobManagerAddress, InetAddress taskManagerAddress) throws Exception {
		
		// IMPORTANT! At this point, the GlobalConfiguration must have been read!
		
		LOG.info("Connecting to JobManager at: " + jobManagerAddress);
		
		// Create RPC connections to the JobManager
		
		JobManagerProtocol jobManager = null;
		InputSplitProviderProtocol splitProvider = null;
		ChannelLookupProtocol channelLookup = null;
		AccumulatorProtocol accumulators = null;
		
		// try/finally block to close proxies if anything goes wrong
		boolean success = false;
		try {
			// create the RPC call proxy to the job manager for jobs
			try {
				jobManager = RPC.getProxy(JobManagerProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
			}
			catch (IOException e) {
				LOG.error("Could not connect to the JobManager: " + e.getMessage(), e);
				throw new Exception("Failed to initialize connection to JobManager: " + e.getMessage(), e);
			}
			
			// Try to create local stub of the global input split provider
			try {
				splitProvider = RPC.getProxy(InputSplitProviderProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
			}
			catch (IOException e) {
				LOG.error(e.getMessage(), e);
				throw new Exception("Failed to initialize connection to global input split provider: " + e.getMessage(), e);
			}

			// Try to create local stub for the lookup service
			try {
				channelLookup = RPC.getProxy(ChannelLookupProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
			}
			catch (IOException e) {
				LOG.error(e.getMessage(), e);
				throw new Exception("Failed to initialize channel lookup protocol. " + e.getMessage(), e);
			}

			// Try to create local stub for the accumulators
			try {
				accumulators = RPC.getProxy(AccumulatorProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
			}
			catch (IOException e) {
				LOG.error("Failed to initialize accumulator protocol: " + e.getMessage(), e);
				throw new Exception("Failed to initialize accumulator protocol: " + e.getMessage(), e);
			}
			
			TaskManager tm = new TaskManager(mode, jobManager, splitProvider, channelLookup, accumulators, jobManagerAddress, taskManagerAddress);
			success = true;
			return tm;
		}
		finally {
			if (!success) {
				stopProxy(jobManager);
				stopProxy(splitProvider);
				stopProxy(channelLookup);
				stopProxy(accumulators);
			}
		}
	}
	

	// --------------------------------------------------------------------------------------------
	//  Executable
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Entry point for the TaskManager executable.
	 * 
	 * @param args Arguments from the command line
	 * @throws IOException 
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException {		
		Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg().withDescription(
			"Specify configuration directory.").create("configDir");
		// tempDir option is used by the YARN client.
		Option tempDir = OptionBuilder.withArgName("temporary directory (overwrites configured option)")
				.hasArg().withDescription(
				"Specify temporary directory.").create(ARG_CONF_DIR);
		configDirOpt.setRequired(true);
		tempDir.setRequired(false);
		Options options = new Options();
		options.addOption(configDirOpt);
		options.addOption(tempDir);
		

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("CLI Parsing failed. Reason: " + e.getMessage());
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}

		String configDir = line.getOptionValue(configDirOpt.getOpt(), null);
		String tempDirVal = line.getOptionValue(tempDir.getOpt(), null);

		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);
		if(tempDirVal != null // the YARN TM runner has set a value for the temp dir
				// the configuration does not contain a temp directory
				&& GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, null) == null) {
			Configuration c = GlobalConfiguration.getConfiguration();
			c.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, tempDirVal);
			LOG.info("Setting temporary directory to "+tempDirVal);
			GlobalConfiguration.includeConfiguration(c);
		}
		
		// print some startup environment info, like user, code revision, etc
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager");
		
		// Create a new task manager object
		try {
			createTaskManager(ExecutionMode.CLUSTER);
		}
		catch (Throwable t) {
			LOG.error("Taskmanager startup failed: " + t.getMessage(), t);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
		
		// park the main thread to keep the JVM alive (all other threads may be daemon threads)
		Object mon = new Object();
		synchronized (mon) {
			try {
				mon.wait();
			} catch (InterruptedException ex) {}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// Miscellaneous Utilities
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Checks, whether the given strings describe existing directories that are writable. If that is not
	 * the case, an exception is raised.
	 * 
	 * @param tempDirs An array of strings which are checked to be paths to writable directories.
	 * @throws Exception Thrown, if any of the mentioned checks fails.
	 */
	private static final void checkTempDirs(final String[] tempDirs) throws Exception {
		for (int i = 0; i < tempDirs.length; ++i) {
			final String dir = tempDirs[i];
			if (dir == null) {
				throw new Exception("Temporary file directory #" + (i + 1) + " is null.");
			}

			final File f = new File(dir);

			if (!f.exists()) {
				throw new Exception("Temporary file directory '" + f.getAbsolutePath() + "' does not exist.");
			}
			if (!f.isDirectory()) {
				throw new Exception("Temporary file directory '" + f.getAbsolutePath() + "' is not a directory.");
			}
			if (!f.canWrite()) {
				throw new Exception("Temporary file directory '" + f.getAbsolutePath() + "' is not writable.");
			}
		}
	}
	
	/**
	 * Stops the given RPC protocol proxy, if it is not null.
	 * This method never throws an exception, it only logs errors.
	 * 
	 * @param protocol The protocol proxy to stop.
	 */
	private static final void stopProxy(VersionedProtocol protocol) {
		if (protocol != null) {
			try {
				RPC.stopProxy(protocol);
			}
			catch (Throwable t) {
				LOG.error("Error while shutting down RPC proxy.", t);
			}
		}
	}
	
	/**
	 * Determines the IP address of the interface from which the TaskManager can connect to the given JobManager
	 * IP address.
	 * 
	 * @param jobManagerAddress The socket address to connect to.
	 * @return The IP address of the interface that connects to the JobManager.
	 * @throws IOException If no connection could be established.
	 */
	private static InetAddress getTaskManagerAddress(InetSocketAddress jobManagerAddress) throws IOException {
		AddressDetectionState strategy = AddressDetectionState.ADDRESS;

		while (true) {
			Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
			while (e.hasMoreElements()) {
				NetworkInterface n = e.nextElement();
				Enumeration<InetAddress> ee = n.getInetAddresses();
				while (ee.hasMoreElements()) {
					InetAddress i = ee.nextElement();
					switch (strategy) {
					case ADDRESS:
						if (hasCommonPrefix(jobManagerAddress.getAddress().getAddress(), i.getAddress())) {
							if (tryToConnect(i, jobManagerAddress, strategy.getTimeout())) {
								LOG.info("Determined " + i + " as the TaskTracker's own IP address");
								return i;
							}
						}
						break;
					case FAST_CONNECT:
					case SLOW_CONNECT:
						boolean correct = tryToConnect(i, jobManagerAddress, strategy.getTimeout());
						if (correct) {
							LOG.info("Determined " + i + " as the TaskTracker's own IP address");
							return i;
						}
						break;
					default:
						throw new RuntimeException("Unkown address detection strategy: " + strategy);
					}
				}
			}
			// state control
			switch (strategy) {
			case ADDRESS:
				strategy = AddressDetectionState.FAST_CONNECT;
				break;
			case FAST_CONNECT:
				strategy = AddressDetectionState.SLOW_CONNECT;
				break;
			case SLOW_CONNECT:
				throw new RuntimeException("The TaskManager is unable to connect to the JobManager (Address: '"+jobManagerAddress+"').");
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Defaulting to detection strategy {}", strategy);
			}
		}
	}
	
	/**
	 * Searches for an available free port and returns the port number.
	 * 
	 * @return An available port.
	 * @throws RuntimeException Thrown, if no free port was found.
	 */
	private static int getAvailablePort() {
		for (int i = 0; i < 50; i++) {
			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(0);
				int port = serverSocket.getLocalPort();
				if (port != 0) {
					return port;
				}
			} catch (IOException e) {

				LOG.debug("Unable to allocate port with exception {}", e);
			} finally {
				if (serverSocket != null) {
					try { serverSocket.close(); } catch (Throwable t) {}
				}
			}
		}
		
		throw new RuntimeException("Could not find a free permitted port on the machine.");
	}
	
	/**
	 * Checks if two addresses have a common prefix (first 2 bytes).
	 * Example: 192.168.???.???
	 * Works also with ipv6, but accepts probably too many addresses
	 */
	private static boolean hasCommonPrefix(byte[] address, byte[] address2) {
		return address[0] == address2[0] && address[1] == address2[1];
	}

	private static boolean tryToConnect(InetAddress fromAddress, SocketAddress toSocket, int timeout) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Trying to connect to JobManager (" + toSocket + ") from local address " + fromAddress
				+ " with timeout " + timeout);
		}
		boolean connectable = true;
		Socket socket = null;
		try {
			socket = new Socket();
			SocketAddress bindP = new InetSocketAddress(fromAddress, 0); // 0 = let the OS choose the port on this
																			// machine
			socket.bind(bindP);
			socket.connect(toSocket, timeout);
		} catch (Exception ex) {
			LOG.info("Failed to connect to JobManager from address '" + fromAddress + "': " + ex.getMessage());
			if (LOG.isDebugEnabled()) {
				LOG.debug("Failed with exception", ex);
			}
			connectable = false;
		} finally {
			if (socket != null) {
				socket.close();
			}
		}
		return connectable;
	}
	
	/**
	 * The states of address detection mechanism.
	 * There is only a state transition if the current state failed to determine the address.
	 */
	private enum AddressDetectionState {
		ADDRESS(50), 		//detect own IP based on the JobManagers IP address. Look for common prefix
		FAST_CONNECT(50),	//try to connect to the JobManager on all Interfaces and all their addresses.
							//this state uses a low timeout (say 50 ms) for fast detection.
		SLOW_CONNECT(1000);	//same as FAST_CONNECT, but with a timeout of 1000 ms (1s).
		
		
		private int timeout;
		AddressDetectionState(int timeout) {
			this.timeout = timeout;
		}
		public int getTimeout() {
			return timeout;
		}
	}
}
