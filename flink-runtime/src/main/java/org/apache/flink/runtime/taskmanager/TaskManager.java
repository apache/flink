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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheProfileRequest;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheProfileResponse;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheUpdate;
import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.HardwareDescriptionFactory;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.ChannelManager;
import org.apache.flink.runtime.io.network.InsufficientResourcesException;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkConnectionManager;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.ipc.Server;
import org.apache.flink.runtime.jobgraph.JobID;
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
import org.apache.flink.runtime.taskmanager.transferenvelope.RegisterTaskManagerResult;
import org.apache.flink.runtime.types.IntegerRecord;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.SerializableArrayList;
import org.apache.flink.util.StringUtils;

/**
 * A task manager receives tasks from the job manager and executes them. After having executed them
 * (or in case of an execution error) it reports the execution result back to the job manager.
 * Task managers are able to automatically discover the job manager and receive its configuration from it
 * as long as the job manager is running on the same local network
 * 
 */
public class TaskManager implements TaskOperationProtocol {

	private static final Log LOG = LogFactory.getLog(TaskManager.class);

	private final static int FAILURE_RETURN_CODE = -1;
	
	private static final int IPC_HANDLER_COUNT = 1;
	
	public final static String ARG_CONF_DIR = "tempDir";
	
	private final JobManagerProtocol jobManager;

	private final InputSplitProviderProtocol globalInputSplitProvider;

	private final ChannelLookupProtocol lookupService;

	private final ExecutorService executorService = Executors.newCachedThreadPool(ExecutorThreadFactory.INSTANCE);
	
	private final AccumulatorProtocol accumulatorProtocolProxy;

	private final Server taskManagerServer;

	private final FileCache fileCache = new FileCache();
	/**
	 * This map contains all the tasks whose threads are in a state other than TERMINATED. If any task
	 * is stored inside this map and its thread status is TERMINATED, this indicates a virtual machine error.
	 * As a result, task status will switch to FAILED and reported to the {@link org.apache.flink.runtime.jobmanager.JobManager}.
	 */
	private final Map<ExecutionVertexID, Task> runningTasks = new ConcurrentHashMap<ExecutionVertexID, Task>();

	private final InstanceConnectionInfo localInstanceConnectionInfo;

	/**
	 * The instance of the {@link ChannelManager} which is responsible for
	 * setting up and cleaning up the byte buffered channels of the tasks.
	 */
	private final ChannelManager channelManager;

	/**
	 * Instance of the task manager profile if profiling is enabled.
	 */
	private final TaskManagerProfiler profiler;

	private final MemoryManager memoryManager;

	private final IOManager ioManager;

	private final HardwareDescription hardwareDescription;

	private final int numberOfSlots;

	private final Thread heartbeatThread;
	
	private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);
	
	/** Stores whether the task manager has already been shut down. */
	private volatile boolean shutdownComplete;
	
	/**
	 * All parameters are obtained from the 
	 * {@link GlobalConfiguration}, which must be loaded prior to instantiating the task manager.
	 */
	public TaskManager(ExecutionMode executionMode) throws Exception {
		if (executionMode == null) {
			throw new NullPointerException("Execution mode must not be null.");
		}
			
		LOG.info("Execution mode: " + executionMode);

		// IMPORTANT! At this point, the GlobalConfiguration must have been read!
		
		final InetSocketAddress jobManagerAddress;
		{
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
				LOG.fatal("Could not resolve JobManager host name.");
				throw new Exception("Could not resolve JobManager host name: " + e.getMessage(), e);
			}
			
			LOG.info("Connecting to JobManager at: " + jobManagerAddress);
		}
		
		// Create RPC connection to the JobManager
		try {
			this.jobManager = RPC.getProxy(JobManagerProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
		} catch (IOException e) {
			LOG.fatal("Could not connect to the JobManager: " + e.getMessage(), e);
			throw new Exception("Failed to initialize connection to JobManager: " + e.getMessage(), e);
		}
		
		int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, -1);
		int dataPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, -1);
		if (ipcPort == -1) {
			ipcPort = getAvailablePort();
		}
		if (dataPort == -1) {
			dataPort = getAvailablePort();
		}
		
		// Determine our own public facing address and start the server
		{
			final InetAddress taskManagerAddress;
			try {
				taskManagerAddress = getTaskManagerAddress(jobManagerAddress);
			}
			catch (Exception e) {
				throw new RuntimeException("The TaskManager failed to determine its own network address.", e);
			}
			
			this.localInstanceConnectionInfo = new InstanceConnectionInfo(taskManagerAddress, ipcPort, dataPort);
			LOG.info("TaskManager connection information:" + this.localInstanceConnectionInfo);

			// Start local RPC server
			try {
				this.taskManagerServer = RPC.getServer(this, taskManagerAddress.getHostAddress(), ipcPort, IPC_HANDLER_COUNT);
				this.taskManagerServer.start();
			} catch (IOException e) {
				LOG.fatal("Failed to start TaskManager server. " + e.getMessage(), e);
				throw new Exception("Failed to start taskmanager server. " + e.getMessage(), e);
			}
		}
		
		// Try to create local stub of the global input split provider
		try {
			this.globalInputSplitProvider = RPC.getProxy(InputSplitProviderProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
		} catch (IOException e) {
			LOG.fatal(e.getMessage(), e);
			throw new Exception("Failed to initialize connection to global input split provider: " + e.getMessage(), e);
		}

		// Try to create local stub for the lookup service
		try {
			this.lookupService = RPC.getProxy(ChannelLookupProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
		} catch (IOException e) {
			LOG.fatal(e.getMessage(), e);
			throw new Exception("Failed to initialize channel lookup protocol. " + e.getMessage(), e);
		}

		// Try to create local stub for the accumulators
		try {
			this.accumulatorProtocolProxy = RPC.getProxy(AccumulatorProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
		} catch (IOException e) {
			LOG.fatal("Failed to initialize accumulator protocol: " + e.getMessage(), e);
			throw new Exception("Failed to initialize accumulator protocol: " + e.getMessage(), e);
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
					int numInThreads = GlobalConfiguration.getInteger(
							ConfigConstants.TASK_MANAGER_NET_NUM_IN_THREADS_KEY,
							ConfigConstants.DEFAULT_TASK_MANAGER_NET_NUM_IN_THREADS);

					int numOutThreads = GlobalConfiguration.getInteger(
							ConfigConstants.TASK_MANAGER_NET_NUM_OUT_THREADS_KEY,
							ConfigConstants.DEFAULT_TASK_MANAGER_NET_NUM_OUT_THREADS);

					int closeAfterIdleForMs = GlobalConfiguration.getInteger(
							ConfigConstants.TASK_MANAGER_NET_CLOSE_AFTER_IDLE_FOR_MS_KEY,
							ConfigConstants.DEFAULT_TASK_MANAGER_NET_CLOSE_AFTER_IDLE_FOR_MS);

					networkConnectionManager = new NettyConnectionManager(
							localInstanceConnectionInfo.address(), localInstanceConnectionInfo.dataPort(),
							bufferSize, numInThreads, numOutThreads, closeAfterIdleForMs);
					break;
			}

			channelManager = new ChannelManager(lookupService, localInstanceConnectionInfo, numBuffers, bufferSize, networkConnectionManager);
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
			throw new Exception("Failed to instantiate ChannelManager.", ioe);
		}

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
		
		this.hardwareDescription = HardwareDescriptionFactory.extractFromSystem();
		
		// initialize the memory manager
		{
			// Check whether the memory size has been explicitly configured.
			final long configuredMemorySize = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1);
			final long memorySize;
			
			if (configuredMemorySize == -1) {
				// no manually configured memory. take a relative fraction of the free heap space
				float fraction = GlobalConfiguration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);
				memorySize = (long) (this.hardwareDescription.getSizeOfFreeMemory() * fraction);
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
				LOG.fatal("Unable to initialize memory manager with " + (memorySize >>> 20) + " megabytes of memory.", t);
				throw new Exception("Unable to initialize memory manager.", t);
			}
		}

		this.ioManager = new IOManager(tmpDirPaths);
		
		this.heartbeatThread = new Thread() {
			@Override
			public void run() {
				runHeartbeatLoop();
			}
		};

		this.heartbeatThread.setName("Heartbeat Thread");
		this.heartbeatThread.start();

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

							LOG.debug(getMemoryUsageStatsAsString(memoryMXBean));

							LOG.debug(getGarbageCollectorStatsAsString(gcMXBeans));
						}
					} catch (InterruptedException e) {
						LOG.warn("Unexpected interruption of memory usage logger thread.");
					}
				}
			}).start();
		}
	}

	private int getAvailablePort() {
		ServerSocket serverSocket = null;
		int port = 0;
		for (int i = 0; i < 50; i++){
			try {
				serverSocket = new ServerSocket(0);
				port = serverSocket.getLocalPort();
				if (port != 0) {
					serverSocket.close();
					break;
				}
			} catch (IOException e) {
				LOG.debug("Unable to allocate port " + e.getMessage(), e);
			}
		}
		if (!serverSocket.isClosed()) {
			try {
				serverSocket.close();
			} catch (IOException e) {
				LOG.debug("error closing port",e);
			}
		}
		return port;
	}

	/**
	 * Entry point for the program.
	 * 
	 * @param args
	 *        arguments from the command line
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
			System.exit(FAILURE_RETURN_CODE);
		}

		String configDir = line.getOptionValue(configDirOpt.getOpt(), null);
		String tempDirVal = line.getOptionValue(tempDir.getOpt(), null);

		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);
		if(tempDirVal != null // the YARN TM runner has set a value for the temp dir
				// the configuration does not contain a temp direcory
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
			new TaskManager(ExecutionMode.CLUSTER);
		} catch (Exception e) {
			LOG.fatal("Taskmanager startup failed: " + e.getMessage(), e);
			System.exit(FAILURE_RETURN_CODE);
		}
		
		// park the main thread to keep the JVM alive (all other threads may be daemon threads)
		Object mon = new Object();
		synchronized (mon) {
			try {
				mon.wait();
			} catch (InterruptedException ex) {}
		}
	}

	/**
	 * This method send the periodic heartbeats.
	 */
	private void runHeartbeatLoop() {
		final long interval = GlobalConfiguration.getInteger(
						ConfigConstants.TASK_MANAGER_HEARTBEAT_INTERVAL_KEY,
						ConfigConstants.DEFAULT_TASK_MANAGER_HEARTBEAT_INTERVAL);

		try {
			while(!shutdownStarted.get()){
				RegisterTaskManagerResult result  = this.jobManager.registerTaskManager(this
								.localInstanceConnectionInfo,this.hardwareDescription,
						new IntegerRecord(this.numberOfSlots));

				if(result.getReturnCode() == RegisterTaskManagerResult.ReturnCode.SUCCESS){
					break;
				}

				try{
					Thread.sleep(50);
				}catch(InterruptedException e){
					if (!shutdownStarted.get()) {
						LOG.error("TaskManager register task manager loop was interrupted without shutdown.");
					}
				}
			}

		} catch (IOException e) {
			if(!shutdownStarted.get()){
				LOG.error("Registering task manager caused an exception: " + e.getMessage(), e);
			}
			return;
		}

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
				this.jobManager.sendHeartbeat(this.localInstanceConnectionInfo);
			} catch (IOException e) {
				if (shutdownStarted.get()) {
					break;
				} else {
					LOG.error("Sending the heart beat caused an exception: " + e.getMessage(), e);
				}
			}
		}
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
	
	/**
	 * Find out the TaskManager's own IP address.
	 */
	private InetAddress getTaskManagerAddress(InetSocketAddress jobManagerAddress) throws IOException {
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
				throw new RuntimeException("The TaskManager failed to detect its own IP address");
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Defaulting to detection strategy " + strategy);
			}
		}
	}
	
	/**
	 * Checks if two addresses have a common prefix (first 2 bytes).
	 * Example: 192.168.???.???
	 * Works also with ipv6, but accepts probably too many addresses
	 */
	private static boolean hasCommonPrefix(byte[] address, byte[] address2) {
		return address[0] == address2[0] && address[1] == address2[1];
	}

	public static boolean tryToConnect(InetAddress fromAddress, SocketAddress toSocket, int timeout) throws IOException {
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
			LOG.info("Failed to determine own IP address from '" + fromAddress + "': " + ex.getMessage());
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
	

	@Override
	public TaskCancelResult cancelTask(final ExecutionVertexID id) throws IOException {

		final Task task = this.runningTasks.get(id);

		if (task == null) {
			final TaskCancelResult taskCancelResult = new TaskCancelResult(id,
				AbstractTaskResult.ReturnCode.TASK_NOT_FOUND);
			taskCancelResult.setDescription("No task with ID " + id + " is currently running");
			return taskCancelResult;
		}

		// Pass call to executor service so IPC thread can return immediately
		final Runnable r = new Runnable() {

			@Override
			public void run() {

				// Finally, request user code to cancel
				task.cancelExecution();
			}
		};

		this.executorService.execute(r);

		return new TaskCancelResult(id, AbstractTaskResult.ReturnCode.SUCCESS);
	}

	@Override
	public TaskKillResult killTask(final ExecutionVertexID id) throws IOException {

		final Task task = this.runningTasks.get(id);

		if (task == null) {
			final TaskKillResult taskKillResult = new TaskKillResult(id,
					AbstractTaskResult.ReturnCode.TASK_NOT_FOUND);
			taskKillResult.setDescription("No task with ID + " + id + " is currently running");
			return taskKillResult;
		}

		// Pass call to executor service so IPC thread can return immediately
		final Runnable r = new Runnable() {

			@Override
			public void run() {

				// Finally, request user code to cancel
				task.killExecution();
			}
		};

		this.executorService.execute(r);

		return new TaskKillResult(id, AbstractTaskResult.ReturnCode.SUCCESS);
	}


	@Override
	public List<TaskSubmissionResult> submitTasks(final List<TaskDeploymentDescriptor> tasks) throws IOException {

		final List<TaskSubmissionResult> submissionResultList = new SerializableArrayList<TaskSubmissionResult>();
		final List<Task> tasksToStart = new ArrayList<Task>();

		// Make sure all tasks are fully registered before they are started
		for (final TaskDeploymentDescriptor tdd : tasks) {

			final JobID jobID = tdd.getJobID();
			final ExecutionVertexID vertexID = tdd.getVertexID();
			RuntimeEnvironment re;

			// retrieve the registered cache files from job configuration and create the local tmp file.
			Map<String, FutureTask<Path>> cpTasks = new HashMap<String, FutureTask<Path>>();
			for (Entry<String, DistributedCacheEntry> e : DistributedCache.readFileInfoFromConfig(tdd.getJobConfiguration())) {
				FutureTask<Path> cp = this.fileCache.createTmpFile(e.getKey(), e.getValue(), jobID);
				cpTasks.put(e.getKey(), cp);
			}

			try {
				re = new RuntimeEnvironment(tdd, this.memoryManager, this.ioManager, new TaskInputSplitProvider(jobID,
					vertexID, this.globalInputSplitProvider), this.accumulatorProtocolProxy, cpTasks);
			} catch (Throwable t) {
				final TaskSubmissionResult result = new TaskSubmissionResult(vertexID,
					AbstractTaskResult.ReturnCode.DEPLOYMENT_ERROR);
				result.setDescription(StringUtils.stringifyException(t));
				LOG.error(result.getDescription(), t);
				submissionResultList.add(result);
				continue;
			}

			final Configuration jobConfiguration = tdd.getJobConfiguration();

			// Register the task
			Task task;
			try {
				task = createAndRegisterTask(vertexID, jobConfiguration, re);
			} catch (InsufficientResourcesException e) {
				final TaskSubmissionResult result = new TaskSubmissionResult(vertexID,
					AbstractTaskResult.ReturnCode.INSUFFICIENT_RESOURCES);
				result.setDescription(e.getMessage());
				LOG.error(result.getDescription(), e);
				submissionResultList.add(result);
				continue;
			}

			if (task == null) {
				final TaskSubmissionResult result = new TaskSubmissionResult(vertexID,
					AbstractTaskResult.ReturnCode.TASK_NOT_FOUND);
				result.setDescription("Task " + re.getTaskNameWithIndex() + " (" + vertexID + ") was already running");
				LOG.error(result.getDescription());
				submissionResultList.add(result);
				continue;
			}

			submissionResultList.add(new TaskSubmissionResult(vertexID, AbstractTaskResult.ReturnCode.SUCCESS));
			tasksToStart.add(task);
		}

		// Now start the tasks
		for (final Task task : tasksToStart) {
			task.startExecution();
		}

		return submissionResultList;
	}

	/**
	 * Registers an newly incoming runtime task with the task manager.
	 * 
	 * @param id
	 *        the ID of the task to register
	 * @param jobConfiguration
	 *        the job configuration that has been attached to the original job graph
	 * @param environment
	 *        the environment of the task to be registered
	 * @return the task to be started or <code>null</code> if a task with the same ID was already running
	 */
	private Task createAndRegisterTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final RuntimeEnvironment environment)
					throws InsufficientResourcesException, IOException {

		if (id == null) {
			throw new IllegalArgumentException("Argument id is null");
		}

		if (environment == null) {
			throw new IllegalArgumentException("Argument environment is null");
		}

		// Task creation and registration must be atomic
		Task task;

		synchronized (this) {
			final Task runningTask = this.runningTasks.get(id);
			boolean registerTask = true;
			if (runningTask == null) {
				task = new Task(id, environment, this);
			} else {

				if (runningTask instanceof Task) {
					// Task is already running
					return null;
				} else {
					// There is already a replay task running, we will simply restart it
					task = runningTask;
					registerTask = false;
				}

			}

			if (registerTask) {
				// Register the task with the byte buffered channel manager
				this.channelManager.register(task);

				boolean enableProfiling = false;
				if (this.profiler != null && jobConfiguration.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
					enableProfiling = true;
				}

				// Register environment, input, and output gates for profiling
				if (enableProfiling) {
					task.registerProfiler(this.profiler, jobConfiguration);
				}

				this.runningTasks.put(id, task);
			}
		}
		return task;
	}

	/**
	 * Unregisters a finished or aborted task.
	 * 
	 * @param id
	 *        the ID of the task to be unregistered
	 */
	private void unregisterTask(final ExecutionVertexID id) {

		// Task de-registration must be atomic
		synchronized (this) {

			final Task task = this.runningTasks.remove(id);
			if (task == null) {
				LOG.error("Cannot find task with ID " + id + " to unregister");
				return;
			}

			// remove the local tmp file for unregistered tasks.
			for (Entry<String, DistributedCacheEntry> e: DistributedCache.readFileInfoFromConfig(task.getEnvironment().getJobConfiguration())) {
				this.fileCache.deleteTmpFile(e.getKey(), e.getValue(), task.getJobID());
			}
			// Unregister task from the byte buffered channel manager
			this.channelManager.unregister(id, task);

			// Unregister task from profiling
			task.unregisterProfiler(this.profiler);

			// Unregister task from memory manager
			task.unregisterMemoryManager(this.memoryManager);

			// Unregister task from library cache manager
			try {
				LibraryCacheManager.unregister(task.getJobID());
			} catch (IOException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Unregistering the job vertex ID " + id + " caused an IOException");
				}
			}
		}
	}


	@Override
	public LibraryCacheProfileResponse getLibraryCacheProfile(LibraryCacheProfileRequest request) throws IOException {

		LibraryCacheProfileResponse response = new LibraryCacheProfileResponse(request);
		String[] requiredLibraries = request.getRequiredLibraries();

		for (int i = 0; i < requiredLibraries.length; i++) {
			if (LibraryCacheManager.contains(requiredLibraries[i]) == null) {
				response.setCached(i, false);
			} else {
				response.setCached(i, true);
			}
		}

		return response;
	}


	@Override
	public void updateLibraryCache(LibraryCacheUpdate update) throws IOException {
		// Nothing to to here
	}

	public void executionStateChanged(final JobID jobID, final ExecutionVertexID id,
			final ExecutionState newExecutionState, final String optionalDescription) {

		// Don't propagate state CANCELING back to the job manager
		if (newExecutionState == ExecutionState.CANCELING) {
			return;
		}

		if (newExecutionState == ExecutionState.FINISHED || newExecutionState == ExecutionState.CANCELED
				|| newExecutionState == ExecutionState.FAILED) {

			// Unregister the task (free all buffers, remove all channels, task-specific class loaders, etc...)
			unregisterTask(id);
		}
		// Get lock on the jobManager object and propagate the state change
		synchronized (this.jobManager) {
			try {
				this.jobManager.updateTaskExecutionState(new TaskExecutionState(jobID, id, newExecutionState,
					optionalDescription));
			} catch (IOException e) {
				LOG.error(e);
			}
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
		
		// first, stop the heartbeat thread and wait for it to terminate
		this.heartbeatThread.interrupt();
		try {
			this.heartbeatThread.join(1000);
		} catch (InterruptedException e) {}

		// Stop RPC proxy for the task manager
		RPC.stopProxy(this.jobManager);

		// Stop RPC proxy for the global input split assigner
		RPC.stopProxy(this.globalInputSplitProvider);

		// Stop RPC proxy for the lookup service
		RPC.stopProxy(this.lookupService);

		// Stop RPC proxy for accumulator reports
		RPC.stopProxy(this.accumulatorProtocolProxy);

		// Shut down the own RPC server
		this.taskManagerServer.stop();

		// Stop profiling if enabled
		if (this.profiler != null) {
			this.profiler.shutdown();
		}

		// Shut down the channel manager
		try {
			this.channelManager.shutdown();
		} catch (IOException e) {
			LOG.warn("ChannelManager did not shutdown properly: " + e.getMessage(), e);
		}

		// Shut down the memory manager
		if (this.ioManager != null) {
			this.ioManager.shutdown();
		}

		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
		}

		this.fileCache.shutdown();

		// Shut down the executor service
		if (this.executorService != null) {
			this.executorService.shutdown();
			try {
				this.executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(e);
				}
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

	@Override
	public void logBufferUtilization() {

		this.channelManager.logBufferUtilization();
	}

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


	@Override
	public void invalidateLookupCacheEntries(final Set<ChannelID> channelIDs) throws IOException {
		this.channelManager.invalidateLookupCacheEntries(channelIDs);
	}

	/**
	 * Checks, whether the given strings describe existing directories that are writable. If that is not
	 * the case, an exception is raised.
	 * 
	 * @param tempDirs
	 *        An array of strings which are checked to be paths to writable directories.
	 * @throws Exception
	 *         Thrown, if any of the mentioned checks fails.
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
}
