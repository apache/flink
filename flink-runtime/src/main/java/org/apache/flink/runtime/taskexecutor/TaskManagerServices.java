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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.runtime.query.QueryableStateUtils;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Container for {@link TaskExecutor} services such as the {@link MemoryManager}, {@link IOManager},
 * {@link NetworkEnvironment}. All services are exclusive to a single {@link TaskExecutor}.
 * Consequently, the respective {@link TaskExecutor} is responsible for closing them.
 */
public class TaskManagerServices {
	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerServices.class);

	@VisibleForTesting
	public static final String LOCAL_STATE_SUB_DIRECTORY_ROOT = "localState";

	/** TaskManager services. */
	private final TaskManagerLocation taskManagerLocation;
	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final NetworkEnvironment networkEnvironment;
	private final BroadcastVariableManager broadcastVariableManager;
	private final TaskSlotTable taskSlotTable;
	private final JobManagerTable jobManagerTable;
	private final JobLeaderService jobLeaderService;
	private final TaskExecutorLocalStateStoresManager taskManagerStateStore;

	TaskManagerServices(
		TaskManagerLocation taskManagerLocation,
		MemoryManager memoryManager,
		IOManager ioManager,
		NetworkEnvironment networkEnvironment,
		BroadcastVariableManager broadcastVariableManager,
		TaskSlotTable taskSlotTable,
		JobManagerTable jobManagerTable,
		JobLeaderService jobLeaderService,
		TaskExecutorLocalStateStoresManager taskManagerStateStore) {

		this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
		this.memoryManager = Preconditions.checkNotNull(memoryManager);
		this.ioManager = Preconditions.checkNotNull(ioManager);
		this.networkEnvironment = Preconditions.checkNotNull(networkEnvironment);
		this.broadcastVariableManager = Preconditions.checkNotNull(broadcastVariableManager);
		this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
		this.jobManagerTable = Preconditions.checkNotNull(jobManagerTable);
		this.jobLeaderService = Preconditions.checkNotNull(jobLeaderService);
		this.taskManagerStateStore = Preconditions.checkNotNull(taskManagerStateStore);
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public MemoryManager getMemoryManager() {
		return memoryManager;
	}

	public IOManager getIOManager() {
		return ioManager;
	}

	public NetworkEnvironment getNetworkEnvironment() {
		return networkEnvironment;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	public BroadcastVariableManager getBroadcastVariableManager() {
		return broadcastVariableManager;
	}

	public TaskSlotTable getTaskSlotTable() {
		return taskSlotTable;
	}

	public JobManagerTable getJobManagerTable() {
		return jobManagerTable;
	}

	public JobLeaderService getJobLeaderService() {
		return jobLeaderService;
	}

	public TaskExecutorLocalStateStoresManager getTaskManagerStateStore() {
		return taskManagerStateStore;
	}

	// --------------------------------------------------------------------------------------------
	//  Shut down method
	// --------------------------------------------------------------------------------------------

	/**
	 * Shuts the {@link TaskExecutor} services down.
	 */
	public void shutDown() throws FlinkException {

		Exception exception = null;

		try {
			taskManagerStateStore.shutdown();
		} catch (Exception e) {
			exception = e;
		}

		try {
			memoryManager.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			ioManager.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			networkEnvironment.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			taskSlotTable.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			jobLeaderService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the TaskManager services.", exception);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Static factory methods for task manager services
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates and returns the task manager services.
	 *
	 * @param resourceID resource ID of the task manager
	 * @param taskManagerServicesConfiguration task manager configuration
	 * @param taskIOExecutor executor for async IO operations.
	 * @param freeHeapMemoryWithDefrag an estimate of the size of the free heap memory
	 * @param maxJvmHeapMemory the maximum JVM heap size
	 * @return task manager components
	 * @throws Exception
	 */
	public static TaskManagerServices fromConfiguration(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration,
			ResourceID resourceID,
			Executor taskIOExecutor,
			long freeHeapMemoryWithDefrag,
			long maxJvmHeapMemory) throws Exception {

		// pre-start checks
		checkTempDirs(taskManagerServicesConfiguration.getTmpDirPaths());

		final NetworkEnvironment network = createNetworkEnvironment(taskManagerServicesConfiguration, maxJvmHeapMemory);
		network.start();

		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(
			resourceID,
			taskManagerServicesConfiguration.getTaskManagerAddress(),
			network.getConnectionManager().getDataPort());

		// this call has to happen strictly after the network stack has been initialized
		final MemoryManager memoryManager = createMemoryManager(taskManagerServicesConfiguration, freeHeapMemoryWithDefrag, maxJvmHeapMemory);

		// start the I/O manager, it will create some temp directories.
		final IOManager ioManager = new IOManagerAsync(taskManagerServicesConfiguration.getTmpDirPaths());

		final BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();

		final List<ResourceProfile> resourceProfiles = new ArrayList<>(taskManagerServicesConfiguration.getNumberOfSlots());

		for (int i = 0; i < taskManagerServicesConfiguration.getNumberOfSlots(); i++) {
			resourceProfiles.add(new ResourceProfile(1.0, 42));
		}

		final TimerService<AllocationID> timerService = new TimerService<>(
			new ScheduledThreadPoolExecutor(1),
			taskManagerServicesConfiguration.getTimerServiceShutdownTimeout());

		final TaskSlotTable taskSlotTable = new TaskSlotTable(resourceProfiles, timerService);

		final JobManagerTable jobManagerTable = new JobManagerTable();

		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);

		LocalRecoveryConfig.LocalRecoveryMode localRecoveryMode = taskManagerServicesConfiguration.getLocalRecoveryMode();

		final String[] stateRootDirectoryStrings = taskManagerServicesConfiguration.getLocalRecoveryStateRootDirectories();

		final File[] stateRootDirectoryFiles = new File[stateRootDirectoryStrings.length];

		for (int i = 0; i < stateRootDirectoryStrings.length; ++i) {
			stateRootDirectoryFiles[i] = new File(stateRootDirectoryStrings[i], LOCAL_STATE_SUB_DIRECTORY_ROOT);
		}

		final TaskExecutorLocalStateStoresManager taskStateManager =
			new TaskExecutorLocalStateStoresManager(localRecoveryMode, stateRootDirectoryFiles, taskIOExecutor);

		return new TaskManagerServices(
			taskManagerLocation,
			memoryManager,
			ioManager,
			network,
			broadcastVariableManager,
			taskSlotTable,
			jobManagerTable,
			jobLeaderService,
			taskStateManager);
	}

	/**
	 * Creates a {@link MemoryManager} from the given {@link TaskManagerServicesConfiguration}.
	 *
	 * @param taskManagerServicesConfiguration to create the memory manager from
	 * @param freeHeapMemoryWithDefrag an estimate of the size of the free heap memory
	 * @param maxJvmHeapMemory the maximum JVM heap size
	 * @return Memory manager
	 * @throws Exception
	 */
	private static MemoryManager createMemoryManager(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration,
			long freeHeapMemoryWithDefrag,
			long maxJvmHeapMemory) throws Exception {
		// computing the amount of memory to use depends on how much memory is available
		// it strictly needs to happen AFTER the network stack has been initialized

		// check if a value has been configured
		long configuredMemory = taskManagerServicesConfiguration.getConfiguredMemory();

		MemoryType memType = taskManagerServicesConfiguration.getMemoryType();

		final long memorySize;

		boolean preAllocateMemory = taskManagerServicesConfiguration.isPreAllocateMemory();

		if (configuredMemory > 0) {
			if (preAllocateMemory) {
				LOG.info("Using {} MB for managed memory." , configuredMemory);
			} else {
				LOG.info("Limiting managed memory to {} MB, memory will be allocated lazily." , configuredMemory);
			}
			memorySize = configuredMemory << 20; // megabytes to bytes
		} else {
			// similar to #calculateNetworkBufferMemory(TaskManagerServicesConfiguration tmConfig)
			float memoryFraction = taskManagerServicesConfiguration.getMemoryFraction();

			if (memType == MemoryType.HEAP) {
				// network buffers allocated off-heap -> use memoryFraction of the available heap:
				long relativeMemSize = (long) (freeHeapMemoryWithDefrag * memoryFraction);
				if (preAllocateMemory) {
					LOG.info("Using {} of the currently free heap space for managed heap memory ({} MB)." ,
						memoryFraction , relativeMemSize >> 20);
				} else {
					LOG.info("Limiting managed memory to {} of the currently free heap space ({} MB), " +
						"memory will be allocated lazily." , memoryFraction , relativeMemSize >> 20);
				}
				memorySize = relativeMemSize;
			} else if (memType == MemoryType.OFF_HEAP) {
				// The maximum heap memory has been adjusted according to the fraction (see
				// calculateHeapSizeMB(long totalJavaMemorySizeMB, Configuration config)), i.e.
				// maxJvmHeap = jvmTotalNoNet - jvmTotalNoNet * memoryFraction = jvmTotalNoNet * (1 - memoryFraction)
				// directMemorySize = jvmTotalNoNet * memoryFraction
				long directMemorySize = (long) (maxJvmHeapMemory / (1.0 - memoryFraction) * memoryFraction);
				if (preAllocateMemory) {
					LOG.info("Using {} of the maximum memory size for managed off-heap memory ({} MB)." ,
						memoryFraction, directMemorySize >> 20);
				} else {
					LOG.info("Limiting managed memory to {} of the maximum memory size ({} MB)," +
						" memory will be allocated lazily.", memoryFraction, directMemorySize >> 20);
				}
				memorySize = directMemorySize;
			} else {
				throw new RuntimeException("No supported memory type detected.");
			}
		}

		// now start the memory manager
		final MemoryManager memoryManager;
		try {
			memoryManager = new MemoryManager(
				memorySize,
				taskManagerServicesConfiguration.getNumberOfSlots(),
				taskManagerServicesConfiguration.getNetworkConfig().networkBufferSize(),
				memType,
				preAllocateMemory);
		} catch (OutOfMemoryError e) {
			if (memType == MemoryType.HEAP) {
				throw new Exception("OutOfMemory error (" + e.getMessage() +
					") while allocating the TaskManager heap memory (" + memorySize + " bytes).", e);
			} else if (memType == MemoryType.OFF_HEAP) {
				throw new Exception("OutOfMemory error (" + e.getMessage() +
					") while allocating the TaskManager off-heap memory (" + memorySize +
					" bytes).Try increasing the maximum direct memory (-XX:MaxDirectMemorySize)", e);
			} else {
				throw e;
			}
		}
		return memoryManager;
	}

	/**
	 * Creates the {@link NetworkEnvironment} from the given {@link TaskManagerServicesConfiguration}.
	 *
	 * @param taskManagerServicesConfiguration to construct the network environment from
	 * @param maxJvmHeapMemory the maximum JVM heap size
	 * @return Network environment
	 * @throws IOException
	 */
	private static NetworkEnvironment createNetworkEnvironment(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration,
			long maxJvmHeapMemory) {

		NetworkEnvironmentConfiguration networkEnvironmentConfiguration = taskManagerServicesConfiguration.getNetworkConfig();

		final long networkBuf = calculateNetworkBufferMemory(taskManagerServicesConfiguration, maxJvmHeapMemory);
		int segmentSize = networkEnvironmentConfiguration.networkBufferSize();

		// tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
		final long numNetBuffersLong = networkBuf / segmentSize;
		if (numNetBuffersLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("The given number of memory bytes (" + networkBuf
				+ ") corresponds to more than MAX_INT pages.");
		}

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(
			(int) numNetBuffersLong,
			segmentSize);

		ConnectionManager connectionManager;
		boolean enableCreditBased = false;
		NettyConfig nettyConfig = networkEnvironmentConfiguration.nettyConfig();
		if (nettyConfig != null) {
			connectionManager = new NettyConnectionManager(nettyConfig);
			enableCreditBased = nettyConfig.isCreditBasedEnabled();
		} else {
			connectionManager = new LocalConnectionManager();
		}

		ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
		TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

		KvStateRegistry kvStateRegistry = new KvStateRegistry();

		QueryableStateConfiguration qsConfig = taskManagerServicesConfiguration.getQueryableStateConfig();

		int numProxyServerNetworkThreads = qsConfig.numProxyServerThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numProxyServerThreads();

		int numProxyServerQueryThreads = qsConfig.numProxyQueryThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numProxyQueryThreads();

		final KvStateClientProxy kvClientProxy = QueryableStateUtils.createKvStateClientProxy(
				taskManagerServicesConfiguration.getTaskManagerAddress(),
				qsConfig.getProxyPortRange(),
				numProxyServerNetworkThreads,
				numProxyServerQueryThreads,
				new DisabledKvStateRequestStats());

		int numStateServerNetworkThreads = qsConfig.numStateServerThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numStateServerThreads();

		int numStateServerQueryThreads = qsConfig.numStateQueryThreads() == 0 ?
				taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numStateQueryThreads();

		final KvStateServer kvStateServer = QueryableStateUtils.createKvStateServer(
				taskManagerServicesConfiguration.getTaskManagerAddress(),
				qsConfig.getStateServerPortRange(),
				numStateServerNetworkThreads,
				numStateServerQueryThreads,
				kvStateRegistry,
				new DisabledKvStateRequestStats());

		// we start the network first, to make sure it can allocate its buffers first
		return new NetworkEnvironment(
			networkBufferPool,
			connectionManager,
			resultPartitionManager,
			taskEventDispatcher,
			kvStateRegistry,
			kvStateServer,
			kvClientProxy,
			networkEnvironmentConfiguration.ioMode(),
			networkEnvironmentConfiguration.partitionRequestInitialBackoff(),
			networkEnvironmentConfiguration.partitionRequestMaxBackoff(),
			networkEnvironmentConfiguration.networkBuffersPerChannel(),
			networkEnvironmentConfiguration.floatingNetworkBuffersPerGate(),
			enableCreditBased);
	}

	/**
	 * Calculates the amount of memory used for network buffers based on the total memory to use and
	 * the according configuration parameters.
	 *
	 * <p>The following configuration parameters are involved:
	 * <ul>
	 *  <li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}, and</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_NUM_BUFFERS} (fallback if the ones above do not exist)</li>
	 * </ul>.
	 *
	 * @param totalJavaMemorySize
	 * 		overall available memory to use (heap and off-heap, in bytes)
	 * @param config
	 * 		configuration object
	 *
	 * @return memory to use for network buffers (in bytes); at least one memory segment
	 */
	@SuppressWarnings("deprecation")
	public static long calculateNetworkBufferMemory(long totalJavaMemorySize, Configuration config) {
		Preconditions.checkArgument(totalJavaMemorySize > 0);

		int segmentSize = config.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);

		final long networkBufBytes;
		if (TaskManagerServicesConfiguration.hasNewNetworkBufConf(config)) {
			// new configuration based on fractions of available memory with selectable min and max
			float networkBufFraction = config.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
			long networkBufMin = config.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN);
			long networkBufMax = config.getLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX);

			TaskManagerServicesConfiguration
				.checkNetworkBufferConfig(segmentSize, networkBufFraction, networkBufMin, networkBufMax);

			networkBufBytes = Math.min(networkBufMax, Math.max(networkBufMin,
				(long) (networkBufFraction * totalJavaMemorySize)));

			TaskManagerServicesConfiguration
				.checkConfigParameter(networkBufBytes < totalJavaMemorySize,
					"(" + networkBufFraction + ", " + networkBufMin + ", " + networkBufMax + ")",
					"(" + TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key() + ", " +
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key() + ", " +
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ")",
					"Network buffer memory size too large: " + networkBufBytes + " >= " +
						totalJavaMemorySize + " (total JVM memory size)");
			TaskManagerServicesConfiguration
				.checkConfigParameter(networkBufBytes >= segmentSize,
					"(" + networkBufFraction + ", " + networkBufMin + ", " + networkBufMax + ")",
					"(" + TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key() + ", " +
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key() + ", " +
						TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ")",
					"Network buffer memory size too small: " + networkBufBytes + " < " +
						segmentSize + " (" + TaskManagerOptions.MEMORY_SEGMENT_SIZE.key() + ")");
		} else {
			// use old (deprecated) network buffers parameter
			int numNetworkBuffers = config.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS);
			networkBufBytes = (long) numNetworkBuffers * (long) segmentSize;

			TaskManagerServicesConfiguration.checkNetworkConfigOld(numNetworkBuffers);

			TaskManagerServicesConfiguration
				.checkConfigParameter(networkBufBytes < totalJavaMemorySize,
					networkBufBytes, TaskManagerOptions.NETWORK_NUM_BUFFERS.key(),
					"Network buffer memory size too large: " + networkBufBytes + " >= " +
						totalJavaMemorySize + " (total JVM memory size)");
			TaskManagerServicesConfiguration
				.checkConfigParameter(networkBufBytes >= segmentSize,
					networkBufBytes, TaskManagerOptions.NETWORK_NUM_BUFFERS.key(),
					"Network buffer memory size too small: " + networkBufBytes + " < " +
						segmentSize + " (" + TaskManagerOptions.MEMORY_SEGMENT_SIZE.key() + ")");
		}

		return networkBufBytes;
	}

	/**
	 * Calculates the amount of memory used for network buffers inside the current JVM instance
	 * based on the available heap or the max heap size and the according configuration parameters.
	 *
	 * <p>For containers or when started via scripts, if started with a memory limit and set to use
	 * off-heap memory, the maximum heap size for the JVM is adjusted accordingly and we are able
	 * to extract the intended values from this.
	 *
	 * <p>The following configuration parameters are involved:
	 * <ul>
	 *  <li>{@link TaskManagerOptions#MANAGED_MEMORY_FRACTION},</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN},</li>
	 * 	<li>{@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}, and</li>
	 *  <li>{@link TaskManagerOptions#NETWORK_NUM_BUFFERS} (fallback if the ones above do not exist)</li>
	 * </ul>.
	 *
	 * @param tmConfig task manager services configuration object
	 * @param maxJvmHeapMemory the maximum JVM heap size
	 *
	 * @return memory to use for network buffers (in bytes)
	 */
	public static long calculateNetworkBufferMemory(TaskManagerServicesConfiguration tmConfig, long maxJvmHeapMemory) {
		final NetworkEnvironmentConfiguration networkConfig = tmConfig.getNetworkConfig();

		final float networkBufFraction = networkConfig.networkBufFraction();
		final long networkBufMin = networkConfig.networkBufMin();
		final long networkBufMax = networkConfig.networkBufMax();

		if (networkBufMin == networkBufMax) {
			// fixed network buffer pool size
			return networkBufMin;
		}

		// relative network buffer pool size using the fraction...

		// The maximum heap memory has been adjusted as in
		// calculateHeapSizeMB(long totalJavaMemorySizeMB, Configuration config))
		// and we need to invert these calculations.

		final MemoryType memType = tmConfig.getMemoryType();

		final long jvmHeapNoNet;
		if (memType == MemoryType.HEAP) {
			jvmHeapNoNet = maxJvmHeapMemory;
		} else if (memType == MemoryType.OFF_HEAP) {

			// check if a value has been configured
			long configuredMemory = tmConfig.getConfiguredMemory() << 20; // megabytes to bytes

			if (configuredMemory > 0) {
				// The maximum heap memory has been adjusted according to configuredMemory, i.e.
				// maxJvmHeap = jvmHeapNoNet - configuredMemory

				jvmHeapNoNet = maxJvmHeapMemory + configuredMemory;
			} else {
				// The maximum heap memory has been adjusted according to the fraction, i.e.
				// maxJvmHeap = jvmHeapNoNet - jvmHeapNoNet * managedFraction = jvmHeapNoNet * (1 - managedFraction)

				final float managedFraction = tmConfig.getMemoryFraction();
				jvmHeapNoNet = (long) (maxJvmHeapMemory / (1.0 - managedFraction));
			}
		} else {
			throw new RuntimeException("No supported memory type detected.");
		}

		// finally extract the network buffer memory size again from:
		// jvmHeapNoNet = jvmHeap - networkBufBytes
		//              = jvmHeap - Math.min(networkBufMax, Math.max(networkBufMin, jvmHeap * netFraction)
		final long networkBufBytes = Math.min(networkBufMax, Math.max(networkBufMin,
			(long) (jvmHeapNoNet / (1.0 - networkBufFraction) * networkBufFraction)));

		TaskManagerServicesConfiguration
			.checkConfigParameter(networkBufBytes < maxJvmHeapMemory,
				"(" + networkBufFraction + ", " + networkBufMin + ", " + networkBufMax + ")",
				"(" + TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key() + ", " +
					TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN.key() + ", " +
					TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX.key() + ")",
				"Network buffer memory size too large: " + networkBufBytes + " >= " +
					maxJvmHeapMemory + "(maximum JVM heap size)");

		return networkBufBytes;
	}

	/**
	 * Calculates the amount of heap memory to use (to set via <tt>-Xmx</tt> and <tt>-Xms</tt>)
	 * based on the total memory to use and the given configuration parameters.
	 *
	 * @param totalJavaMemorySizeMB
	 * 		overall available memory to use (heap and off-heap)
	 * @param config
	 * 		configuration object
	 *
	 * @return heap memory to use (in megabytes)
	 */
	public static long calculateHeapSizeMB(long totalJavaMemorySizeMB, Configuration config) {
		Preconditions.checkArgument(totalJavaMemorySizeMB > 0);

		// subtract the Java memory used for network buffers (always off-heap)
		final long networkBufMB =
			calculateNetworkBufferMemory(
				totalJavaMemorySizeMB << 20, // megabytes to bytes
				config) >> 20; // bytes to megabytes
		final long remainingJavaMemorySizeMB = totalJavaMemorySizeMB - networkBufMB;

		// split the available Java memory between heap and off-heap

		final boolean useOffHeap = config.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP);

		final long heapSizeMB;
		if (useOffHeap) {

			long offHeapSize = config.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);

			if (offHeapSize <= 0) {
				// calculate off-heap section via fraction
				double fraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
				offHeapSize = (long) (fraction * remainingJavaMemorySizeMB);
			}

			TaskManagerServicesConfiguration
				.checkConfigParameter(offHeapSize < remainingJavaMemorySizeMB, offHeapSize,
					TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
					"Managed memory size too large for " + networkBufMB +
						" MB network buffer memory and a total of " + totalJavaMemorySizeMB +
						" MB JVM memory");

			heapSizeMB = remainingJavaMemorySizeMB - offHeapSize;
		} else {
			heapSizeMB = remainingJavaMemorySizeMB;
		}

		return heapSizeMB;
	}

	/**
	 * Validates that all the directories denoted by the strings do actually exist or can be created, are proper
	 * directories (not files), and are writable.
	 *
	 * @param tmpDirs The array of directory paths to check.
	 * @throws IOException Thrown if any of the directories does not exist and cannot be created or is not writable
	 *                     or is a file, rather than a directory.
	 */
	private static void checkTempDirs(String[] tmpDirs) throws IOException {
		for (String dir : tmpDirs) {
			if (dir != null && !dir.equals("")) {
				File file = new File(dir);
				if (!file.exists()) {
					if (!file.mkdirs()) {
						throw new IOException("Temporary file directory " + file.getAbsolutePath() + " does not exist and could not be created.");
					}
				}
				if (!file.isDirectory()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not a directory.");
				}
				if (!file.canWrite()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not writable.");
				}

				if (LOG.isInfoEnabled()) {
					long totalSpaceGb = file.getTotalSpace() >> 30;
					long usableSpaceGb = file.getUsableSpace() >> 30;
					double usablePercentage = (double) usableSpaceGb / totalSpaceGb * 100;
					String path = file.getAbsolutePath();
					LOG.info(String.format("Temporary file directory '%s': total %d GB, " + "usable %d GB (%.2f%% usable)",
						path, totalSpaceGb, usableSpaceGb, usablePercentage));
				}
			} else {
				throw new IllegalArgumentException("Temporary file directory #$id is null.");
			}
		}
	}
}
