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

import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.netty.DisabledKvStateRequestStats;
import org.apache.flink.runtime.query.netty.KvStateServer;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskexecutor.utils.TaskExecutorMetricsInitializer;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Container for {@link TaskExecutor} services such as the {@link MemoryManager}, {@link IOManager},
 * {@link NetworkEnvironment} and the {@link MetricRegistry}.
 */
public class TaskManagerServices {
	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerServices.class);

	/** TaskManager services */
	private final TaskManagerLocation taskManagerLocation;
	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final NetworkEnvironment networkEnvironment;
	private final MetricRegistry metricRegistry;
	private final TaskManagerMetricGroup taskManagerMetricGroup;
	private final BroadcastVariableManager broadcastVariableManager;
	private final FileCache fileCache;
	private final TaskSlotTable taskSlotTable;
	private final JobManagerTable jobManagerTable;
	private final JobLeaderService jobLeaderService;

	private TaskManagerServices(
		TaskManagerLocation taskManagerLocation,
		MemoryManager memoryManager,
		IOManager ioManager,
		NetworkEnvironment networkEnvironment,
		MetricRegistry metricRegistry,
		TaskManagerMetricGroup taskManagerMetricGroup,
		BroadcastVariableManager broadcastVariableManager,
		FileCache fileCache,
		TaskSlotTable taskSlotTable,
		JobManagerTable jobManagerTable,
		JobLeaderService jobLeaderService) {

		this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
		this.memoryManager = Preconditions.checkNotNull(memoryManager);
		this.ioManager = Preconditions.checkNotNull(ioManager);
		this.networkEnvironment = Preconditions.checkNotNull(networkEnvironment);
		this.metricRegistry = Preconditions.checkNotNull(metricRegistry);
		this.taskManagerMetricGroup = Preconditions.checkNotNull(taskManagerMetricGroup);
		this.broadcastVariableManager = Preconditions.checkNotNull(broadcastVariableManager);
		this.fileCache = Preconditions.checkNotNull(fileCache);
		this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
		this.jobManagerTable = Preconditions.checkNotNull(jobManagerTable);
		this.jobLeaderService = Preconditions.checkNotNull(jobLeaderService);
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

	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	public TaskManagerMetricGroup getTaskManagerMetricGroup() {
		return taskManagerMetricGroup;
	}

	public BroadcastVariableManager getBroadcastVariableManager() {
		return broadcastVariableManager;
	}

	public FileCache getFileCache() {
		return fileCache;
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

	// --------------------------------------------------------------------------------------------
	//  Static factory methods for task manager services
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates and returns the task manager services.
	 *
	 * @param resourceID resource ID of the task manager
	 * @param taskManagerServicesConfiguration task manager configuration
	 * @return task manager components
	 * @throws Exception
	 */
	public static TaskManagerServices fromConfiguration(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration,
			ResourceID resourceID) throws Exception {

		// pre-start checks
		checkTempDirs(taskManagerServicesConfiguration.getTmpDirPaths());

		final NetworkEnvironment network = createNetworkEnvironment(taskManagerServicesConfiguration);
		network.start();

		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(
			resourceID,
			taskManagerServicesConfiguration.getTaskManagerAddress(),
			network.getConnectionManager().getDataPort());

		// this call has to happen strictly after the network stack has been initialized
		final MemoryManager memoryManager = createMemoryManager(taskManagerServicesConfiguration);

		// start the I/O manager, it will create some temp directories.
		final IOManager ioManager = new IOManagerAsync(taskManagerServicesConfiguration.getTmpDirPaths());

		final MetricRegistry metricRegistry = new MetricRegistry(
				taskManagerServicesConfiguration.getMetricRegistryConfiguration());

		final TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(
			metricRegistry,
			taskManagerLocation.getHostname(),
			taskManagerLocation.getResourceID().toString());

		// Initialize the TM metrics
		TaskExecutorMetricsInitializer.instantiateStatusMetrics(taskManagerMetricGroup, network);

		final BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();

		final FileCache fileCache = new FileCache(taskManagerServicesConfiguration.getTmpDirPaths());

		final List<ResourceProfile> resourceProfiles = new ArrayList<>(taskManagerServicesConfiguration.getNumberOfSlots());

		for (int i = 0; i < taskManagerServicesConfiguration.getNumberOfSlots(); i++) {
			resourceProfiles.add(new ResourceProfile(1.0, 42L));
		}

		final TimerService<AllocationID> timerService = new TimerService<>(
			new ScheduledThreadPoolExecutor(1),
			taskManagerServicesConfiguration.getTimerServiceShutdownTimeout());

		final TaskSlotTable taskSlotTable = new TaskSlotTable(resourceProfiles, timerService);

		final JobManagerTable jobManagerTable = new JobManagerTable();

		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation);
		
		return new TaskManagerServices(
			taskManagerLocation,
			memoryManager,
			ioManager,
			network,
			metricRegistry,
			taskManagerMetricGroup,
			broadcastVariableManager,
			fileCache,
			taskSlotTable,
			jobManagerTable,
			jobLeaderService);
	}

	/**
	 * Creates a {@link MemoryManager} from the given {@link TaskManagerServicesConfiguration}.
	 *
	 * @param taskManagerServicesConfiguration to create the memory manager from
	 * @return Memory manager
	 * @throws Exception
	 */
	private static MemoryManager createMemoryManager(TaskManagerServicesConfiguration taskManagerServicesConfiguration) throws Exception {
		// computing the amount of memory to use depends on how much memory is available
		// it strictly needs to happen AFTER the network stack has been initialized

		MemoryType memType = taskManagerServicesConfiguration.getNetworkConfig().memoryType();

		// check if a value has been configured
		long configuredMemory = taskManagerServicesConfiguration.getConfiguredMemory();

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
			float memoryFraction = taskManagerServicesConfiguration.getMemoryFraction();

			if (memType == MemoryType.HEAP) {
				long relativeMemSize = (long) (EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag() * memoryFraction);
				if (preAllocateMemory) {
					LOG.info("Using {} of the currently free heap space for managed heap memory ({} MB)." ,
						memoryFraction , relativeMemSize >> 20);
				} else {
					LOG.info("Limiting managed memory to {} of the currently free heap space ({} MB), " +
						"memory will be allocated lazily." , memoryFraction , relativeMemSize >> 20);
				}
				memorySize = relativeMemSize;
			} else if (memType == MemoryType.OFF_HEAP) {
				// The maximum heap memory has been adjusted according to the fraction
				long maxMemory = EnvironmentInformation.getMaxJvmHeapMemory();
				long directMemorySize = (long) (maxMemory / (1.0 - memoryFraction) * memoryFraction);
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
	 * @return Network environment
	 * @throws IOException
	 */
	private static NetworkEnvironment createNetworkEnvironment(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration) throws IOException {

		NetworkEnvironmentConfiguration networkEnvironmentConfiguration = taskManagerServicesConfiguration.getNetworkConfig();

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(
			networkEnvironmentConfiguration.numNetworkBuffers(),
			networkEnvironmentConfiguration.networkBufferSize(),
			networkEnvironmentConfiguration.memoryType());

		ConnectionManager connectionManager;

		if (networkEnvironmentConfiguration.nettyConfig() != null) {
			connectionManager = new NettyConnectionManager(networkEnvironmentConfiguration.nettyConfig());
		} else {
			connectionManager = new LocalConnectionManager();
		}

		ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
		TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

		KvStateRegistry kvStateRegistry = new KvStateRegistry();
		KvStateServer kvStateServer;

		if (taskManagerServicesConfiguration.getQueryableStateConfig().enabled()) {
			QueryableStateConfiguration qsConfig = taskManagerServicesConfiguration.getQueryableStateConfig();

			int numNetworkThreads = qsConfig.numServerThreads() == 0 ?
					taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numServerThreads();

			int numQueryThreads = qsConfig.numQueryThreads() == 0 ?
					taskManagerServicesConfiguration.getNumberOfSlots() : qsConfig.numQueryThreads();

			kvStateServer = new KvStateServer(
				taskManagerServicesConfiguration.getTaskManagerAddress(),
				qsConfig.port(),
				numNetworkThreads,
				numQueryThreads,
				kvStateRegistry,
				new DisabledKvStateRequestStats());
		} else {
			kvStateServer = null;
		}

		// we start the network first, to make sure it can allocate its buffers first
		return new NetworkEnvironment(
			networkBufferPool,
			connectionManager,
			resultPartitionManager,
			taskEventDispatcher,
			kvStateRegistry,
			kvStateServer,
			networkEnvironmentConfiguration.ioMode(),
			networkEnvironmentConfiguration.partitionRequestInitialBackoff(),
			networkEnvironmentConfiguration.partitionRequestMaxBackoff());
	}

	/**
	 * Validates that all the directories denoted by the strings do actually exist, are proper
	 * directories (not files), and are writable.
	 *
	 * @param tmpDirs The array of directory paths to check.
	 * @throws IOException Thrown if any of the directories does not exist or is not writable
	 *                     or is a file, rather than a directory.
	 */
	private static void checkTempDirs(String[] tmpDirs) throws IOException {
		for (String dir : tmpDirs) {
			if (dir != null && !dir.equals("")) {
				File file = new File(dir);
				if (!file.exists()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " does not exist.");
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
					double usablePercentage = (double)usableSpaceGb / totalSpaceGb * 100;
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
