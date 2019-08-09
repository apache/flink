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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;

/**
 * Container for {@link TaskExecutor} services such as the {@link MemoryManager}, {@link IOManager},
 * {@link ShuffleEnvironment}. All services are exclusive to a single {@link TaskExecutor}.
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
	private final ShuffleEnvironment<?, ?> shuffleEnvironment;
	private final KvStateService kvStateService;
	private final BroadcastVariableManager broadcastVariableManager;
	private final TaskSlotTable taskSlotTable;
	private final JobManagerTable jobManagerTable;
	private final JobLeaderService jobLeaderService;
	private final TaskExecutorLocalStateStoresManager taskManagerStateStore;
	private final TaskEventDispatcher taskEventDispatcher;

	TaskManagerServices(
		TaskManagerLocation taskManagerLocation,
		MemoryManager memoryManager,
		IOManager ioManager,
		ShuffleEnvironment<?, ?> shuffleEnvironment,
		KvStateService kvStateService,
		BroadcastVariableManager broadcastVariableManager,
		TaskSlotTable taskSlotTable,
		JobManagerTable jobManagerTable,
		JobLeaderService jobLeaderService,
		TaskExecutorLocalStateStoresManager taskManagerStateStore,
		TaskEventDispatcher taskEventDispatcher) {

		this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
		this.memoryManager = Preconditions.checkNotNull(memoryManager);
		this.ioManager = Preconditions.checkNotNull(ioManager);
		this.shuffleEnvironment = Preconditions.checkNotNull(shuffleEnvironment);
		this.kvStateService = Preconditions.checkNotNull(kvStateService);
		this.broadcastVariableManager = Preconditions.checkNotNull(broadcastVariableManager);
		this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
		this.jobManagerTable = Preconditions.checkNotNull(jobManagerTable);
		this.jobLeaderService = Preconditions.checkNotNull(jobLeaderService);
		this.taskManagerStateStore = Preconditions.checkNotNull(taskManagerStateStore);
		this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
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

	public ShuffleEnvironment<?, ?> getShuffleEnvironment() {
		return shuffleEnvironment;
	}

	public KvStateService getKvStateService() {
		return kvStateService;
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

	public TaskEventDispatcher getTaskEventDispatcher() {
		return taskEventDispatcher;
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
			ioManager.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			shuffleEnvironment.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			kvStateService.shutdown();
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

		taskEventDispatcher.clearAll();

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
	 * @param taskManagerServicesConfiguration task manager configuration
	 * @param taskManagerMetricGroup metric group of the task manager
	 * @param taskIOExecutor executor for async IO operations
	 * @return task manager components
	 * @throws Exception
	 */
	public static TaskManagerServices fromConfiguration(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration,
			MetricGroup taskManagerMetricGroup,
			Executor taskIOExecutor) throws Exception {

		// pre-start checks
		checkTempDirs(taskManagerServicesConfiguration.getTmpDirPaths());

		final TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

		// start the I/O manager, it will create some temp directories.
		final IOManager ioManager = new IOManagerAsync(taskManagerServicesConfiguration.getTmpDirPaths());

		final ShuffleEnvironment<?, ?> shuffleEnvironment = createShuffleEnvironment(
			taskManagerServicesConfiguration,
			taskEventDispatcher,
			taskManagerMetricGroup);
		final int dataPort = shuffleEnvironment.start();

		final KvStateService kvStateService = KvStateService.fromConfiguration(taskManagerServicesConfiguration);
		kvStateService.start();

		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(
			taskManagerServicesConfiguration.getResourceID(),
			taskManagerServicesConfiguration.getTaskManagerAddress(),
			dataPort);

		// this call has to happen strictly after the network stack has been initialized
		final MemoryManager memoryManager = createMemoryManager(taskManagerServicesConfiguration);
		final long managedMemorySize = memoryManager.getMemorySize();

		final BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();

		final int numOfSlots = taskManagerServicesConfiguration.getNumberOfSlots();
		final List<ResourceProfile> resourceProfiles =
			Collections.nCopies(numOfSlots, computeSlotResourceProfile(numOfSlots, managedMemorySize));

		final TimerService<AllocationID> timerService = new TimerService<>(
			new ScheduledThreadPoolExecutor(1),
			taskManagerServicesConfiguration.getTimerServiceShutdownTimeout());

		final TaskSlotTable taskSlotTable = new TaskSlotTable(resourceProfiles, timerService);

		final JobManagerTable jobManagerTable = new JobManagerTable();

		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation, taskManagerServicesConfiguration.getRetryingRegistrationConfiguration());

		final String[] stateRootDirectoryStrings = taskManagerServicesConfiguration.getLocalRecoveryStateRootDirectories();

		final File[] stateRootDirectoryFiles = new File[stateRootDirectoryStrings.length];

		for (int i = 0; i < stateRootDirectoryStrings.length; ++i) {
			stateRootDirectoryFiles[i] = new File(stateRootDirectoryStrings[i], LOCAL_STATE_SUB_DIRECTORY_ROOT);
		}

		final TaskExecutorLocalStateStoresManager taskStateManager = new TaskExecutorLocalStateStoresManager(
			taskManagerServicesConfiguration.isLocalRecoveryEnabled(),
			stateRootDirectoryFiles,
			taskIOExecutor);

		return new TaskManagerServices(
			taskManagerLocation,
			memoryManager,
			ioManager,
			shuffleEnvironment,
			kvStateService,
			broadcastVariableManager,
			taskSlotTable,
			jobManagerTable,
			jobLeaderService,
			taskStateManager,
			taskEventDispatcher);
	}

	private static ShuffleEnvironment<?, ?> createShuffleEnvironment(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration,
			TaskEventDispatcher taskEventDispatcher,
			MetricGroup taskManagerMetricGroup) throws FlinkException {

		final ShuffleEnvironmentContext shuffleEnvironmentContext = new ShuffleEnvironmentContext(
			taskManagerServicesConfiguration.getConfiguration(),
			taskManagerServicesConfiguration.getResourceID(),
			taskManagerServicesConfiguration.getMaxJvmHeapMemory(),
			taskManagerServicesConfiguration.isLocalCommunicationOnly(),
			taskManagerServicesConfiguration.getTaskManagerAddress(),
			taskEventDispatcher,
			taskManagerMetricGroup);

		return ShuffleServiceLoader
			.loadShuffleServiceFactory(taskManagerServicesConfiguration.getConfiguration())
			.createShuffleEnvironment(shuffleEnvironmentContext);
	}

	/**
	 * Creates a {@link MemoryManager} from the given {@link TaskManagerServicesConfiguration}.
	 *
	 * @param taskManagerServicesConfiguration to create the memory manager from
	 * @return Memory manager
	 * @throws Exception
	 */
	private static MemoryManager createMemoryManager(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration) throws Exception {
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
				long freeHeapMemoryWithDefrag = taskManagerServicesConfiguration.getFreeHeapMemoryWithDefrag();
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
				long maxJvmHeapMemory = taskManagerServicesConfiguration.getMaxJvmHeapMemory();
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
				taskManagerServicesConfiguration.getPageSize(),
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

		// all values below here are in bytes

		final long totalProcessMemory = megabytesToBytes(totalJavaMemorySizeMB);
		final long networkReservedMemory = getReservedNetworkMemory(config, totalProcessMemory);
		final long heapAndManagedMemory = totalProcessMemory - networkReservedMemory;

		if (config.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP)) {
			final long managedMemorySize = getManagedMemoryFromHeapAndManaged(config, heapAndManagedMemory);

			ConfigurationParserUtils.checkConfigParameter(managedMemorySize < heapAndManagedMemory, managedMemorySize,
				TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
					"Managed memory size too large for " + (networkReservedMemory >> 20) +
						" MB network buffer memory and a total of " + totalJavaMemorySizeMB +
						" MB JVM memory");

			return bytesToMegabytes(heapAndManagedMemory - managedMemorySize);
		}
		else {
			return bytesToMegabytes(heapAndManagedMemory);
		}
	}

	/**
	 * Gets the size of managed memory from the JVM process size, which at that point includes
	 * network buffer memory, managed memory, and non-flink-managed heap memory.
	 * All values are in bytes.
	 */
	public static long getManagedMemoryFromProcessMemory(Configuration config, long totalProcessMemory) {
		final long heapAndManagedMemory = totalProcessMemory - getReservedNetworkMemory(config, totalProcessMemory);
		return getManagedMemoryFromHeapAndManaged(config, heapAndManagedMemory);
	}

	/**
	 * Gets the size of managed memory from the heap size after subtracting network buffer memory.
	 * All values are in bytes.
	 */
	public static long getManagedMemoryFromHeapAndManaged(Configuration config, long heapAndManagedMemory) {
		if (config.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE)) {
			// take the configured absolute value
			final String sizeValue = config.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE);
			try {
				return MemorySize.parse(sizeValue, MEGA_BYTES).getBytes();
			}
			catch (IllegalArgumentException e) {
				throw new IllegalConfigurationException(
					"Could not read " + TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), e);
			}
		}
		else {
			// calculate managed memory size via fraction
			final float fraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
			return (long) (fraction * heapAndManagedMemory);
		}
	}

	/**
	 * Gets the amount of memory reserved for networking, given the total JVM memory.
	 * All values are in bytes.
	 */
	public static long getReservedNetworkMemory(Configuration config, long totalProcessMemory) {
		// subtract the Java memory used for network buffers (always off-heap)
		return NettyShuffleEnvironmentConfiguration.calculateNetworkBufferMemory(totalProcessMemory, config);
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

	public static ResourceProfile computeSlotResourceProfile(int numOfSlots, long managedMemorySize) {
		int managedMemoryPerSlotMB = (int) bytesToMegabytes(managedMemorySize / numOfSlots);
		return new ResourceProfile(
			Double.MAX_VALUE,
			Integer.MAX_VALUE,
			Integer.MAX_VALUE,
			Integer.MAX_VALUE,
			Integer.MAX_VALUE,
			managedMemoryPerSlotMB,
			Collections.emptyMap());
	}

	private static long bytesToMegabytes(long bytes) {
		return bytes >> 20;
	}

	private static long megabytesToBytes(long megabytes) {
		return megabytes << 20;
	}
}
