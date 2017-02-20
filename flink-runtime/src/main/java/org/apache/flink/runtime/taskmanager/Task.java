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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineTaskNotCheckpointingException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineTaskNotReadyException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.jobgraph.tasks.StoppableTask;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Task represents one execution of a parallel subtask on a TaskManager.
 * A Task wraps a Flink operator (which may be a user function) and
 * runs it, providing all services necessary for example to consume input data,
 * produce its results (intermediate result partitions) and communicate
 * with the JobManager.
 *
 * <p>The Flink operators (implemented as subclasses of
 * {@link AbstractInvokable} have only data readers, -writers, and certain event callbacks.
 * The task connects those to the network stack and actor messages, and tracks the state
 * of the execution and handles exceptions.
 *
 * <p>Tasks have no knowledge about how they relate to other tasks, or whether they
 * are the first attempt to execute the task, or a repeated attempt. All of that
 * is only known to the JobManager. All the task knows are its own runnable code,
 * the task's configuration, and the IDs of the intermediate results to consume and
 * produce (if any).
 *
 * <p>Each Task is run by one dedicated thread.
 */
public class Task implements Runnable, TaskActions {

	/** The class logger. */
	private static final Logger LOG = LoggerFactory.getLogger(Task.class);

	/** The tread group that contains all task threads */
	private static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

	/** For atomic state updates */
	private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Task.class, ExecutionState.class, "executionState");

	// ------------------------------------------------------------------------
	//  Constant fields that are part of the initial Task construction
	// ------------------------------------------------------------------------

	/** The job that the task belongs to */
	private final JobID jobId;

	/** The vertex in the JobGraph whose code the task executes */
	private final JobVertexID vertexId;

	/** The execution attempt of the parallel subtask */
	private final ExecutionAttemptID executionId;

	/** ID which identifies the slot in which the task is supposed to run */
	private final AllocationID allocationId;

	/** TaskInfo object for this task */
	private final TaskInfo taskInfo;

	/** The name of the task, including subtask indexes */
	private final String taskNameWithSubtask;

	/** The job-wide configuration object */
	private final Configuration jobConfiguration;

	/** The task-specific configuration */
	private final Configuration taskConfiguration;

	/** The jar files used by this task */
	private final Collection<BlobKey> requiredJarFiles;

	/** The classpaths used by this task */
	private final Collection<URL> requiredClasspaths;

	/** The name of the class that holds the invokable code */
	private final String nameOfInvokableClass;

	/** Access to task manager configuration and host names*/
	private final TaskManagerRuntimeInfo taskManagerConfig;
	
	/** The memory manager to be used by this task */
	private final MemoryManager memoryManager;

	/** The I/O manager to be used by this task */
	private final IOManager ioManager;

	/** The BroadcastVariableManager to be used by this task */
	private final BroadcastVariableManager broadcastVariableManager;

	/** Serialized version of the job specific execution configuration (see {@link ExecutionConfig}). */
	private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

	private final ResultPartition[] producedPartitions;

	private final ResultPartitionWriter[] writers;

	private final SingleInputGate[] inputGates;

	private final Map<IntermediateDataSetID, SingleInputGate> inputGatesById;

	/** Connection to the task manager */
	private final TaskManagerActions taskManagerActions;

	/** Input split provider for the task */
	private final InputSplitProvider inputSplitProvider;

	/** Checkpoint notifier used to communicate with the CheckpointCoordinator */
	private final CheckpointResponder checkpointResponder;

	/** All listener that want to be notified about changes in the task's execution state */
	private final List<TaskExecutionStateListener> taskExecutionStateListeners;

	/** The library cache, from which the task can request its required JAR files */
	private final LibraryCacheManager libraryCache;

	/** The cache for user-defined files that the invokable requires */
	private final FileCache fileCache;

	/** The gateway to the network stack, which handles inputs and produced results */
	private final NetworkEnvironment network;

	/** The registry of this task which enables live reporting of accumulators */
	private final AccumulatorRegistry accumulatorRegistry;

	/** The thread that executes the task */
	private final Thread executingThread;

	/** Parent group for all metrics of this task */
	private final TaskMetricGroup metrics;

	/** Partition producer state checker to request partition states from */
	private final PartitionProducerStateChecker partitionProducerStateChecker;

	/** Executor to run future callbacks */
	private final Executor executor;

	// ------------------------------------------------------------------------
	//  Fields that control the task execution. All these fields are volatile
	//  (which means that they introduce memory barriers), to establish
	//  proper happens-before semantics on parallel modification
	// ------------------------------------------------------------------------

	/** atomic flag that makes sure the invokable is canceled exactly once upon error */
	private final AtomicBoolean invokableHasBeenCanceled;

	/** The invokable of this task, if initialized */
	private volatile AbstractInvokable invokable;

	/** The current execution state of the task */
	private volatile ExecutionState executionState = ExecutionState.CREATED;

	/** The observed exception, in case the task execution failed */
	private volatile Throwable failureCause;

	/** Serial executor for asynchronous calls (checkpoints, etc), lazily initialized */
	private volatile ExecutorService asyncCallDispatcher;

	/**
	 * The handles to the states that the task was initialized with. Will be set
	 * to null after the initialization, to be memory friendly.
	 */
	private volatile TaskStateHandles taskStateHandles;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationInterval;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationTimeout;

	/**
	 * <p><b>IMPORTANT:</b> This constructor may not start any work that would need to
	 * be undone in the case of a failing task deployment.</p>
	 */
	public Task(
		JobInformation jobInformation,
		TaskInformation taskInformation,
		ExecutionAttemptID executionAttemptID,
		AllocationID slotAllocationId,
		int subtaskIndex,
		int attemptNumber,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		int targetSlotNumber,
		TaskStateHandles taskStateHandles,
		MemoryManager memManager,
		IOManager ioManager,
		NetworkEnvironment networkEnvironment,
		BroadcastVariableManager bcVarManager,
		TaskManagerActions taskManagerActions,
		InputSplitProvider inputSplitProvider,
		CheckpointResponder checkpointResponder,
		LibraryCacheManager libraryCache,
		FileCache fileCache,
		TaskManagerRuntimeInfo taskManagerConfig,
		TaskMetricGroup metricGroup,
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
		PartitionProducerStateChecker partitionProducerStateChecker,
		Executor executor) {

		Preconditions.checkNotNull(jobInformation);
		Preconditions.checkNotNull(taskInformation);

		Preconditions.checkArgument(0 <= subtaskIndex, "The subtask index must be positive.");
		Preconditions.checkArgument(0 <= attemptNumber, "The attempt number must be positive.");
		Preconditions.checkArgument(0 <= targetSlotNumber, "The target slot number must be positive.");

		this.taskInfo = new TaskInfo(
				taskInformation.getTaskName(),
				taskInformation.getNumberOfKeyGroups(),
				subtaskIndex,
				taskInformation.getNumberOfSubtasks(),
				attemptNumber);

		this.jobId = jobInformation.getJobId();
		this.vertexId = taskInformation.getJobVertexId();
		this.executionId  = Preconditions.checkNotNull(executionAttemptID);
		this.allocationId = Preconditions.checkNotNull(slotAllocationId);
		this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
		this.jobConfiguration = jobInformation.getJobConfiguration();
		this.taskConfiguration = taskInformation.getTaskConfiguration();
		this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
		this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
		this.nameOfInvokableClass = taskInformation.getInvokableClassName();
		this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();
		this.taskStateHandles = taskStateHandles;

		Configuration tmConfig = taskManagerConfig.getConfiguration();
		this.taskCancellationInterval = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
		this.taskCancellationTimeout = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);

		this.memoryManager = Preconditions.checkNotNull(memManager);
		this.ioManager = Preconditions.checkNotNull(ioManager);
		this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
		this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

		this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
		this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
		this.taskManagerActions = checkNotNull(taskManagerActions);

		this.libraryCache = Preconditions.checkNotNull(libraryCache);
		this.fileCache = Preconditions.checkNotNull(fileCache);
		this.network = Preconditions.checkNotNull(networkEnvironment);
		this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

		this.taskExecutionStateListeners = new CopyOnWriteArrayList<>();
		this.metrics = metricGroup;

		this.partitionProducerStateChecker = Preconditions.checkNotNull(partitionProducerStateChecker);
		this.executor = Preconditions.checkNotNull(executor);

		// create the reader and writer structures

		final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';

		// Produced intermediate result partitions
		this.producedPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];
		this.writers = new ResultPartitionWriter[resultPartitionDeploymentDescriptors.size()];

		int counter = 0;

		for (ResultPartitionDeploymentDescriptor desc: resultPartitionDeploymentDescriptors) {
			ResultPartitionID partitionId = new ResultPartitionID(desc.getPartitionId(), executionId);

			this.producedPartitions[counter] = new ResultPartition(
				taskNameWithSubtaskAndId,
				this,
				jobId,
				partitionId,
				desc.getPartitionType(),
				desc.getNumberOfSubpartitions(),
				desc.getMaxParallelism(),
				networkEnvironment.getResultPartitionManager(),
				resultPartitionConsumableNotifier,
				ioManager,
				desc.sendScheduleOrUpdateConsumersMessage());

			writers[counter] = new ResultPartitionWriter(producedPartitions[counter]);

			++counter;
		}

		// Consumed intermediate result partitions
		this.inputGates = new SingleInputGate[inputGateDeploymentDescriptors.size()];
		this.inputGatesById = new HashMap<>();

		counter = 0;

		for (InputGateDeploymentDescriptor inputGateDeploymentDescriptor: inputGateDeploymentDescriptors) {
			SingleInputGate gate = SingleInputGate.create(
				taskNameWithSubtaskAndId,
				jobId,
				executionId,
				inputGateDeploymentDescriptor,
				networkEnvironment,
				this,
				metricGroup.getIOMetricGroup());

			inputGates[counter] = gate;
			inputGatesById.put(gate.getConsumedResultId(), gate);

			++counter;
		}

		invokableHasBeenCanceled = new AtomicBoolean(false);

		// finally, create the executing thread, but do not start it
		executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);

		if (this.metrics != null && this.metrics.getIOMetricGroup() != null) {
			// add metrics for buffers
			this.metrics.getIOMetricGroup().initializeBufferMetrics(this);
		}
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public JobID getJobID() {
		return jobId;
	}

	public JobVertexID getJobVertexId() {
		return vertexId;
	}

	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	public ResultPartitionWriter[] getAllWriters() {
		return writers;
	}

	public SingleInputGate[] getAllInputGates() {
		return inputGates;
	}

	public ResultPartition[] getProducedPartitions() {
		return producedPartitions;
	}

	public SingleInputGate getInputGateById(IntermediateDataSetID id) {
		return inputGatesById.get(id);
	}

	public AccumulatorRegistry getAccumulatorRegistry() {
		return accumulatorRegistry;
	}

	public TaskMetricGroup getMetricGroup() {
		return metrics;
	}

	public Thread getExecutingThread() {
		return executingThread;
	}

	@VisibleForTesting
	long getTaskCancellationInterval() {
		return taskCancellationInterval;
	}

	@VisibleForTesting
	long getTaskCancellationTimeout() {
		return taskCancellationTimeout;
	}

	// ------------------------------------------------------------------------
	//  Task Execution
	// ------------------------------------------------------------------------

	/**
	 * Returns the current execution state of the task.
	 * @return The current execution state of the task.
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	/**
	 * Checks whether the task has failed, is canceled, or is being canceled at the moment.
	 * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
	 */
	public boolean isCanceledOrFailed() {
		return executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.CANCELED ||
				executionState == ExecutionState.FAILED;
	}

	/**
	 * If the task has failed, this method gets the exception that caused this task to fail.
	 * Otherwise this method returns null.
	 *
	 * @return The exception that caused the task to fail, or null, if the task has not failed.
	 */
	public Throwable getFailureCause() {
		return failureCause;
	}

	/**
	 * Starts the task's thread.
	 */
	public void startTaskThread() {
		executingThread.start();
	}

	/**
	 * The core work method that bootstraps the task and executes it code
	 */
	@Override
	public void run() {

		// ----------------------------
		//  Initial State transition
		// ----------------------------
		while (true) {
			ExecutionState current = this.executionState;
			if (current == ExecutionState.CREATED) {
				if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
					// success, we can start our work
					break;
				}
			}
			else if (current == ExecutionState.FAILED) {
				// we were immediately failed. tell the TaskManager that we reached our final state
				notifyFinalState();
				return;
			}
			else if (current == ExecutionState.CANCELING) {
				if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
					// we were immediately canceled. tell the TaskManager that we reached our final state
					notifyFinalState();
					return;
				}
			}
			else {
				throw new IllegalStateException("Invalid state for beginning of operation of task " + this + '.');
			}
		}

		// all resource acquisitions and registrations from here on
		// need to be undone in the end
		Map<String, Future<Path>> distributedCacheEntries = new HashMap<String, Future<Path>>();
		AbstractInvokable invokable = null;

		ClassLoader userCodeClassLoader;
		try {
			// ----------------------------
			//  Task Bootstrap - We periodically
			//  check for canceling as a shortcut
			// ----------------------------

			// activate safety net for task thread
			FileSystemSafetyNet.initializeSafetyNetForThread();

			// first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			LOG.info("Loading JAR files for task {}.", this);

			userCodeClassLoader = createUserCodeClassloader(libraryCache);
			final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);

			if (executionConfig.getTaskCancellationInterval() >= 0) {
				// override task cancellation interval from Flink config if set in ExecutionConfig
				taskCancellationInterval = executionConfig.getTaskCancellationInterval();
			}

			if (executionConfig.getTaskCancellationTimeout() >= 0) {
				// override task cancellation timeout from Flink config if set in ExecutionConfig
				taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
			}

			// now load the task's invokable code
			invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass);

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			// register the task with the network stack
			// this operation may fail if the system does not have enough
			// memory to run the necessary data exchanges
			// the registration must also strictly be undone
			// ----------------------------------------------------------------

			LOG.info("Registering task at network: {}.", this);

			network.registerTask(this);

			// next, kick off the background copying of files for the distributed cache
			try {
				for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
						DistributedCache.readFileInfoFromConfig(jobConfiguration))
				{
					LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
					Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId);
					distributedCacheEntries.put(entry.getKey(), cp);
				}
			}
			catch (Exception e) {
				throw new Exception(
					String.format("Exception while adding files to distributed cache of task %s (%s).", taskNameWithSubtask, executionId),
					e);
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  call the user code initialization methods
			// ----------------------------------------------------------------

			TaskKvStateRegistry kvStateRegistry = network
					.createKvStateTaskRegistry(jobId, getJobVertexId());

			Environment env = new RuntimeEnvironment(
				jobId, vertexId, executionId, executionConfig, taskInfo,
				jobConfiguration, taskConfiguration, userCodeClassLoader,
				memoryManager, ioManager, broadcastVariableManager,
				accumulatorRegistry, kvStateRegistry, inputSplitProvider,
				distributedCacheEntries, writers, inputGates,
				checkpointResponder, taskManagerConfig, metrics, this);

			// let the task code create its readers and writers
			invokable.setEnvironment(env);

			// the very last thing before the actual execution starts running is to inject
			// the state into the task. the state is non-empty if this is an execution
			// of a task that failed but had backuped state from a checkpoint

			if (null != taskStateHandles) {
				if (invokable instanceof StatefulTask) {
					StatefulTask op = (StatefulTask) invokable;
					op.setInitialState(taskStateHandles);
				} else {
					throw new IllegalStateException("Found operator state for a non-stateful task invokable");
				}
				// be memory and GC friendly - since the code stays in invoke() for a potentially long time,
				// we clear the reference to the state handle
				//noinspection UnusedAssignment
				taskStateHandles = null;
			}

			// ----------------------------------------------------------------
			//  actual task core work
			// ----------------------------------------------------------------

			// we must make strictly sure that the invokable is accessible to the cancel() call
			// by the time we switched to running.
			this.invokable = invokable;

			// switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
			if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
				throw new CancelTaskException();
			}

			// notify everyone that we switched to running
			notifyObservers(ExecutionState.RUNNING, null);
			taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));

			// make sure the user code classloader is accessible thread-locally
			executingThread.setContextClassLoader(userCodeClassLoader);

			// run the invokable
			invokable.invoke();

			// make sure, we enter the catch block if the task leaves the invoke() method due
			// to the fact that it has been canceled
			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  finalization of a successful execution
			// ----------------------------------------------------------------

			// finish the produced partitions. if this fails, we consider the execution failed.
			for (ResultPartition partition : producedPartitions) {
				if (partition != null) {
					partition.finish();
				}
			}

			// try to mark the task as finished
			// if that fails, the task was canceled/failed in the meantime
			if (transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
				notifyObservers(ExecutionState.FINISHED, null);
			}
			else {
				throw new CancelTaskException();
			}
		}
		catch (Throwable t) {

			// ----------------------------------------------------------------
			// the execution failed. either the invokable code properly failed, or
			// an exception was thrown as a side effect of cancelling
			// ----------------------------------------------------------------

			try {
				// check if the exception is unrecoverable
				if (ExceptionUtils.isJvmFatalError(t) || 
					(t instanceof OutOfMemoryError && taskManagerConfig.shouldExitJvmOnOutOfMemoryError()))
				{
					// terminate the JVM immediately
					// don't attempt a clean shutdown, because we cannot expect the clean shutdown to complete
					try {
						LOG.error("Encountered fatal error {} - terminating the JVM", t.getClass().getName(), t);
					} finally {
						Runtime.getRuntime().halt(-1);
					}
				}

				// transition into our final state. we should be either in DEPLOYING, RUNNING, CANCELING, or FAILED
				// loop for multiple retries during concurrent state changes via calls to cancel() or
				// to failExternally()
				while (true) {
					ExecutionState current = this.executionState;

					if (current == ExecutionState.RUNNING || current == ExecutionState.DEPLOYING) {
						if (t instanceof CancelTaskException) {
							if (transitionState(current, ExecutionState.CANCELED)) {
								cancelInvokable();

								notifyObservers(ExecutionState.CANCELED, null);
								break;
							}
						}
						else {
							if (transitionState(current, ExecutionState.FAILED, t)) {
								// proper failure of the task. record the exception as the root cause
								String errorMessage = String.format("Execution of %s (%s) failed.", taskNameWithSubtask, executionId);
								failureCause = t;
								cancelInvokable();

								notifyObservers(ExecutionState.FAILED, new Exception(errorMessage, t));
								break;
							}
						}
					}
					else if (current == ExecutionState.CANCELING) {
						if (transitionState(current, ExecutionState.CANCELED)) {
							notifyObservers(ExecutionState.CANCELED, null);
							break;
						}
					}
					else if (current == ExecutionState.FAILED) {
						// in state failed already, no transition necessary any more
						break;
					}
					// unexpected state, go to failed
					else if (transitionState(current, ExecutionState.FAILED, t)) {
						LOG.error("Unexpected state in task {} ({}) during an exception: {}.", taskNameWithSubtask, executionId, current);
						break;
					}
					// else fall through the loop and
				}
			}
			catch (Throwable tt) {
				String message = String.format("FATAL - exception in exception handler of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, tt);
				notifyFatalError(message, tt);
			}
		}
		finally {
			try {
				LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

				// stop the async dispatcher.
				// copy dispatcher reference to stack, against concurrent release
				ExecutorService dispatcher = this.asyncCallDispatcher;
				if (dispatcher != null && !dispatcher.isShutdown()) {
					dispatcher.shutdownNow();
				}

				// free the network resources
				network.unregisterTask(this);

				// free memory resources
				if (invokable != null) {
					memoryManager.releaseAll(invokable);
				}

				// remove all of the tasks library resources
				libraryCache.unregisterTask(jobId, executionId);

				// remove all files in the distributed cache
				removeCachedFiles(distributedCacheEntries, fileCache);

				// close and de-activate safety net for task thread
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

				notifyFinalState();
			}
			catch (Throwable t) {
				// an error in the resource cleanup is fatal
				String message = String.format("FATAL - exception in resource cleanup of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, t);
				notifyFatalError(message, t);
			}

			// un-register the metrics at the end so that the task may already be
			// counted as finished when this happens
			// errors here will only be logged
			try {
				metrics.close();
			}
			catch (Throwable t) {
				LOG.error("Error during metrics de-registration of task {} ({}).", taskNameWithSubtask, executionId, t);
			}
		}
	}

	private ClassLoader createUserCodeClassloader(LibraryCacheManager libraryCache) throws Exception {
		long startDownloadTime = System.currentTimeMillis();

		// triggers the download of all missing jar files from the job manager
		libraryCache.registerTask(jobId, executionId, requiredJarFiles, requiredClasspaths);

		LOG.debug("Register task {} at library cache manager took {} milliseconds",
				executionId, System.currentTimeMillis() - startDownloadTime);

		ClassLoader userCodeClassLoader = libraryCache.getClassLoader(jobId);
		if (userCodeClassLoader == null) {
			throw new Exception("No user code classloader available.");
		}
		return userCodeClassLoader;
	}

	private AbstractInvokable loadAndInstantiateInvokable(ClassLoader classLoader, String className) throws Exception {
		Class<? extends AbstractInvokable> invokableClass;
		try {
			invokableClass = Class.forName(className, true, classLoader)
					.asSubclass(AbstractInvokable.class);
		}
		catch (Throwable t) {
			throw new Exception("Could not load the task's invokable class.", t);
		}
		try {
			return invokableClass.newInstance();
		}
		catch (Throwable t) {
			throw new Exception("Could not instantiate the task's invokable class.", t);
		}
	}

	private void removeCachedFiles(Map<String, Future<Path>> entries, FileCache fileCache) {
		// cancel and release all distributed cache files
		try {
			for (Map.Entry<String, Future<Path>> entry : entries.entrySet()) {
				String name = entry.getKey();
				try {
					fileCache.deleteTmpFile(name, jobId);
				}
				catch (Exception e) {
					// unpleasant, but we continue
					LOG.error("Distributed Cache could not remove cached file registered under '"
							+ name + "'.", e);
				}
			}
		}
		catch (Throwable t) {
			LOG.error("Error while removing cached local files from distributed cache.");
		}
	}

	private void notifyFinalState() {
		taskManagerActions.notifyFinalState(executionId);
	}

	private void notifyFatalError(String message, Throwable cause) {
		taskManagerActions.notifyFatalError(message, cause);
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState of the execution
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
		return transitionState(currentState, newState, null);
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState of the execution
	 * @param cause of the transition change or null
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ExecutionState currentState, ExecutionState newState, Throwable cause) {
		if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
			if (cause == null) {
				LOG.info("{} ({}) switched from {} to {}.", taskNameWithSubtask, executionId, currentState, newState);
			} else {
				LOG.info("{} ({}) switched from {} to {}.", taskNameWithSubtask, executionId, currentState, newState, cause);
			}

			return true;
		} else {
			return false;
		}
	}

	// ----------------------------------------------------------------------------------------------------------------
	//  Stopping / Canceling / Failing the task from the outside
	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Stops the executing task by calling {@link StoppableTask#stop()}.
	 * <p>
	 * This method never blocks.
	 * </p>
	 * 
	 * @throws UnsupportedOperationException
	 *             if the {@link AbstractInvokable} does not implement {@link StoppableTask}
	 */
	public void stopExecution() throws UnsupportedOperationException {
		LOG.info("Attempting to stop task {} ({}).", taskNameWithSubtask, executionId);
		if (invokable instanceof StoppableTask) {
			Runnable runnable = new Runnable() {
				@Override
				public void run() {
					try {
						((StoppableTask)invokable).stop();
					} catch(RuntimeException e) {
						LOG.error("Stopping task {} ({}) failed.", taskNameWithSubtask, executionId, e);
						taskManagerActions.failTask(executionId, e);
					}
				}
			};
			executeAsyncCallRunnable(runnable, String.format("Stopping source task %s (%s).", taskNameWithSubtask, executionId));
		} else {
			throw new UnsupportedOperationException(String.format("Stopping not supported by task %s (%s).", taskNameWithSubtask, executionId));
		}
	}

	/**
	 * Cancels the task execution. If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to CANCELING, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 * 
	 * <p>This method never blocks.</p>
	 */
	public void cancelExecution() {
		LOG.info("Attempting to cancel task {} ({}).", taskNameWithSubtask, executionId);
		cancelOrFailAndCancelInvokable(ExecutionState.CANCELING, null);
	}

	/**
	 * Marks task execution failed for an external reason (a reason other than the task code itself
	 * throwing an exception). If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to FAILED, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	@Override
	public void failExternally(Throwable cause) {
		LOG.info("Attempting to fail task externally {} ({}).", taskNameWithSubtask, executionId);
		cancelOrFailAndCancelInvokable(ExecutionState.FAILED, cause);
	}

	private void cancelOrFailAndCancelInvokable(ExecutionState targetState, Throwable cause) {
		while (true) {
			ExecutionState current = executionState;

			// if the task is already canceled (or canceling) or finished or failed,
			// then we need not do anything
			if (current.isTerminal() || current == ExecutionState.CANCELING) {
				LOG.info("Task {} is already in state {}", taskNameWithSubtask, current);
				return;
			}

			if (current == ExecutionState.DEPLOYING || current == ExecutionState.CREATED) {
				if (transitionState(current, targetState, cause)) {
					// if we manage this state transition, then the invokable gets never called
					// we need not call cancel on it
					this.failureCause = cause;
					notifyObservers(
						targetState,
						new Exception(
							String.format(
								"Cancel or fail execution of %s (%s).",
								taskNameWithSubtask,
								executionId),
							cause));
					return;
				}
			}
			else if (current == ExecutionState.RUNNING) {
				if (transitionState(ExecutionState.RUNNING, targetState, cause)) {
					// we are canceling / failing out of the running state
					// we need to cancel the invokable
					if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
						this.failureCause = cause;
						notifyObservers(
							targetState,
							new Exception(
								String.format(
									"Cancel or fail execution of %s (%s).",
									taskNameWithSubtask,
									executionId),
								cause));

						LOG.info("Triggering cancellation of task code {} ({}).", taskNameWithSubtask, executionId);

						// because the canceling may block on user code, we cancel from a separate thread
						// we do not reuse the async call handler, because that one may be blocked, in which
						// case the canceling could not continue

						// The canceller calls cancel and interrupts the executing thread once
						Runnable canceler = new TaskCanceler(
								LOG,
								invokable,
								executingThread,
								taskNameWithSubtask,
								taskCancellationInterval,
								taskCancellationTimeout,
								taskManagerActions,
								producedPartitions,
								inputGates);
						Thread cancelThread = new Thread(executingThread.getThreadGroup(), canceler,
								String.format("Canceler for %s (%s).", taskNameWithSubtask, executionId));
						cancelThread.setDaemon(true);
						cancelThread.start();
					}
					return;
				}
			}
			else {
				throw new IllegalStateException(String.format("Unexpected state: %s of task %s (%s).",
					current, taskNameWithSubtask, executionId));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State Listeners
	// ------------------------------------------------------------------------

	public void registerExecutionListener(TaskExecutionStateListener listener) {
		taskExecutionStateListeners.add(listener);
	}

	private void notifyObservers(ExecutionState newState, Throwable error) {
		TaskExecutionState stateUpdate = new TaskExecutionState(jobId, executionId, newState, error);

		for (TaskExecutionStateListener listener : taskExecutionStateListeners) {
			listener.notifyTaskExecutionStateChanged(stateUpdate);
		}
	}

	// ------------------------------------------------------------------------
	//  Partition State Listeners
	// ------------------------------------------------------------------------

	@Override
	public void triggerPartitionProducerStateCheck(
		JobID jobId,
		final IntermediateDataSetID intermediateDataSetId,
		final ResultPartitionID resultPartitionId) {

		org.apache.flink.runtime.concurrent.Future<ExecutionState> futurePartitionState =
			partitionProducerStateChecker.requestPartitionProducerState(
				jobId,
				intermediateDataSetId,
				resultPartitionId);

		futurePartitionState.handleAsync(new BiFunction<ExecutionState, Throwable, Void>() {
			@Override
			public Void apply(ExecutionState executionState, Throwable throwable) {
				try {
					if (executionState != null) {
						onPartitionStateUpdate(
							intermediateDataSetId,
							resultPartitionId,
							executionState);
					} else if (throwable instanceof TimeoutException) {
						// our request timed out, assume we're still running and try again
						onPartitionStateUpdate(
							intermediateDataSetId,
							resultPartitionId,
							ExecutionState.RUNNING);
					} else if (throwable instanceof PartitionProducerDisposedException) {
						String msg = String.format("Producer %s of partition %s disposed. Cancelling execution.",
							resultPartitionId.getProducerId(), resultPartitionId.getPartitionId());
						LOG.info(msg, throwable);
						cancelExecution();
					} else {
						failExternally(throwable);
					}
				} catch (IOException | InterruptedException e) {
					failExternally(e);
				}

				return null;
			}
		}, executor);
	}

	// ------------------------------------------------------------------------
	//  Notifications on the invokable
	// ------------------------------------------------------------------------

	/**
	 * Calls the invokable to trigger a checkpoint, if the invokable implements the interface
	 * {@link StatefulTask}.
	 * 
	 * @param checkpointID The ID identifying the checkpoint.
	 * @param checkpointTimestamp The timestamp associated with the checkpoint.
	 * @param checkpointOptions Options for performing this checkpoint.
	 */
	public void triggerCheckpointBarrier(
			final long checkpointID,
			long checkpointTimestamp,
			final CheckpointOptions checkpointOptions) {

		final AbstractInvokable invokable = this.invokable;
		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointID, checkpointTimestamp);

		if (executionState == ExecutionState.RUNNING && invokable != null) {
			if (invokable instanceof StatefulTask) {
				// build a local closure
				final StatefulTask statefulTask = (StatefulTask) invokable;
				final String taskName = taskNameWithSubtask;

				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						// activate safety net for checkpointing thread
						FileSystemSafetyNet.initializeSafetyNetForThread();
						try {
							boolean success = statefulTask.triggerCheckpoint(checkpointMetaData, checkpointOptions);
							if (!success) {
								checkpointResponder.declineCheckpoint(
										getJobID(), getExecutionId(), checkpointID,
										new CheckpointDeclineTaskNotReadyException(taskName));
							}
						}
						catch (Throwable t) {
							if (getExecutionState() == ExecutionState.RUNNING) {
								failExternally(new Exception(
									"Error while triggering checkpoint " + checkpointID + " for " +
										taskNameWithSubtask, t));
							} else {
								LOG.debug("Encountered error while triggering checkpoint {} for " +
									"{} ({}) while being not in state running.", checkpointID,
									taskNameWithSubtask, executionId, t);
							}
						} finally {
							// close and de-activate safety net for checkpointing thread
							FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
						}
					}
				};
				executeAsyncCallRunnable(runnable, String.format("Checkpoint Trigger for %s (%s).", taskNameWithSubtask, executionId));
			}
			else {
				checkpointResponder.declineCheckpoint(jobId, executionId, checkpointID,
						new CheckpointDeclineTaskNotCheckpointingException(taskNameWithSubtask));
				
				LOG.error("Task received a checkpoint request, but is not a checkpointing task - {} ({}).",
						taskNameWithSubtask, executionId);

			}
		}
		else {
			LOG.debug("Declining checkpoint request for non-running task {} ({}).", taskNameWithSubtask, executionId);

			// send back a message that we did not do the checkpoint
			checkpointResponder.declineCheckpoint(jobId, executionId, checkpointID,
					new CheckpointDeclineTaskNotReadyException(taskNameWithSubtask));
		}
	}

	public void notifyCheckpointComplete(final long checkpointID) {
		AbstractInvokable invokable = this.invokable;

		if (executionState == ExecutionState.RUNNING && invokable != null) {
			if (invokable instanceof StatefulTask) {

				// build a local closure
				final StatefulTask statefulTask = (StatefulTask) invokable;
				final String taskName = taskNameWithSubtask;

				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						try {
							statefulTask.notifyCheckpointComplete(checkpointID);
						}
						catch (Throwable t) {
							if (getExecutionState() == ExecutionState.RUNNING) {
								// fail task if checkpoint confirmation failed.
								failExternally(new RuntimeException(
									"Error while confirming checkpoint",
									t));
							}
						}
					}
				};
				executeAsyncCallRunnable(runnable, "Checkpoint Confirmation for " + taskName);
			}
			else {
				LOG.error("Task received a checkpoint commit notification, but is not a checkpoint committing task - {}.",
						taskNameWithSubtask);
			}
		}
		else {
			LOG.debug("Ignoring checkpoint commit notification for non-running task {}.", taskNameWithSubtask);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Answer to a partition state check issued after a failed partition request.
	 */
	@VisibleForTesting
	void onPartitionStateUpdate(
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionID resultPartitionId,
			ExecutionState producerState) throws IOException, InterruptedException {

		if (executionState == ExecutionState.RUNNING) {
			final SingleInputGate inputGate = inputGatesById.get(intermediateDataSetId);

			if (inputGate != null) {
				if (producerState == ExecutionState.SCHEDULED
					|| producerState == ExecutionState.DEPLOYING
					|| producerState == ExecutionState.RUNNING
					|| producerState == ExecutionState.FINISHED) {

					// Retrigger the partition request
					inputGate.retriggerPartitionRequest(resultPartitionId.getPartitionId());

				} else if (producerState == ExecutionState.CANCELING
					|| producerState == ExecutionState.CANCELED
					|| producerState == ExecutionState.FAILED) {

					// The producing execution has been canceled or failed. We
					// don't need to re-trigger the request since it cannot
					// succeed.
					if (LOG.isDebugEnabled()) {
						LOG.debug("Cancelling task {} after the producer of partition {} with attempt ID {} has entered state {}.",
							taskNameWithSubtask,
							resultPartitionId.getPartitionId(),
							resultPartitionId.getProducerId(),
							producerState);
					}

					cancelExecution();
				} else {
					// Any other execution state is unexpected. Currently, only
					// state CREATED is left out of the checked states. If we
					// see a producer in this state, something went wrong with
					// scheduling in topological order.
					String msg = String.format("Producer with attempt ID %s of partition %s in unexpected state %s.",
						resultPartitionId.getProducerId(),
						resultPartitionId.getPartitionId(),
						producerState);

					failExternally(new IllegalStateException(msg));
				}
			} else {
				failExternally(new IllegalStateException("Received partition producer state for " +
						"unknown input gate " + intermediateDataSetId + "."));
			}
		} else {
			LOG.debug("Task {} ignored a partition producer state notification, because it's not running.", taskNameWithSubtask);
		}
	}

	/**
	 * Utility method to dispatch an asynchronous call on the invokable.
	 * 
	 * @param runnable The async call runnable.
	 * @param callName The name of the call, for logging purposes.
	 */
	private void executeAsyncCallRunnable(Runnable runnable, String callName) {
		// make sure the executor is initialized. lock against concurrent calls to this function
		synchronized (this) {
			if (executionState != ExecutionState.RUNNING) {
				return;
			}
			
			// get ourselves a reference on the stack that cannot be concurrently modified
			ExecutorService executor = this.asyncCallDispatcher;
			if (executor == null) {
				// first time use, initialize
				executor = Executors.newSingleThreadExecutor(
						new DispatcherThreadFactory(TASK_THREADS_GROUP, "Async calls on " + taskNameWithSubtask));
				this.asyncCallDispatcher = executor;
				
				// double-check for execution state, and make sure we clean up after ourselves
				// if we created the dispatcher while the task was concurrently canceled
				if (executionState != ExecutionState.RUNNING) {
					executor.shutdown();
					asyncCallDispatcher = null;
					return;
				}
			}

			LOG.debug("Invoking async call {} on task {}", callName, taskNameWithSubtask);

			try {
				executor.submit(runnable);
			}
			catch (RejectedExecutionException e) {
				// may be that we are concurrently finished or canceled.
				// if not, report that something is fishy
				if (executionState == ExecutionState.RUNNING) {
					throw new RuntimeException("Async call was rejected, even though the task is running.", e);
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void cancelInvokable() {
		// in case of an exception during execution, we still call "cancel()" on the task
		if (invokable != null && this.invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
			try {
				invokable.cancel();
			}
			catch (Throwable t) {
				LOG.error("Error while canceling task {}.", taskNameWithSubtask, t);
			}
		}
	}

	@Override
	public String toString() {
		return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
	}

	/**
	 * This runner calls cancel() on the invokable and periodically interrupts the
	 * thread until it has terminated.
	 */
	private static class TaskCanceler implements Runnable {

		private final Logger logger;
		private final AbstractInvokable invokable;
		private final Thread executer;
		private final String taskName;
		private final ResultPartition[] producedPartitions;
		private final SingleInputGate[] inputGates;

		/** Interrupt interval. */
		private final long interruptInterval;

		/** Timeout after which a fatal error notification happens. */
		private final long interruptTimeout;

		/** TaskManager to notify about a timeout */
		private final TaskManagerActions taskManager;

		/** Watch Dog thread */
		@Nullable
		private final Thread watchDogThread;

		public TaskCanceler(
				Logger logger,
				AbstractInvokable invokable,
				Thread executer,
				String taskName,
				long cancellationInterval,
				long cancellationTimeout,
				TaskManagerActions taskManager,
				ResultPartition[] producedPartitions,
				SingleInputGate[] inputGates) {

			this.logger = logger;
			this.invokable = invokable;
			this.executer = executer;
			this.taskName = taskName;
			this.interruptInterval = cancellationInterval;
			this.interruptTimeout = cancellationTimeout;
			this.taskManager = taskManager;
			this.producedPartitions = producedPartitions;
			this.inputGates = inputGates;

			if (cancellationTimeout > 0) {
				// The watch dog repeatedly interrupts the executor until
				// the cancellation timeout kicks in (at which point the
				// task manager is notified about a fatal error) or the
				// executor has terminated.
				this.watchDogThread = new Thread(
						executer.getThreadGroup(),
						new TaskCancelerWatchDog(),
						"WatchDog for " + taskName + " cancellation");
				this.watchDogThread.setDaemon(true);
			} else {
				this.watchDogThread = null;
			}
		}

		@Override
		public void run() {
			try {
				if (watchDogThread != null) {
					watchDogThread.start();
				}

				// the user-defined cancel method may throw errors.
				// we need do continue despite that
				try {
					invokable.cancel();
				} catch (Throwable t) {
					logger.error("Error while canceling the task {}.", taskName, t);
				}

				// Early release of input and output buffer pools. We do this
				// in order to unblock async Threads, which produce/consume the
				// intermediate streams outside of the main Task Thread (like
				// the Kafka consumer).
				//
				// Don't do this before cancelling the invokable. Otherwise we
				// will get misleading errors in the logs.
				for (ResultPartition partition : producedPartitions) {
					try {
						partition.destroyBufferPool();
					} catch (Throwable t) {
						LOG.error("Failed to release result partition buffer pool for task {}.", taskName, t);
					}
				}

				for (SingleInputGate inputGate : inputGates) {
					try {
						inputGate.releaseAllResources();
					} catch (Throwable t) {
						LOG.error("Failed to release input gate for task {}.", taskName, t);
					}
				}

				// interrupt the running thread initially
				executer.interrupt();
				try {
					executer.join(interruptInterval);
				}
				catch (InterruptedException e) {
					// we can ignore this
				}

				if (watchDogThread != null) {
					watchDogThread.interrupt();
					watchDogThread.join();
				}
			} catch (Throwable t) {
				logger.error("Error in the task canceler for task {}.", taskName, t);
			}
		}

		/**
		 * Watchdog for the cancellation. If the task is stuck in cancellation,
		 * we notify the task manager about a fatal error.
		 */
		private class TaskCancelerWatchDog implements Runnable {

			@Override
			public void run() {
				long intervalNanos = TimeUnit.NANOSECONDS.convert(interruptInterval, TimeUnit.MILLISECONDS);
				long timeoutNanos = TimeUnit.NANOSECONDS.convert(interruptTimeout, TimeUnit.MILLISECONDS);
				long deadline = System.nanoTime() + timeoutNanos;

				try {
					// Initial wait before interrupting periodically
					Thread.sleep(interruptInterval);
				} catch (InterruptedException ignored) {
				}

				// It is possible that the user code does not react to the task canceller.
				// for that reason, we spawn this separate thread that repeatedly interrupts
				// the user code until it exits. If the suer user code does not exit within
				// the timeout, we notify the job manager about a fatal error.
				while (executer.isAlive()) {
					long now = System.nanoTime();

					// build the stack trace of where the thread is stuck, for the log
					StringBuilder bld = new StringBuilder();
					StackTraceElement[] stack = executer.getStackTrace();
					for (StackTraceElement e : stack) {
						bld.append(e).append('\n');
					}

					if (now >= deadline) {
						long duration = TimeUnit.SECONDS.convert(interruptInterval, TimeUnit.MILLISECONDS);
						String msg = String.format("Task '%s' did not react to cancelling signal in " +
										"the last %d seconds, but is stuck in method:\n %s",
								taskName,
								duration,
								bld.toString());

						logger.info("Notifying TaskManager about fatal error. {}.", msg);

						taskManager.notifyFatalError(msg, null);

						return; // done, don't forget to leave the loop
					} else {
						logger.warn("Task '{}' did not react to cancelling signal, but is stuck in method:\n {}",
								taskName, bld.toString());

						executer.interrupt();
						try {
							long timeLeftNanos = Math.min(intervalNanos, deadline - now);
							long timeLeftMillis = TimeUnit.MILLISECONDS.convert(timeLeftNanos, TimeUnit.NANOSECONDS);

							if (timeLeftMillis > 0) {
								executer.join(timeLeftMillis);
							}
						} catch (InterruptedException ignored) {
						}
					}
				}
			}
		}
	}
}
