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

package org.apache.flink.runtime.execution;

import akka.actor.ActorRef;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.PartitionConsumerDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.taskmanager.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

public class RuntimeEnvironment implements Environment, Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(RuntimeEnvironment.class);

	private static final ThreadGroup TASK_THREADS = new ThreadGroup("Task Threads");

	/** The ActorRef to the job manager */
	private final ActorRef jobManager;

	/** The task that owns this environment */
	private final Task owner;

	/** The job configuration encapsulated in the environment object. */
	private final Configuration jobConfiguration;

	/** The task configuration encapsulated in the environment object. */
	private final Configuration taskConfiguration;

	/** ClassLoader for all user code classes */
	private final ClassLoader userCodeClassLoader;

	/** Instance of the class to be run in this environment. */
	private final AbstractInvokable invokable;

	/** The memory manager of the current environment (currently the one associated with the executing TaskManager). */
	private final MemoryManager memoryManager;

	/** The I/O manager of the current environment (currently the one associated with the executing TaskManager). */
	private final IOManager ioManager;

	/** The input split provider that can be queried for new input splits. */
	private final InputSplitProvider inputSplitProvider;

	/** The thread executing the task in the environment. */
	private Thread executingThread;

	private final BroadcastVariableManager broadcastVariableManager;

	private final Map<String, FutureTask<Path>> cacheCopyTasks = new HashMap<String, FutureTask<Path>>();

	private final AtomicBoolean canceled = new AtomicBoolean();

	private final IntermediateResultPartition[] producedPartitions;

	private final BufferWriter[] writers;

	private final BufferReader[] readers;

	private final Map<IntermediateDataSetID, BufferReader> readersById = new HashMap<IntermediateDataSetID, BufferReader>();

	public RuntimeEnvironment(
			ActorRef jobManager, Task owner, TaskDeploymentDescriptor tdd, ClassLoader userCodeClassLoader,
			MemoryManager memoryManager, IOManager ioManager, InputSplitProvider inputSplitProvider,
			BroadcastVariableManager broadcastVariableManager, NetworkEnvironment networkEnvironment) throws Exception {

		this.owner = checkNotNull(owner);

		this.memoryManager = checkNotNull(memoryManager);
		this.ioManager = checkNotNull(ioManager);
		this.inputSplitProvider = checkNotNull(inputSplitProvider);
		this.jobManager = checkNotNull(jobManager);

		this.broadcastVariableManager = checkNotNull(broadcastVariableManager);

		try {
			// Produced intermediate result partitions
			final List<PartitionDeploymentDescriptor> partitions = tdd.getProducedPartitions();

			this.producedPartitions = new IntermediateResultPartition[partitions.size()];
			this.writers = new BufferWriter[partitions.size()];

			for (int i = 0; i < this.producedPartitions.length; i++) {
				this.producedPartitions[i] = IntermediateResultPartition.create(this, i, owner.getJobID(), owner.getExecutionId(), networkEnvironment, partitions.get(i));
				writers[i] = new BufferWriter(this.producedPartitions[i]);
			}

			// Consumed intermediate result partitions
			final List<PartitionConsumerDeploymentDescriptor> consumedPartitions = tdd.getConsumedPartitions();

			this.readers = new BufferReader[consumedPartitions.size()];

			for (int i = 0; i < readers.length; i++) {
				readers[i] = BufferReader.create(this, networkEnvironment, consumedPartitions.get(i));

				// The readers are organized by key for task updates/channel updates at runtime
				readersById.put(readers[i].getConsumedResultId(), readers[i]);
			}

			// ----------------------------------------------------------------
			// Invokable setup
			// ----------------------------------------------------------------
			// Note: This has to be done *after* the readers and writers have
			// been setup, because the invokable relies on them for I/O.
			// ----------------------------------------------------------------

			// Load and instantiate the invokable class
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
			// Class of the task to run in this environment
			Class<? extends AbstractInvokable> invokableClass;
			try {
				final String className = tdd.getInvokableClassName();
				invokableClass = Class.forName(className, true, userCodeClassLoader).asSubclass(AbstractInvokable.class);
			}
			catch (Throwable t) {
				throw new Exception("Could not load invokable class.", t);
			}

			try {
				this.invokable = invokableClass.newInstance();
			}
			catch (Throwable t) {
				throw new Exception("Could not instantiate the invokable class.", t);
			}

			this.jobConfiguration = tdd.getJobConfiguration();
			this.taskConfiguration = tdd.getTaskConfiguration();

			this.invokable.setEnvironment(this);
			this.invokable.registerInputOutput();
		}
		catch (Throwable t) {
			throw new Exception("Error setting up runtime environment: " + t.getMessage(), t);
		}
	}

	/**
	 * Returns the task invokable instance.
	 */
	public AbstractInvokable getInvokable() {
		return this.invokable;
	}

	@Override
	public JobID getJobID() {
		return this.owner.getJobID();
	}

	@Override
	public JobVertexID getJobVertexId() {
		return this.owner.getVertexID();
	}

	@Override
	public void run() {
		// quick fail in case the task was cancelled while the thread was started
		if (owner.isCanceledOrFailed()) {
			owner.cancelingDone();
			return;
		}

		try {
			Thread.currentThread().setContextClassLoader(userCodeClassLoader);
			invokable.invoke();

			// Make sure, we enter the catch block when the task has been canceled
			if (owner.isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// Finish the produced partitions
			if (producedPartitions != null) {
				for (IntermediateResultPartition partition : producedPartitions) {
					if (partition != null) {
						partition.finish();
					}
				}
			}

			if (owner.isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// Finally, switch execution state to FINISHED and report to job manager
			if (!owner.markAsFinished()) {
				throw new Exception("Could *not* notify job manager that the task is finished.");
			}
		}
		catch (Throwable t) {
			LOG.error("Error during running invokable: " + t.getMessage(), t);

			if (!owner.isCanceledOrFailed()) {
				// Perform clean up when the task failed and has been not canceled by the user
				try {
					invokable.cancel();
				}
				catch (Throwable t2) {
					LOG.error("Error while canceling the task", t2);
				}
			}

			// if we are already set as cancelled or failed (when failure is triggered externally),
			// mark that the thread is done.
			if (owner.isCanceledOrFailed() || t instanceof CancelTaskException) {
				owner.cancelingDone();
			}
			else {
				// failure from inside the task thread. notify the task of the failure
				owner.markFailed(t);
			}
		}
	}

	/**
	 * Returns the thread, which is assigned to execute the user code.
	 */
	public Thread getExecutingThread() {
		synchronized (this) {
			if (executingThread == null) {
				String name = owner.getTaskNameWithSubtasks();

				if (LOG.isDebugEnabled()) {
					name = name + " (" + owner.getExecutionId() + ")";
				}

				executingThread = new Thread(TASK_THREADS, this, name);
			}

			return executingThread;
		}
	}

	public void cancelExecution() {
		if (!canceled.compareAndSet(false, true)) {
			return;
		}

		LOG.info("Canceling {} ({}).", owner.getTaskNameWithSubtasks(), owner.getExecutionId());

		// Request user code to shut down
		if (invokable != null) {
			try {
				invokable.cancel();
			}
			catch (Throwable e) {
				LOG.error("Error while canceling the task.", e);
			}
		}

		final Thread executingThread = this.executingThread;
		if (executingThread != null) {
			// interrupt the running thread and wait for it to die
			executingThread.interrupt();
			try {
				executingThread.join(5000);
			}
			catch (InterruptedException e) {
			}
			if (!executingThread.isAlive()) {
				return;
			}
			// Continuously interrupt the user thread until it changed to state CANCELED
			while (executingThread != null && executingThread.isAlive()) {
				LOG.warn("Task " + owner.getTaskNameWithSubtasks() + " did not react to cancelling signal. Sending repeated interrupt.");
				if (LOG.isDebugEnabled()) {
					StringBuilder bld = new StringBuilder("Task ").append(owner.getTaskNameWithSubtasks()).append(" is stuck in method:\n");
					StackTraceElement[] stack = executingThread.getStackTrace();
					for (StackTraceElement e : stack) {
						bld.append(e).append('\n');
					}
					LOG.debug(bld.toString());
				}
				executingThread.interrupt();
				try {
					executingThread.join(1000);
				}
				catch (InterruptedException e) {
				}
			}
		}
	}

	@Override
	public ActorRef getJobManager() {
		return jobManager;
	}

	@Override
	public IOManager getIOManager() {
		return ioManager;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return memoryManager;
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		return broadcastVariableManager;
	}

	@Override
	public BufferWriter getWriter(int index) {
		checkElementIndex(index, writers.length, "Illegal environment writer request.");

		return writers[checkElementIndex(index, writers.length)];
	}

	@Override
	public BufferWriter[] getAllWriters() {
		return writers;
	}

	@Override
	public BufferReader getReader(int index) {
		checkElementIndex(index, readers.length, "Illegal environment reader request.");

		return readers[index];
	}

	@Override
	public BufferReader[] getAllReaders() {
		return readers;
	}

	public IntermediateResultPartition[] getProducedPartitions() {
		return producedPartitions;
	}

	public BufferReader getReaderById(IntermediateDataSetID id) {
		return readersById.get(id);
	}

	@Override
	public Configuration getTaskConfiguration() {
		return taskConfiguration;
	}

	@Override
	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	@Override
	public int getNumberOfSubtasks() {
		return owner.getNumberOfSubtasks();
	}

	@Override
	public int getIndexInSubtaskGroup() {
		return owner.getSubtaskIndex();
	}

	@Override
	public String getTaskName() {
		return owner.getTaskName();
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return inputSplitProvider;
	}

	@Override
	public String getTaskNameWithSubtasks() {
		return owner.getTaskNameWithSubtasks();
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return userCodeClassLoader;
	}

	public void addCopyTasksForCacheFile(Map<String, FutureTask<Path>> copyTasks) {
		cacheCopyTasks.putAll(copyTasks);
	}

	public void addCopyTaskForCacheFile(String name, FutureTask<Path> copyTask) {
		cacheCopyTasks.put(name, copyTask);
	}

	@Override
	public Map<String, FutureTask<Path>> getCopyTask() {
		return cacheCopyTasks;
	}
}
