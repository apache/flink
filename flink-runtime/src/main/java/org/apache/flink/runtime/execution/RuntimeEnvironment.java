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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.deployment.ChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.IntermediateResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;
import org.apache.flink.runtime.taskmanager.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

public class RuntimeEnvironment implements Environment, Runnable {

	/** The log object used for debugging. */
	private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeEnvironment.class);

	private static final ThreadGroup TASK_THREADS = new ThreadGroup("Task Threads");

	// --------------------------------------------------------------------------------------------

	/** The task that owns this environment */
	private final Task owner;

	/** The job configuration encapsulated in the environment object. */
	private final Configuration jobConfiguration;

	/** The task configuration encapsulated in the environment object. */
	private final Configuration taskConfiguration;

	/** ClassLoader for all user code classes */
	private final ClassLoader userCodeClassLoader;

	/** Class of the task to run in this environment. */
	private final Class<? extends AbstractInvokable> invokableClass;

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

	/** The RPC proxy to report accumulators to JobManager. */
	private final AccumulatorProtocol accumulatorProtocolProxy;

	private final Map<String, FutureTask<Path>> cacheCopyTasks = new HashMap<String, FutureTask<Path>>();

	private final IntermediateResultPartitionManager partitionManager;

	private final TaskEventDispatcher taskEventDispatcher;

	private AtomicBoolean canceled = new AtomicBoolean();

	private BufferWriter[] writers;

	private BufferReader[] readers;

	private IntermediateResultPartition[] producedPartitions;

	public RuntimeEnvironment(
			Task owner, TaskDeploymentDescriptor tdd, ClassLoader userCodeClassLoader,
			MemoryManager memoryManager, IOManager ioManager, BufferPoolFactory bufferPoolFactory,
			IntermediateResultPartitionManager partitionManager, TaskEventDispatcher taskEventDispatcher,
			InputSplitProvider inputSplitProvider, AccumulatorProtocol accumulatorProtocolProxy) throws Exception {

		this.owner = checkNotNull(owner);

		this.memoryManager = checkNotNull(memoryManager);
		this.ioManager = checkNotNull(ioManager);
		this.inputSplitProvider = checkNotNull(inputSplitProvider);
		this.accumulatorProtocolProxy = checkNotNull(accumulatorProtocolProxy);
		this.partitionManager = checkNotNull(partitionManager);
		this.taskEventDispatcher = checkNotNull(taskEventDispatcher);

		boolean success = false;

		try {
			// ----------------------------------------------------------------
			// Produced intermediate result partitions and writers
			// ----------------------------------------------------------------
			List<IntermediateResultPartitionDeploymentDescriptor> irpdd = tdd.getProducedPartitions();

			this.producedPartitions = new IntermediateResultPartition[irpdd.size()];
			this.writers = new BufferWriter[irpdd.size()];

			for (int i = 0; i < producedPartitions.length; i++) {
				IntermediateResultPartitionDeploymentDescriptor irp = irpdd.get(i);

				BufferPool bufferPool = bufferPoolFactory.createBufferPool(irp.getGdd().getChannels().size(), false);

				producedPartitions[i] = new IntermediateResultPartition(getJobID(), bufferPool, irp);

				writers[i] = new BufferWriter(producedPartitions[i]);
			}

			registerAllWritersWithTaskEventDispatcher();

			// ----------------------------------------------------------------
			// Consumed intermediate result partition readers
			// ----------------------------------------------------------------
			List<GateDeploymentDescriptor> inGates = tdd.getInputGates();

			this.readers = new BufferReader[inGates.size()];

			int numReaders = inGates.size();

			for (int i = 0; i < numReaders; i++) {
				GateDeploymentDescriptor gdd = inGates.get(i);

				List<ChannelDeploymentDescriptor> cdds = gdd.getChannels();

				InputChannel[] inputChannels = new LocalInputChannel[cdds.size()];

				BufferPool bufferPool = bufferPoolFactory.createBufferPool(inputChannels.length + 2, true);
				BufferReader reader = new BufferReader(inputChannels, bufferPool);

				for (int j = 0; j < cdds.size(); j++) {
					inputChannels[j] = new LocalInputChannel(j, null, reader, partitionManager, taskEventDispatcher);
				}

				readers[i] = reader;
			}

			// ----------------------------------------------------------------
			// Invokable setup
			// ----------------------------------------------------------------
			// Note: This has to be done *after* the readers and writers have
			// been setup, because the invokable relies on them for I/O.
			// ----------------------------------------------------------------
			// load and instantiate the invokable class
			this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
			try {
				final String className = tdd.getInvokableClassName();
				this.invokableClass = Class.forName(className, true, userCodeClassLoader).asSubclass(AbstractInvokable.class);
			}
			catch (Throwable t) {
				throw new Exception("Could not load invokable class.", t);
			}

			try {
				this.invokable = this.invokableClass.newInstance();
			}
			catch (Throwable t) {
				throw new Exception("Could not instantiate the invokable class.", t);
			}

			this.jobConfiguration = tdd.getJobConfiguration();
			this.taskConfiguration = tdd.getTaskConfiguration();

			this.invokable.setEnvironment(this);
			this.invokable.registerInputOutput();

			success = true;
		}
		finally {
			if (!success) {
				unregisterAndDiscardProducedPartitions();

				releaseAllReaderResources();

				unregisterAllWritersFromTaskEventDispatcher();
			}
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
	public void run() {
		// quick fail in case the task was cancelled while the tread was started
		if (owner.isCanceledOrFailed()) {
			owner.cancelingDone();
			return;
		}

		try {
			Thread.currentThread().setContextClassLoader(userCodeClassLoader);
			this.invokable.invoke();

			// Make sure, we enter the catch block when the task has been canceled
			if (this.owner.isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			finishProducedPartitions();

			releaseAllReaderResources();

			unregisterAllWritersFromTaskEventDispatcher();

			if (this.owner.isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// Finally, switch execution state to FINISHED and report to job manager
			if (!owner.markAsFinished()) {
				throw new Exception("Could notify job manager that the task is finished.");
			}
		}
		catch (Throwable t) {
			if (!this.owner.isCanceledOrFailed()) {

				// Perform clean up when the task failed and has been not canceled by the user
				try {
					this.invokable.cancel();
				}
				catch (Throwable t2) {
					LOGGER.error("Error while canceling the task", t2);
				}
			}

			unregisterAndDiscardProducedPartitions();

			releaseAllReaderResources();

			unregisterAllWritersFromTaskEventDispatcher();

			// if we are already set as cancelled or failed (when failure is triggered externally),
			// mark that the thread is done.
			if (this.owner.isCanceledOrFailed() || t instanceof CancelTaskException) {
				this.owner.cancelingDone();
			}
			else {
				// failure from inside the task thread. notify the task of the failure
				this.owner.markFailed(t);
			}
		}
	}

	/**
	 * Returns the thread which is assigned to execute the user code.
	 *
	 * @return the thread which is assigned to execute the user code
	 */
	public Thread getExecutingThread() {
		synchronized (this) {

			if (this.executingThread == null) {
				String name = owner.getTaskNameWithSubtasks();
				this.executingThread = new Thread(TASK_THREADS, this, name);
			}

			return this.executingThread;
		}
	}

	public void cancelExecution() {
		if (!canceled.compareAndSet(false, true)) {
			return;
		}

		LOGGER.info("Canceling " + owner.getTaskNameWithSubtasks());

		// Request user code to shut down
		if (this.invokable != null) {
			try {
				this.invokable.cancel();
			}
			catch (Throwable e) {
				LOGGER.error("Error while cancelling the task.", e);
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
				LOGGER.warn("Task " + owner.getTaskNameWithSubtasks() + " did not react to cancelling signal. Sending repeated interrupt.");

				if (LOGGER.isDebugEnabled()) {
					StringBuilder bld = new StringBuilder("Task ").append(owner.getTaskNameWithSubtasks()).append(" is stuck in method:\n");

					StackTraceElement[] stack = executingThread.getStackTrace();
					for (StackTraceElement e : stack) {
						bld.append(e).append('\n');
					}
					LOGGER.debug(bld.toString());
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
	public IOManager getIOManager() {
		return this.ioManager;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	@Override
	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	@Override
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	@Override
	public int getCurrentNumberOfSubtasks() {
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
		return this.inputSplitProvider;
	}

	public IntermediateResultPartition[] outputGates() {
		return producedPartitions;
	}

	public BufferReader[] inputGates() {
		return readers;
	}

	@Override
	public AccumulatorProtocol getAccumulatorProtocolProxy() {
		return accumulatorProtocolProxy;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return userCodeClassLoader;
	}

	public void addCopyTasksForCacheFile(Map<String, FutureTask<Path>> copyTasks) {
		this.cacheCopyTasks.putAll(copyTasks);
	}

	public void addCopyTaskForCacheFile(String name, FutureTask<Path> copyTask) {
		this.cacheCopyTasks.put(name, copyTask);
	}

	@Override
	public Map<String, FutureTask<Path>> getCopyTask() {
		return this.cacheCopyTasks;
	}

	public BufferWriter getWriter(int index) {
		return writers[checkElementIndex(index, writers.length)];
	}

	public BufferWriter[] getAllWriters() {
		return writers;
	}

	@Override
	public BufferReader getReader(int index) {
		return readers[index];
	}

	@Override
	public BufferReader[] getAllReaders() {
		return new BufferReader[0];
	}

	// ------------------------------------------------------------------------
	// Resource release
	// ------------------------------------------------------------------------

	private void registerAllWritersWithTaskEventDispatcher() {
		if (writers != null) {
			for (BufferWriter writer : writers) {
				if (writer != null) {
					taskEventDispatcher.register(writer.getPartitionId(), writer);
				}
			}
		}
	}

	private void unregisterAllWritersFromTaskEventDispatcher() {
		if (writers != null) {
			for (BufferWriter writer : writers) {
				if (writer != null) {
					taskEventDispatcher.unregister(writer.getPartitionId());
				}
			}
		}
	}

	private void releaseAllReaderResources() {
		if (readers != null) {
			for (BufferReader reader : readers) {
				if (reader != null) {
					reader.releaseAllResources();
				}
			}
		}
	}

	private void finishProducedPartitions() throws IOException {
		if (producedPartitions != null) {
			for (IntermediateResultPartition partition : producedPartitions) {
				if (partition != null) {
					partition.finish();
				}
			}
		}
	}

	private void unregisterAndDiscardProducedPartitions() {
		if (producedPartitions != null) {
			for (IntermediateResultPartition partition : producedPartitions) {
				partitionManager.unregisterIntermediateResultPartition(partition);

				partition.discard();
			}
		}
	}
}
