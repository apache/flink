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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.io.network.bufferprovider.LocalBufferPool;
import org.apache.flink.runtime.io.network.bufferprovider.LocalBufferPoolOwner;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.channels.OutputChannel;
import org.apache.flink.runtime.io.network.gates.GateID;
import org.apache.flink.runtime.io.network.gates.InputGate;
import org.apache.flink.runtime.io.network.gates.OutputGate;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;
import org.apache.flink.runtime.taskmanager.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class RuntimeEnvironment implements Environment, BufferProvider, LocalBufferPoolOwner, Runnable {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(RuntimeEnvironment.class);

	private static final ThreadGroup TASK_THREADS = new ThreadGroup("Task Threads");
	
	/** The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds). */
	private static final int SLEEPINTERVAL = 100;
	
	
	
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
	
	
	/** List of output gates created by the task. */
	private final ArrayList<OutputGate> outputGates = new ArrayList<OutputGate>();

	/** List of input gates created by the task. */
	private final ArrayList<InputGate<? extends IOReadableWritable>> inputGates = new ArrayList<InputGate<? extends IOReadableWritable>>();

	/** Unbound input gate IDs which are required for deserializing an environment in the course of an RPC call. */
	private final Queue<GateID> unboundInputGateIDs = new ArrayDeque<GateID>();

	/** The memory manager of the current environment (currently the one associated with the executing TaskManager). */
	private final MemoryManager memoryManager;

	/** The I/O manager of the current environment (currently the one associated with the executing TaskManager). */
	private final IOManager ioManager;

	/** The input split provider that can be queried for new input splits.  */
	private final InputSplitProvider inputSplitProvider;

	
	/** The thread executing the task in the environment. */
	private Thread executingThread;

	/**
	 * The RPC proxy to report accumulators to JobManager
	 */
	private final AccumulatorProtocol accumulatorProtocolProxy;

	private final Map<String,FutureTask<Path>> cacheCopyTasks = new HashMap<String, FutureTask<Path>>();
	
	private LocalBufferPool outputBufferPool;
	
	private AtomicBoolean canceled = new AtomicBoolean();


	public RuntimeEnvironment(Task owner, TaskDeploymentDescriptor tdd,
							ClassLoader userCodeClassLoader,
							MemoryManager memoryManager, IOManager ioManager,
							InputSplitProvider inputSplitProvider,
							AccumulatorProtocol accumulatorProtocolProxy)
		throws Exception
	{
		Preconditions.checkNotNull(owner);
		Preconditions.checkNotNull(memoryManager);
		Preconditions.checkNotNull(ioManager);
		Preconditions.checkNotNull(inputSplitProvider);
		Preconditions.checkNotNull(accumulatorProtocolProxy);
		Preconditions.checkNotNull(userCodeClassLoader);
		
		this.owner = owner;

		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.inputSplitProvider = inputSplitProvider;
		this.accumulatorProtocolProxy = accumulatorProtocolProxy;

		// load and instantiate the invokable class
		this.userCodeClassLoader = userCodeClassLoader;
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

		List<GateDeploymentDescriptor> inGates = tdd.getInputGates();
		List<GateDeploymentDescriptor> outGates = tdd.getOutputGates();
		
		
		if (this.inputGates.size() != inGates.size()) {
			throw new Exception("The number of readers created in 'registerInputOutput()' "
					+ "is different than the number of connected incoming edges in the job graph.");
		}
		if (this.outputGates.size() != outGates.size()) {
			throw new Exception("The number of writers created in 'registerInputOutput()' "
					+ "is different than the number of connected outgoing edges in the job graph.");
		}
		
		for (int i = 0; i < inGates.size(); i++) {
			this.inputGates.get(i).initializeChannels(inGates.get(i));
		}
		for (int i = 0; i < outGates.size(); i++) {
			this.outputGates.get(i).initializeChannels(outGates.get(i));
		}
	}

	/**
	 * Returns the invokable object that represents the Nephele task.
	 *
	 * @return the invokable object that represents the Nephele task
	 */
	public AbstractInvokable getInvokable() {
		return this.invokable;
	}

	@Override
	public JobID getJobID() {
		return this.owner.getJobID();
	}

	@Override
	public GateID getNextUnboundInputGateID() {
		return this.unboundInputGateIDs.poll();
	}

	@Override
	public OutputGate createAndRegisterOutputGate() {
		OutputGate gate = new OutputGate(getJobID(), new GateID(), getNumberOfOutputGates());
		this.outputGates.add(gate);

		return gate;
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
			
			// If there is any unclosed input gate, close it and propagate close operation to corresponding output gate
			closeInputGates();

			// First, close all output gates to indicate no records will be emitted anymore
			requestAllOutputGatesToClose();

			// Wait until all input channels are closed
			waitForInputChannelsToBeClosed();

			// Now we wait until all output channels have written out their data and are closed
			waitForOutputChannelsToBeClosed();
			
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
				} catch (Throwable t2) {
					LOG.error("Error while canceling the task", t2);
				}
			}

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			// if we are already set as cancelled or failed (when failure is triggered externally),
			// mark that the thread is done.
			if (this.owner.isCanceledOrFailed() || t instanceof CancelTaskException) {
				this.owner.cancelingDone();
			}
			else {
				// failure from inside the task thread. notify the task of teh failure
				this.owner.markFailed(t);
			}
		}
		finally {
			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();
		}
	}

	@Override
	public <T extends IOReadableWritable> InputGate<T> createAndRegisterInputGate() {
		InputGate<T> gate = new InputGate<T>(getJobID(), new GateID(), getNumberOfInputGates());
		this.inputGates.add(gate);

		return gate;
	}

	public int getNumberOfOutputGates() {
		return this.outputGates.size();
	}

	@Override
	public int getNumberOfInputGates() {
		return this.inputGates.size();
	}

	@Override
	public int getNumberOfOutputChannels() {
		int numberOfOutputChannels = 0;
		for (int i = 0; i < this.outputGates.size(); ++i) {
			numberOfOutputChannels += this.outputGates.get(i).getNumChannels();
		}

		return numberOfOutputChannels;
	}

	@Override
	public int getNumberOfInputChannels() {
		int numberOfInputChannels = 0;
		for (int i = 0; i < this.inputGates.size(); ++i) {
			numberOfInputChannels += this.inputGates.get(i).getNumberOfInputChannels();
		}

		return numberOfInputChannels;
	}

	/**
	 * Returns the registered input gate with index <code>pos</code>.
	 *
	 * @param pos the index of the input gate to return
	 * @return the input gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public InputGate<? extends IOReadableWritable> getInputGate(final int pos) {
		if (pos < this.inputGates.size()) {
			return this.inputGates.get(pos);
		}

		return null;
	}

	/**
	 * Returns the registered output gate with index <code>pos</code>.
	 *
	 * @param index the index of the output gate to return
	 * @return the output gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public OutputGate getOutputGate(int index) {
		if (index < this.outputGates.size()) {
			return this.outputGates.get(index);
		}

		return null;
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

		LOG.info("Canceling " + owner.getTaskNameWithSubtasks());

		// Request user code to shut down
		if (this.invokable != null) {
			try {
				this.invokable.cancel();
			} catch (Throwable e) {
				LOG.error("Error while cancelling the task.", e);
			}
		}
		
		final Thread executingThread = this.executingThread;
		if (executingThread != null) {
			// interrupt the running thread and wait for it to die
			executingThread.interrupt();
			
			try {
				executingThread.join(5000);
			} catch (InterruptedException e) {}
			
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
				} catch (InterruptedException e) {}
			}
		}
	}

	/**
	 * Blocks until all output channels are closed.
	 *
	 * @throws InterruptedException thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForOutputChannelsToBeClosed() throws InterruptedException {
		// Make sure, we leave this method with an InterruptedException when the task has been canceled
		if (this.owner.isCanceledOrFailed()) {
			return;
		}

		for (OutputGate og : this.outputGates) {
			og.waitForGateToBeClosed();
		}
	}

	/**
	 * Blocks until all input channels are closed.
	 *
	 * @throws IOException          thrown if an error occurred while closing the input channels
	 * @throws InterruptedException thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForInputChannelsToBeClosed() throws IOException, InterruptedException {
		// Wait for disconnection of all output gates
		while (!canceled.get()) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.owner.isCanceledOrFailed()) {
				throw new InterruptedException();
			}

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfInputGates(); i++) {
				final InputGate<? extends IOReadableWritable> eig = this.inputGates.get(i);
				if (!eig.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			}
			else {
				Thread.sleep(SLEEPINTERVAL);
			}
		}
	}

	/**
	 * Closes all input gates which are not already closed.
	 */
	private void closeInputGates() throws IOException, InterruptedException {
		for (int i = 0; i < this.inputGates.size(); i++) {
			final InputGate<? extends IOReadableWritable> eig = this.inputGates.get(i);
			// Important: close must be called on each input gate exactly once
			eig.close();
		}

	}

	/**
	 * Requests all output gates to be closed.
	 */
	private void requestAllOutputGatesToClose() throws IOException, InterruptedException {
		for (int i = 0; i < this.outputGates.size(); i++) {
			this.outputGates.get(i).requestClose();
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

	/**
	 * Releases the allocated resources (particularly buffer) of input and output channels attached to this task. This
	 * method should only be called after the respected task has stopped running.
	 */
	private void releaseAllChannelResources() {
		for (int i = 0; i < this.inputGates.size(); i++) {
			this.inputGates.get(i).releaseAllChannelResources();
		}

		for (int i = 0; i < this.outputGates.size(); i++) {
			this.outputGates.get(i).releaseAllChannelResources();
		}
	}

	@Override
	public Set<ChannelID> getOutputChannelIDs() {
		Set<ChannelID> ids = new HashSet<ChannelID>();

		for (OutputGate gate : this.outputGates) {
			for (OutputChannel channel : gate.channels()) {
				ids.add(channel.getID());
			}
		}

		return Collections.unmodifiableSet(ids);
	}

	@Override
	public Set<ChannelID> getInputChannelIDs() {
		final Set<ChannelID> inputChannelIDs = new HashSet<ChannelID>();

		final Iterator<InputGate<? extends IOReadableWritable>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {

			final InputGate<? extends IOReadableWritable> outputGate = gateIterator.next();
			for (int i = 0; i < outputGate.getNumberOfInputChannels(); ++i) {
				inputChannelIDs.add(outputGate.getInputChannel(i).getID());
			}
		}

		return Collections.unmodifiableSet(inputChannelIDs);
	}

	@Override
	public Set<GateID> getInputGateIDs() {
		final Set<GateID> inputGateIDs = new HashSet<GateID>();

		final Iterator<InputGate<? extends IOReadableWritable>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {
			inputGateIDs.add(gateIterator.next().getGateID());
		}

		return Collections.unmodifiableSet(inputGateIDs);
	}

	@Override
	public Set<GateID> getOutputGateIDs() {
		final Set<GateID> outputGateIDs = new HashSet<GateID>();

		final Iterator<OutputGate> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {
			outputGateIDs.add(gateIterator.next().getGateID());
		}

		return Collections.unmodifiableSet(outputGateIDs);
	}


	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {
		OutputGate outputGate = null;
		final Iterator<OutputGate> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {
			final OutputGate candidateGate = gateIterator.next();
			if (candidateGate.getGateID().equals(gateID)) {
				outputGate = candidateGate;
				break;
			}
		}

		if (outputGate == null) {
			throw new IllegalArgumentException("Cannot find output gate with ID " + gateID);
		}

		final Set<ChannelID> outputChannelIDs = new HashSet<ChannelID>();

		for (int i = 0; i < outputGate.getNumChannels(); ++i) {
			outputChannelIDs.add(outputGate.getChannel(i).getID());
		}

		return Collections.unmodifiableSet(outputChannelIDs);
	}


	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {
		InputGate<? extends IOReadableWritable> inputGate = null;
		final Iterator<InputGate<? extends IOReadableWritable>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {
			final InputGate<? extends IOReadableWritable> candidateGate = gateIterator.next();
			if (candidateGate.getGateID().equals(gateID)) {
				inputGate = candidateGate;
				break;
			}
		}

		if (inputGate == null) {
			throw new IllegalArgumentException("Cannot find input gate with ID " + gateID);
		}

		final Set<ChannelID> inputChannelIDs = new HashSet<ChannelID>();

		for (int i = 0; i < inputGate.getNumberOfInputChannels(); ++i) {
			inputChannelIDs.add(inputGate.getInputChannel(i).getID());
		}

		return Collections.unmodifiableSet(inputChannelIDs);
	}

	public List<OutputGate> outputGates() {
		return this.outputGates;
	}

	public List<InputGate<? extends IOReadableWritable>> inputGates() {
		return this.inputGates;
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

	@Override
	public BufferProvider getOutputBufferProvider() {
		return this;
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                            BufferProvider methods
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Buffer requestBuffer(int minBufferSize) throws IOException {
		return this.outputBufferPool.requestBuffer(minBufferSize);
	}

	@Override
	public Buffer requestBufferBlocking(int minBufferSize) throws IOException, InterruptedException {
		return this.outputBufferPool.requestBufferBlocking(minBufferSize);
	}

	@Override
	public int getBufferSize() {
		return this.outputBufferPool.getBufferSize();
	}

	@Override
	public BufferAvailabilityRegistration registerBufferAvailabilityListener(BufferAvailabilityListener listener) {
		return this.outputBufferPool.registerBufferAvailabilityListener(listener);
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                       LocalBufferPoolOwner methods
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public int getNumberOfChannels() {
		return getNumberOfOutputChannels();
	}

	@Override
	public void setDesignatedNumberOfBuffers(int numBuffers) {
		this.outputBufferPool.setNumDesignatedBuffers(numBuffers);
	}

	@Override
	public void clearLocalBufferPool() {
		this.outputBufferPool.destroy();
	}

	@Override
	public void registerGlobalBufferPool(GlobalBufferPool globalBufferPool) {
		if (this.outputBufferPool == null) {
			this.outputBufferPool = new LocalBufferPool(globalBufferPool, 1);
		}
	}

	@Override
	public void logBufferUtilization() {
		LOG.info(String.format("\t%s: %d available, %d requested, %d designated",
				owner.getTaskNameWithSubtasks(),
				this.outputBufferPool.numAvailableBuffers(),
				this.outputBufferPool.numRequestedBuffers(),
				this.outputBufferPool.numDesignatedBuffers()));
	}

	@Override
	public void reportAsynchronousEvent() {
		this.outputBufferPool.reportAsynchronousEvent();
	}
}
