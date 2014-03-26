/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.execution;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.AccumulatorProtocol;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.runtime.io.channels.OutputChannel;
import eu.stratosphere.runtime.io.gates.GateID;
import eu.stratosphere.runtime.io.gates.InputGate;
import eu.stratosphere.runtime.io.gates.OutputGate;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.bufferprovider.GlobalBufferPool;
import eu.stratosphere.runtime.io.network.bufferprovider.LocalBufferPool;
import eu.stratosphere.runtime.io.network.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.FutureTask;

/**
 * The user code of every Nephele task runs inside a <code>RuntimeEnvironment</code> object. The environment provides
 * important services to the task. It keeps track of setting up the communication channels and provides access to input
 * splits, memory manager, etc.
 * <p/>
 * This class is thread-safe.
 */
public class RuntimeEnvironment implements Environment, BufferProvider, LocalBufferPoolOwner, Runnable {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(RuntimeEnvironment.class);

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 100;

	/**
	 * List of output gates created by the task.
	 */
	private final List<OutputGate> outputGates = new CopyOnWriteArrayList<OutputGate>();

	/**
	 * List of input gates created by the task.
	 */
	private final List<InputGate<? extends IOReadableWritable>> inputGates = new CopyOnWriteArrayList<InputGate<? extends IOReadableWritable>>();

	/**
	 * Queue of unbound output gate IDs which are required for deserializing an environment in the course of an RPC
	 * call.
	 */
	private final Queue<GateID> unboundOutputGateIDs = new ArrayDeque<GateID>();

	/**
	 * Queue of unbound input gate IDs which are required for deserializing an environment in the course of an RPC
	 * call.
	 */
	private final Queue<GateID> unboundInputGateIDs = new ArrayDeque<GateID>();

	/**
	 * The memory manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private final MemoryManager memoryManager;

	/**
	 * The io manager of the current environment (currently the one associated with the executing TaskManager).
	 */
	private final IOManager ioManager;

	/**
	 * Class of the task to run in this environment.
	 */
	private final Class<? extends AbstractInvokable> invokableClass;

	/**
	 * Instance of the class to be run in this environment.
	 */
	private final AbstractInvokable invokable;

	/**
	 * The ID of the job this task belongs to.
	 */
	private final JobID jobID;

	/**
	 * The job configuration encapsulated in the environment object.
	 */
	private final Configuration jobConfiguration;

	/**
	 * The task configuration encapsulated in the environment object.
	 */
	private final Configuration taskConfiguration;

	/**
	 * The input split provider that can be queried for new input splits.
	 */
	private final InputSplitProvider inputSplitProvider;

	/**
	 * The observer object for the task's execution.
	 */
	private volatile ExecutionObserver executionObserver = null;
	
	/**
	 * The thread executing the task in the environment.
	 */
	private volatile Thread executingThread;

	/**
	 * The RPC proxy to report accumulators to JobManager
	 */
	private AccumulatorProtocol accumulatorProtocolProxy = null;

	/**
	 * The index of this subtask in the subtask group.
	 */
	private final int indexInSubtaskGroup;

	/**
	 * The current number of subtasks the respective task is split into.
	 */
	private final int currentNumberOfSubtasks;

	/**
	 * The name of the task running in this environment.
	 */
	private final String taskName;

	private LocalBufferPool outputBufferPool;

	private final Map<String,FutureTask<Path>> cacheCopyTasks;
	
	private volatile boolean canceled;

	/**
	 * Constructs a runtime environment from a task deployment description.
	 * 
	 * @param tdd
	 *        the task deployment description
	 * @param memoryManager
	 *        the task manager's memory manager component
	 * @param ioManager
	 *        the task manager's I/O manager component
	 * @param inputSplitProvider
	 *        the input split provider for this environment
	 * @throws Exception
	 *         thrown if an error occurs while instantiating the invokable class
	 */
	public RuntimeEnvironment(final TaskDeploymentDescriptor tdd,
							final MemoryManager memoryManager, final IOManager ioManager,
							final InputSplitProvider inputSplitProvider,
							AccumulatorProtocol accumulatorProtocolProxy, Map<String, FutureTask<Path>> cpTasks) throws Exception {

		this.jobID = tdd.getJobID();
		this.taskName = tdd.getTaskName();
		this.invokableClass = tdd.getInvokableClass();
		this.jobConfiguration = tdd.getJobConfiguration();
		this.taskConfiguration = tdd.getTaskConfiguration();
		this.indexInSubtaskGroup = tdd.getIndexInSubtaskGroup();
		this.currentNumberOfSubtasks = tdd.getCurrentNumberOfSubtasks();
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.inputSplitProvider = inputSplitProvider;
		this.accumulatorProtocolProxy = accumulatorProtocolProxy;
		this.cacheCopyTasks = cpTasks;

		this.invokable = this.invokableClass.newInstance();
		this.invokable.setEnvironment(this);
		this.invokable.registerInputOutput();

		int numOutputGates = tdd.getNumberOfOutputGateDescriptors();

		for (int i = 0; i < numOutputGates; ++i) {
			this.outputGates.get(i).initializeChannels(tdd.getOutputGateDescriptor(i));
		}

		int numInputGates = tdd.getNumberOfInputGateDescriptors();

		for (int i = 0; i < numInputGates; i++) {
			this.inputGates.get(i).initializeChannels(tdd.getInputGateDescriptor(i));
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
		return this.jobID;
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
		if (invokable == null) {
			LOG.fatal("ExecutionEnvironment has no Invokable set");
		}

		// Now the actual program starts to run
		changeExecutionState(ExecutionState.RUNNING, null);

		// If the task has been canceled in the mean time, do not even start it
		if (this.executionObserver.isCanceled()) {
			changeExecutionState(ExecutionState.CANCELED, null);
			return;
		}

		try {
			ClassLoader cl = LibraryCacheManager.getClassLoader(jobID);
			Thread.currentThread().setContextClassLoader(cl);
			this.invokable.invoke();

			// Make sure, we enter the catch block when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}
		} catch (Throwable t) {
			if (!this.executionObserver.isCanceled()) {

				// Perform clean up when the task failed and has been not canceled by the user
				try {
					this.invokable.cancel();
				} catch (Throwable t2) {
					LOG.error(StringUtils.stringifyException(t2));
				}
			}

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled() || t instanceof CancelTaskException) {
				changeExecutionState(ExecutionState.CANCELED, null);
			}
			else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(t));
			}

			return;
		}

		// Task finished running, but there may be unconsumed output data in some of the channels
		changeExecutionState(ExecutionState.FINISHING, null);

		try {
			// If there is any unclosed input gate, close it and propagate close operation to corresponding output gate
			closeInputGates();

			// First, close all output gates to indicate no records will be emitted anymore
			requestAllOutputGatesToClose();

			// Wait until all input channels are closed
			waitForInputChannelsToBeClosed();

			// Now we wait until all output channels have written out their data and are closed
			waitForOutputChannelsToBeClosed();
		} catch (Throwable t) {

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled() || t instanceof CancelTaskException) {
				changeExecutionState(ExecutionState.CANCELED, null);
			}
			else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(t));
			}

			return;
		}

		// Release all resources that may currently be allocated by the individual channels
		releaseAllChannelResources();

		// Finally, switch execution state to FINISHED and report to job manager
		changeExecutionState(ExecutionState.FINISHED, null);
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
				if (this.taskName == null) {
					this.executingThread = new Thread(this);
				}
				else {
					this.executingThread = new Thread(this, getTaskNameWithIndex());
				}
			}

			return this.executingThread;
		}
	}
	
	public void cancelExecution() {
		canceled = true;

		LOG.info("Canceling " + getTaskNameWithIndex());

		// Request user code to shut down
		if (this.invokable != null) {
			try {
				this.invokable.cancel();
			} catch (Throwable e) {
				LOG.error("Error while cancelling the task.", e);
			}
		}
		
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
			LOG.warn("Task " + getTaskName() + " did not react to cancelling signal. Sending repeated interrupt.");

			if (LOG.isDebugEnabled()) {
				StringBuilder bld = new StringBuilder("Task ").append(getTaskName()).append(" is stuck in method:\n");
				
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

	/**
	 * Blocks until all output channels are closed.
	 *
	 * @throws IOException          thrown if an error occurred while closing the output channels
	 * @throws InterruptedException thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForOutputChannelsToBeClosed() throws InterruptedException {
		// Make sure, we leave this method with an InterruptedException when the task has been canceled
		if (this.executionObserver.isCanceled()) {
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
		while (!canceled) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.executionObserver.isCanceled()) {
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
		return this.currentNumberOfSubtasks;
	}

	@Override
	public int getIndexInSubtaskGroup() {
		return this.indexInSubtaskGroup;
	}

	private void changeExecutionState(final ExecutionState newExecutionState, final String optionalMessage) {
		if (this.executionObserver != null) {
			this.executionObserver.executionStateChanged(newExecutionState, optionalMessage);
		}
	}

	@Override
	public String getTaskName() {
		return this.taskName;
	}

	/**
	 * Returns the name of the task with its index in the subtask group and the total number of subtasks.
	 *
	 * @return the name of the task with its index in the subtask group and the total number of subtasks
	 */
	public String getTaskNameWithIndex() {
		return String.format("%s (%d/%d)", this.taskName, getIndexInSubtaskGroup() + 1, getCurrentNumberOfSubtasks());
	}

	/**
	 * Sets the execution observer for this environment.
	 *
	 * @param executionObserver the execution observer for this environment
	 */
	public void setExecutionObserver(final ExecutionObserver executionObserver) {
		this.executionObserver = executionObserver;
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	@Override
	public void userThreadStarted(final Thread userThread) {
		if (this.executionObserver != null) {
			this.executionObserver.userThreadStarted(userThread);
		}
	}

	@Override
	public void userThreadFinished(final Thread userThread) {
		if (this.executionObserver != null) {
			this.executionObserver.userThreadFinished(userThread);
		}
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
				getTaskNameWithIndex(),
				this.outputBufferPool.numAvailableBuffers(),
				this.outputBufferPool.numRequestedBuffers(),
				this.outputBufferPool.numDesignatedBuffers()));
	}

	@Override
	public void reportAsynchronousEvent() {
		this.outputBufferPool.reportAsynchronousEvent();
	}
}
