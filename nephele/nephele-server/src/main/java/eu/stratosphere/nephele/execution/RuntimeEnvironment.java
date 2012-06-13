/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.execution;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.deployment.ChannelDeploymentDescriptor;
import eu.stratosphere.nephele.deployment.GateDeploymentDescriptor;
import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializerFactory;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The user code of every Nephele task runs inside a <code>RuntimeEnvironment</code> object. The environment provides
 * important services to the task. It keeps track of setting up the communication channels and provides access to input
 * splits, memory manager, etc.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class RuntimeEnvironment implements Environment, Runnable {

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
	private final List<OutputGate<? extends Record>> outputGates = new CopyOnWriteArrayList<OutputGate<? extends Record>>();

	/**
	 * List of input gates created by the task.
	 */
	private final List<InputGate<? extends Record>> inputGates = new CopyOnWriteArrayList<InputGate<? extends Record>>();

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
	 * The thread executing the task in the environment.
	 */
	private volatile Thread executingThread = null;

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

	/**
	 * Creates a new runtime environment object which contains the runtime information for the encapsulated Nephele
	 * task.
	 * 
	 * @param jobID
	 *        the ID of the original Nephele job
	 * @param taskName
	 *        the name of task running in this environment
	 * @param invokableClass
	 *        invokableClass the class that should be instantiated as a Nephele task
	 * @param taskConfiguration
	 *        the configuration object which was attached to the original {@link JobVertex}
	 * @param jobConfiguration
	 *        the configuration object which was attached to the original {@link JobGraph}
	 * @throws Exception
	 *         thrown if an error occurs while instantiating the invokable class
	 */
	public RuntimeEnvironment(final JobID jobID, final String taskName,
			final Class<? extends AbstractInvokable> invokableClass, final Configuration taskConfiguration,
			final Configuration jobConfiguration) throws Exception {

		this.jobID = jobID;
		this.taskName = taskName;
		this.invokableClass = invokableClass;
		this.taskConfiguration = taskConfiguration;
		this.jobConfiguration = jobConfiguration;
		this.indexInSubtaskGroup = 0;
		this.currentNumberOfSubtasks = 0;
		this.memoryManager = null;
		this.ioManager = null;
		this.inputSplitProvider = null;

		this.invokable = this.invokableClass.newInstance();
		this.invokable.setEnvironment(this);
		this.invokable.registerInputOutput();
	}

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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public RuntimeEnvironment(final TaskDeploymentDescriptor tdd, final MemoryManager memoryManager,
			final IOManager ioManager, final InputSplitProvider inputSplitProvider) throws Exception {

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

		this.invokable = this.invokableClass.newInstance();
		this.invokable.setEnvironment(this);
		this.invokable.registerInputOutput();

		if (!this.unboundOutputGateIDs.isEmpty() && LOG.isErrorEnabled()) {
			LOG.error("Inconsistency: " + this.unboundOutputGateIDs.size() + " unbound output gate IDs left");
		}

		if (!this.unboundInputGateIDs.isEmpty() && LOG.isErrorEnabled()) {
			LOG.error("Inconsistency: " + this.unboundInputGateIDs.size() + " unbound output gate IDs left");
		}

		final int noogdd = tdd.getNumberOfOutputGateDescriptors();
		for (int i = 0; i < noogdd; ++i) {
			final GateDeploymentDescriptor gdd = tdd.getOutputGateDescriptor(i);
			final OutputGate og = this.outputGates.get(i);
			final ChannelType channelType = gdd.getChannelType();
			final CompressionLevel compressionLevel = gdd.getCompressionLevel();
			og.setChannelType(channelType);

			final int nocdd = gdd.getNumberOfChannelDescriptors();
			for (int j = 0; j < nocdd; ++j) {

				final ChannelDeploymentDescriptor cdd = gdd.getChannelDescriptor(j);
				switch (channelType) {
				case FILE:
					og.createFileOutputChannel(og, cdd.getOutputChannelID(), cdd.getInputChannelID(), compressionLevel);
					break;
				case NETWORK:
					og.createNetworkOutputChannel(og, cdd.getOutputChannelID(), cdd.getInputChannelID(),
						compressionLevel);
					break;
				case INMEMORY:
					og.createInMemoryOutputChannel(og, cdd.getOutputChannelID(), cdd.getInputChannelID(),
						compressionLevel);
					break;
				default:
					throw new IllegalStateException("Unknown channel type");
				}
			}
		}

		final int noigdd = tdd.getNumberOfInputGateDescriptors();
		for (int i = 0; i < noigdd; ++i) {
			final GateDeploymentDescriptor gdd = tdd.getInputGateDescriptor(i);
			final InputGate ig = this.inputGates.get(i);
			final ChannelType channelType = gdd.getChannelType();
			final CompressionLevel compressionLevel = gdd.getCompressionLevel();
			ig.setChannelType(channelType);

			final int nicdd = gdd.getNumberOfChannelDescriptors();
			for (int j = 0; j < nicdd; ++j) {

				final ChannelDeploymentDescriptor cdd = gdd.getChannelDescriptor(j);
				switch (channelType) {
				case FILE:
					ig.createFileInputChannel(ig, cdd.getInputChannelID(), cdd.getOutputChannelID(), compressionLevel);
					break;
				case NETWORK:
					ig.createNetworkInputChannel(ig, cdd.getInputChannelID(), cdd.getOutputChannelID(),
						compressionLevel);
					break;
				case INMEMORY:
					ig.createInMemoryInputChannel(ig, cdd.getInputChannelID(), cdd.getOutputChannelID(),
						compressionLevel);
					break;
				default:
					throw new IllegalStateException("Unknown channel type");
				}
			}
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getNextUnboundInputGateID() {

		return this.unboundInputGateIDs.poll();
	}

	/**
	 * {@inheritDoc}
	 */
	public GateID getNextUnboundOutputGateID() {

		return this.unboundOutputGateIDs.poll();
	}

	/**
	 * {@inheritDoc}
	 */
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

			// Activate input channels
			activateInputChannels();

			this.invokable.invoke();

			// Make sure, we enter the catch block when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

		} catch (Exception e) {

			if (!this.executionObserver.isCanceled()) {

				// Perform clean up when the task failed and has been not canceled by the user
				try {
					this.invokable.cancel();
				} catch (Exception e2) {
					LOG.error(StringUtils.stringifyException(e2));
				}
			}

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
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
		} catch (Exception e) {

			// Release all resources that may currently be allocated by the individual channels
			releaseAllChannelResources();

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Release all resources that may currently be allocated by the individual channels
		releaseAllChannelResources();

		// Finally, switch execution state to FINISHED and report to job manager
		changeExecutionState(ExecutionState.FINISHED, null);
	}

	/**
	 * Activates all of the task's input channels.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while transmitting one of the activation requests to the corresponding
	 *         output channels
	 * @throws InterruptedException
	 *         throws if the task is interrupted while waiting for the activation process to complete
	 */
	private void activateInputChannels() throws IOException, InterruptedException {

		for (int i = 0; i < getNumberOfInputGates(); ++i) {
			this.inputGates.get(i).activateInputChannels();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> OutputGate<T> createOutputGate(final GateID gateID, Class<T> outputClass,
			final ChannelSelector<T> selector, final boolean isBroadcast)
	{
		final RuntimeOutputGate<T> rog = new RuntimeOutputGate<T>(getJobID(), gateID, outputClass,
															getNumberOfOutputGates(), selector, isBroadcast);
		return rog;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> InputGate<T> createInputGate(final GateID gateID,
										final RecordDeserializerFactory<T> deserializerFactory)
	{
		final RuntimeInputGate<T> rig = new RuntimeInputGate<T>(getJobID(), gateID, deserializerFactory, getNumberOfInputGates());
		return rig;
	}

	@Override
	public void registerOutputGate(OutputGate<? extends Record> outputGate) {

		this.outputGates.add(outputGate);
	}

	@Override
	public void registerInputGate(InputGate<? extends Record> inputGate) {

		this.inputGates.add(inputGate);
	}

	/**
	 * {@inheritDoc}
	 */
	public int getNumberOfOutputGates() {
		return this.outputGates.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {
		return this.inputGates.size();
	}

	/**
	 * Returns the registered input gate with index <code>pos</code>.
	 * 
	 * @param pos
	 *        the index of the input gate to return
	 * @return the input gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public InputGate<? extends Record> getInputGate(final int pos) {
		if (pos < this.inputGates.size()) {
			return this.inputGates.get(pos);
		}

		return null;
	}

	/**
	 * Returns the registered output gate with index <code>pos</code>.
	 * 
	 * @param pos
	 *        the index of the output gate to return
	 * @return the output gate at index <code>pos</code> or <code>null</code> if no such index exists
	 */
	public OutputGate<? extends Record> getOutputGate(final int pos) {
		if (pos < this.outputGates.size()) {
			return this.outputGates.get(pos);
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
				} else {
					this.executingThread = new Thread(this, getTaskNameWithIndex());
				}
			}

			return this.executingThread;
		}
	}

	/**
	 * Blocks until all output channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the output channels
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForOutputChannelsToBeClosed() throws IOException, InterruptedException {

		// Wait for disconnection of all output gates
		while (true) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfOutputGates(); i++) {
				final OutputGate<? extends Record> og = this.outputGates.get(i);
				if (!og.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				Thread.sleep(SLEEPINTERVAL);
			}
		}
	}

	/**
	 * Blocks until all input channels are closed.
	 * 
	 * @throws IOException
	 *         thrown if an error occurred while closing the input channels
	 * @throws InterruptedException
	 *         thrown if the thread waiting for the channels to be closed is interrupted
	 */
	private void waitForInputChannelsToBeClosed() throws IOException, InterruptedException {

		// Wait for disconnection of all output gates
		while (true) {

			// Make sure, we leave this method with an InterruptedException when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

			boolean allClosed = true;
			for (int i = 0; i < getNumberOfInputGates(); i++) {
				final InputGate<? extends Record> eig = this.inputGates.get(i);
				if (!eig.isClosed()) {
					allClosed = false;
				}
			}

			if (allClosed) {
				break;
			} else {
				Thread.sleep(SLEEPINTERVAL);
			}
		}
	}

	/**
	 * Closes all input gates which are not already closed.
	 */
	private void closeInputGates() throws IOException, InterruptedException {

		for (int i = 0; i < this.inputGates.size(); i++) {
			final InputGate<? extends Record> eig = this.inputGates.get(i);
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {
		return this.ioManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCurrentNumberOfSubtasks() {

		return this.currentNumberOfSubtasks;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndexInSubtaskGroup() {

		return this.indexInSubtaskGroup;
	}

	private void changeExecutionState(final ExecutionState newExecutionState, final String optionalMessage) {

		if (this.executionObserver != null) {
			this.executionObserver.executionStateChanged(newExecutionState, optionalMessage);
		}
	}

	/**
	 * {@inheritDoc}
	 */
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

		return this.taskName + " (" + (getIndexInSubtaskGroup() + 1) + "/"
			+ getCurrentNumberOfSubtasks() + ")";
	}

	/**
	 * Sets the execution observer for this environment.
	 * 
	 * @param executionObserver
	 *        the execution observer for this environment
	 */
	public void setExecutionObserver(final ExecutionObserver executionObserver) {
		this.executionObserver = executionObserver;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		if (this.executionObserver != null) {
			this.executionObserver.userThreadStarted(userThread);
		}
	}

	/**
	 * {@inheritDoc}
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDs() {

		final Set<ChannelID> outputChannelIDs = new HashSet<ChannelID>();

		final Iterator<OutputGate<? extends Record>> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {

			final OutputGate<? extends Record> outputGate = gateIterator.next();
			for (int i = 0; i < outputGate.getNumberOfOutputChannels(); ++i) {
				outputChannelIDs.add(outputGate.getOutputChannel(i).getID());
			}
		}

		return Collections.unmodifiableSet(outputChannelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDs() {

		final Set<ChannelID> inputChannelIDs = new HashSet<ChannelID>();

		final Iterator<InputGate<? extends Record>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {

			final InputGate<? extends Record> outputGate = gateIterator.next();
			for (int i = 0; i < outputGate.getNumberOfInputChannels(); ++i) {
				inputChannelIDs.add(outputGate.getInputChannel(i).getID());
			}
		}

		return Collections.unmodifiableSet(inputChannelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getInputGateIDs() {

		final Set<GateID> inputGateIDs = new HashSet<GateID>();

		final Iterator<InputGate<? extends Record>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {
			inputGateIDs.add(gateIterator.next().getGateID());
		}

		return Collections.unmodifiableSet(inputGateIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getOutputGateIDs() {

		final Set<GateID> outputGateIDs = new HashSet<GateID>();

		final Iterator<OutputGate<? extends Record>> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {
			outputGateIDs.add(gateIterator.next().getGateID());
		}

		return Collections.unmodifiableSet(outputGateIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {

		OutputGate<? extends Record> outputGate = null;
		final Iterator<OutputGate<? extends Record>> gateIterator = this.outputGates.iterator();
		while (gateIterator.hasNext()) {
			final OutputGate<? extends Record> candidateGate = gateIterator.next();
			if (candidateGate.getGateID().equals(gateID)) {
				outputGate = candidateGate;
				break;
			}
		}

		if (outputGate == null) {
			throw new IllegalArgumentException("Cannot find output gate with ID " + gateID);
		}

		final Set<ChannelID> outputChannelIDs = new HashSet<ChannelID>();

		for (int i = 0; i < outputGate.getNumberOfOutputChannels(); ++i) {
			outputChannelIDs.add(outputGate.getOutputChannel(i).getID());
		}

		return Collections.unmodifiableSet(outputChannelIDs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {

		InputGate<? extends Record> inputGate = null;
		final Iterator<InputGate<? extends Record>> gateIterator = this.inputGates.iterator();
		while (gateIterator.hasNext()) {
			final InputGate<? extends Record> candidateGate = gateIterator.next();
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
}
